#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
load_to_starrocks.py —— 把导出的 CSV 灌进 StarRocks
====================================================
配套 export_hive_to_csv.py：那边先导出，这边再灌。两步分开是为了**串行**跑，
避免 Spark 驱动和 StarRocks 容器同时吃内存（本机只有 7.6G，同时跑必 OOM）。

★ 表模型为什么全用 Duplicate Key（明细模型）★
  · 这次的目标是「复刻」——把 Spark 已经跑出来的结果原样搬过去，不做任何聚合语义
  · Primary Key / Aggregate 模型都要求 key 列 NOT NULL，Hive 侧只要有 NULL 就会灌失败
  · Duplicate Key 的 key 只是排序键/前缀索引，不要求唯一、不要求非空，最稳妥
  · 代价是少了主键去重能力；真需要时再对 DIM 单独改成 Primary Key 模型

★ 幂等：默认**先 TRUNCATE 再灌**。
  Stream Load 本身是追加语义，重跑一次就会翻倍（ads_trade_daily 试灌过一次后
  再跑全量，行数从 35 变成 70 才发现）。对「复刻」这种全量重建场景，
  重跑必须得到同样的结果，所以默认每次清空。真要追加时用 --append。

用法：
  python starrocks/load_to_starrocks.py --manifest ~/sr_export/manifest.json
  python starrocks/load_to_starrocks.py --only dws_traffic_hour_daily
  python starrocks/load_to_starrocks.py --dry-run        # 只打印会做什么，不执行
"""

import argparse
import json
import subprocess
import sys
import time
from pathlib import Path

import pymysql

# ---------------------------------------------------------------------------
# 目标表结构（StarRocks 侧）
#   columns : (列名, SR 类型)，顺序与 CSV 一致
#   key     : DUPLICATE KEY 的列（必须是 columns 的前缀，这里统一取前 1~2 列）
#   buckets : 分桶数，按数据量给
# ---------------------------------------------------------------------------
SCHEMA = {
    # ---------------- DWS ----------------
    "dws.dws_product_daily": {
        "key": ["product_id"],
        "buckets": 4,
        "columns": [
            ("product_id", "BIGINT"), ("category_id", "BIGINT"), ("brand", "VARCHAR(128)"),
            ("exposure_uv", "BIGINT"), ("cart_uv", "BIGINT"), ("purchase_uv", "BIGINT"),
            ("sales_cnt", "BIGINT"), ("sales_amount", "DOUBLE"), ("event_date", "DATE"),
        ],
    },
    "dws.dws_traffic_hour_daily": {
        "key": ["event_hour"],
        "buckets": 1,
        "columns": [
            ("event_hour", "INT"), ("pv", "BIGINT"), ("uv", "BIGINT"), ("view_pv", "BIGINT"),
            ("cart_cnt", "BIGINT"), ("purchase_cnt", "BIGINT"), ("purchase_uv", "BIGINT"),
            ("gmv", "DOUBLE"), ("session_cnt", "BIGINT"), ("event_date", "DATE"),
        ],
    },
    "dws.dws_user_behavior_daily": {
        "key": ["user_id"],
        "buckets": 4,
        "columns": [
            ("user_id", "BIGINT"), ("active_action_cnt", "BIGINT"),
            ("purchase_amount", "DOUBLE"), ("purchase_product_cnt", "BIGINT"),
            ("purchase_cnt", "BIGINT"), ("event_date", "DATE"),
        ],
    },
    "dws.dws_user_session_daily": {
        "key": ["user_id"],
        "buckets": 4,
        "columns": [
            ("user_id", "BIGINT"), ("user_session", "VARCHAR(128)"), ("view_cnt", "BIGINT"),
            ("cart_cnt", "BIGINT"), ("purchase_cnt", "BIGINT"), ("remove_cart_cnt", "BIGINT"),
            ("session_duration_sec", "BIGINT"), ("event_date", "DATE"),
        ],
    },
    # ---------------- ADS ----------------
    "ads.ads_trade_daily": {
        "key": ["event_date"],
        "buckets": 1,
        "columns": [
            ("event_date", "DATE"), ("gmv", "DOUBLE"), ("order_cnt", "BIGINT"),
            ("buyer_cnt", "BIGINT"), ("avg_order_value", "DOUBLE"), ("arppu", "DOUBLE"),
        ],
    },
    "ads.ads_conversion_funnel_daily": {
        "key": ["event_date"],
        "buckets": 1,
        "columns": [
            ("event_date", "DATE"), ("view_uv", "BIGINT"), ("cart_uv", "BIGINT"),
            ("purchase_uv", "BIGINT"), ("view_to_cart_rate", "DOUBLE"),
            ("view_to_purchase_rate", "DOUBLE"), ("cart_to_purchase_rate", "DOUBLE"),
        ],
    },
    "ads.ads_session_behavior_daily": {
        "key": ["event_date"],
        "buckets": 1,
        "columns": [
            ("event_date", "DATE"), ("session_cnt", "BIGINT"), ("avg_duration_sec", "DOUBLE"),
            ("avg_action_cnt", "DOUBLE"), ("cart_purchase_rate", "DOUBLE"),
        ],
    },
    "ads.ads_user_retention_daily": {
        "key": ["event_date"],
        "buckets": 1,
        "columns": [
            ("event_date", "DATE"), ("d0_uv", "BIGINT"), ("d1_uv", "BIGINT"),
            ("d1_rate", "DOUBLE"), ("d3_uv", "BIGINT"), ("d3_rate", "DOUBLE"),
            ("d7_uv", "BIGINT"), ("d7_rate", "DOUBLE"),
        ],
    },
    "ads.ads_traffic_hour_daily": {
        "key": ["event_date"],
        "buckets": 1,
        "columns": [
            ("event_date", "DATE"), ("event_hour", "INT"), ("time_bucket", "VARCHAR(32)"),
            ("pv", "BIGINT"), ("uv", "BIGINT"), ("purchase_cnt", "BIGINT"),
            ("purchase_uv", "BIGINT"), ("gmv", "DOUBLE"), ("pv_share", "DOUBLE"),
            ("order_share", "DOUBLE"), ("purchase_rate", "DOUBLE"), ("is_peak_hour", "INT"),
        ],
    },
    "ads.ads_product_hot_rank_daily": {
        "key": ["event_date"],
        "buckets": 1,
        "columns": [
            ("event_date", "DATE"), ("rank", "INT"), ("product_id", "BIGINT"),
            ("category_id", "BIGINT"), ("brand", "VARCHAR(128)"), ("sales_cnt", "BIGINT"),
            ("sales_amount", "DOUBLE"), ("exposure_uv", "BIGINT"),
        ],
    },
    "ads.ads_user_rfm_snapshot": {
        "key": ["user_id"],
        "buckets": 2,
        "columns": [
            ("user_id", "BIGINT"), ("r_days", "BIGINT"), ("f_cnt", "BIGINT"),
            ("m_amount", "DOUBLE"), ("rfm_seg", "VARCHAR(32)"), ("as_of_dt", "DATE"),
        ],
    },
    # ---------------- DIM ----------------
    "dim.dim_category": {
        "key": ["category_id"],
        "buckets": 1,
        "columns": [
            ("category_id", "BIGINT"), ("category_code", "VARCHAR(128)"),
            ("category_l1", "VARCHAR(64)"), ("category_l2", "VARCHAR(64)"),
            ("category_l3", "VARCHAR(64)"),
        ],
    },
    "dim.dim_date": {
        "key": ["date_id"],
        "buckets": 1,
        "columns": [
            ("date_id", "DATE"), ("year", "INT"), ("month", "INT"), ("day", "INT"),
            ("week_of_year", "INT"), ("day_of_week", "INT"), ("is_workday", "INT"),
        ],
    },
    "dim.dim_product_scd2": {
        "key": ["dim_product_sk"],
        "buckets": 2,
        "columns": [
            ("dim_product_sk", "BIGINT"), ("product_id", "BIGINT"), ("category_id", "BIGINT"),
            ("category_code", "VARCHAR(128)"), ("brand", "VARCHAR(128)"),
            ("dw_start_date", "DATE"), ("dw_end_date", "DATE"), ("dw_is_current", "INT"),
        ],
    },
    "dim.dim_user": {
        "key": ["user_id"],
        "buckets": 4,
        "columns": [
            ("user_id", "BIGINT"), ("first_seen_date", "DATE"), ("last_seen_date", "DATE"),
            ("active_days", "BIGINT"), ("first_purchase_date", "DATE"), ("purchase_days", "BIGINT"),
        ],
    },
    "dim.dim_session": {
        "key": ["user_session"],
        "buckets": 4,
        "columns": [
            ("user_session", "VARCHAR(128)"), ("user_id", "BIGINT"),
            ("session_start_time", "DATETIME"), ("session_end_time", "DATETIME"),
        ],
    },
}

# 保留字/易冲突的列名，建表时加反引号
QUOTED = {"rank", "year", "month", "day"}


def q(name):
    return f"`{name}`" if name in QUOTED else name


def connect(host, port, user, password):
    return pymysql.connect(host=host, port=port, user=user, password=password, charset="utf8")


def create_table(conn, db, tbl, spec):
    cols_ddl = ",\n  ".join(f"{q(c)} {t}" for c, t in spec["columns"])
    key_ddl = ", ".join(q(k) for k in spec["key"])
    dist = spec["key"][0]
    ddl = (
        f"CREATE TABLE IF NOT EXISTS `{db}`.`{tbl}` (\n"
        f"  {cols_ddl}\n"
        f")\n"
        f"DUPLICATE KEY({key_ddl})\n"
        f"DISTRIBUTED BY HASH({q(dist)}) BUCKETS {spec['buckets']}\n"
        f'PROPERTIES ("replication_num" = "1")'
    )
    with conn.cursor() as cur:
        cur.execute(f"CREATE DATABASE IF NOT EXISTS `{db}`")
        cur.execute(ddl)
    return ddl


def stream_load(csv_path, db, tbl, columns, fe_host, fe_port, user, password):
    """走 FE 的 8030（FE 会 307 重定向到 BE，curl 用 --location-trusted 跟过去）。"""
    url = f"http://{fe_host}:{fe_port}/api/{db}/{tbl}/_stream_load"
    cmd = [
        "curl", "-s", "--location-trusted",
        "-u", f"{user}:{password}",
        "-H", "format: csv",
        "-H", "column_separator: ,",
        "-H", f"columns: {', '.join(columns)}",
        "-H", "max_filter_ratio: 0.01",
        "-T", str(csv_path),
        "-XPUT", url,
    ]
    out = subprocess.run(cmd, capture_output=True, text=True, timeout=7200)
    try:
        return json.loads(out.stdout)
    except Exception:
        return {"Status": "PARSE_FAIL", "Message": (out.stdout or out.stderr)[:400]}


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--manifest", default=str(Path.home() / "sr_export" / "manifest.json"))
    ap.add_argument("--fe-host", default="127.0.0.1")
    ap.add_argument("--fe-http-port", type=int, default=8030, help="FE HTTP（Stream Load 入口）")
    ap.add_argument("--query-port", type=int, default=9030, help="FE MySQL 协议端口")
    ap.add_argument("--user", default="root")
    ap.add_argument("--password", default="")
    ap.add_argument("--only", nargs="*", help="只处理指定的表")
    ap.add_argument("--append", action="store_true", help="追加而不是先清空（默认先 TRUNCATE）")
    ap.add_argument("--dry-run", action="store_true")
    args = ap.parse_args()

    manifest = json.loads(Path(args.manifest).read_text(encoding="utf-8"))
    items = [m for m in manifest if "rows" in m]
    if args.only:
        items = [m for m in items if m["table"] in args.only]

    print(f"📥 待灌入 {len(items)} 张表（manifest: {args.manifest}）")
    if args.dry_run:
        for m in items:
            print(f"   {m['db']}.{m['table']:<30s} {m['rows']:>12,d} 行")
        return

    conn = connect(args.fe_host, args.query_port, args.user, args.password)
    results = []
    for m in items:
        fq = f"{m['db']}.{m['table']}"
        spec = SCHEMA.get(fq)
        if spec is None:
            print(f"  ⚠️  {fq}: SCHEMA 里没定义，跳过")
            continue

        # columns 头必须按 CSV 的真实列序 —— manifest 里带着，比 SCHEMA 的建表顺序可靠
        csv_cols = m["columns"]
        schema_cols = [c for c, _ in spec["columns"]]
        if set(csv_cols) != set(schema_cols):
            print(f"  ❌ {fq:<34s} 列名对不上：CSV={sorted(csv_cols)} vs SCHEMA={sorted(schema_cols)}")
            results.append({"table": fq, "hive": m["rows"], "sr": None, "ok": False,
                            "error": "列名集合不一致"})
            continue

        t0 = time.time()
        try:
            create_table(conn, m["db"], m["table"], spec)
            if not args.append:
                with conn.cursor() as cur:
                    cur.execute(f"TRUNCATE TABLE `{m['db']}`.`{m['table']}`")
            r = stream_load(m["csv"], m["db"], m["table"], csv_cols,
                            args.fe_host, args.fe_http_port, args.user, args.password)
            status = r.get("Status", "?")
            if status == "Success":
                n_loaded = r.get("NumberLoadedRows", 0)
                with conn.cursor() as cur:
                    cur.execute(f"SELECT COUNT(*) FROM `{m['db']}`.`{m['table']}`")
                    n_sr = cur.fetchone()[0]
                ok = (n_sr == m["rows"])
                print(f"  {'✅' if ok else '❌'} {fq:<34s} Hive {m['rows']:>12,d} "
                      f"→ SR {n_sr:>12,d}  ({time.time()-t0:.0f}s)")
                results.append({"table": fq, "hive": m["rows"], "sr": n_sr, "ok": ok})
            else:
                msg = r.get("Message", "")[:200]
                print(f"  ❌ {fq:<34s} {status}: {msg}")
                results.append({"table": fq, "hive": m["rows"], "sr": None, "ok": False,
                                "error": msg})
        except Exception as e:
            print(f"  ❌ {fq:<34s} {type(e).__name__}: {str(e)[:160]}")
            results.append({"table": fq, "hive": m["rows"], "sr": None, "ok": False,
                            "error": str(e)[:200]})
    conn.close()

    ok = [r for r in results if r["ok"]]
    print()
    print(f"✅ 完成 {len(ok)}/{len(results)} 张表")
    bad = [r for r in results if not r["ok"]]
    if bad:
        print("   失败：")
        for r in bad:
            print(f"     - {r['table']}: {r.get('error', '行数不一致')}")


if __name__ == "__main__":
    main()
