#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
bench_starrocks.py — StarRocks 查询性能基准（BI 典型负载，真实扫描）
数据: dwd.dwd_event_fact 109,806,507 行 / 61 分区
方法: 每查询预热 1 次后计时 3 次，取最优 + 平均
吞吐: 基于 Q1 真实扫描估算行/s 与 MB/s，不把元数据 COUNT 当扫描
输出: 控制台表格 + bench_results/starrocks_bench.json
用法: .venv/bin/python starrocks/bench_starrocks.py [--only Q7 --out ...]
--only 未给 --out 时不写文件；SELECT VERSION() 是 MySQL 兼容版本，真实版本用 current_version()。
"""

import argparse
import json
import time
import os
from pathlib import Path

import pymysql

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from config.env import load_env  # noqa: E402

load_env()

SR_FE = (os.environ.get("SR_HOST", "127.0.0.1"), int(os.environ.get("SR_PORT", "9030")))
SR_USER, SR_PASS = os.environ.get("SR_USER", "root"), os.environ.get("SR_PASSWORD", "")
DB = "dwd"
FACT = f"{DB}.dwd_event_fact"
REPEAT = 3
OUT_DIR = Path(__file__).resolve().parent / "bench_results"

# (编号, 说明, 类型, SQL)  类型: scan=真实扫描基准, agg=聚合, meta=元数据(非扫描)
QUERIES = [
    ("Q1", "真实列扫描 SUM(price)", "scan", f"SELECT SUM(price) FROM {FACT}"),
    ("Q1m", "COUNT(*)（元数据优化，不扫描）", "meta", f"SELECT COUNT(*) FROM {FACT}"),
    ("Q2", "单天裁剪（date= 过滤 COUNT）", "agg", f"SELECT COUNT(*) FROM {FACT} WHERE event_date='2019-10-15'"),
    ("Q3", "小基数分组（event_type）", "agg", f"SELECT event_type, COUNT(*) FROM {FACT} GROUP BY event_type"),
    ("Q4", "按日聚合 GMV（61 组）", "agg", f"SELECT event_date, COUNT(*), SUM(price) FROM {FACT} GROUP BY event_date"),
    ("Q5", "UV 精确去重（全量 user_id）", "agg", f"SELECT COUNT(DISTINCT user_id) FROM {FACT}"),
    ("Q6", "大基数分组（product_id）", "agg", f"SELECT product_id, COUNT(*), SUM(price) FROM {FACT} GROUP BY product_id"),
    ("Q7", "漏斗 3 层 UV（单天）", "agg",
     f"SELECT COUNT(DISTINCT IF(event_type='view',user_id,NULL)),"
     f" COUNT(DISTINCT IF(event_type='cart',user_id,NULL)),"
     f" COUNT(DISTINCT IF(event_type='purchase',user_id,NULL))"
     f" FROM {FACT} WHERE event_date='2019-10-15'"),
    ("Q8", "JOIN 热榜×类目 + 排序 LIMIT", "agg",
     f"SELECT h.product_id, c.category_code, h.sales_amount"
     f" FROM ads.ads_product_hot_rank_daily h"
     f" LEFT JOIN dim.dim_category c USING (category_id)"
     f" WHERE h.event_date='2019-11-03' ORDER BY h.sales_amount DESC LIMIT 100"),
    ("Q9", "购买明细按用户分组（71 万组）", "agg",
     f"SELECT user_id, COUNT(*), SUM(price) FROM {FACT} WHERE event_type='purchase' GROUP BY user_id"),
    ("Q10", "窗口函数排名（热榜 Top100 模式）", "agg",
     f"SELECT * FROM (SELECT event_date, product_id, sales_amount,"
     f" ROW_NUMBER() OVER (PARTITION BY event_date ORDER BY sales_amount DESC) AS rn"
     f" FROM dws.dws_product_daily) t WHERE rn <= 100"),
]

# 每条查询的预期返回行数（109,806,507 行 / 61 分区快照）；不等即记 error，耗时不可对比。
EXPECTED_ROWS = {
    "Q1": 1, "Q1m": 1, "Q2": 1, "Q3": 3, "Q4": 61, "Q5": 1,
    "Q6": 206876, "Q7": 1, "Q8": 100, "Q9": 707466, "Q10": 6100,
}


def fetch_storage(cur, db, tbl):
    """取表占用的存储字节数，返回 (bytes, 来源)。

    首选 information_schema.tables.data_length；返回 0（FE 刚启动、统计未加载）时
    回退 SHOW DATA 解析 Size，不能把 0 静默写入结果 JSON。
    """
    cur.execute("SELECT data_length FROM information_schema.tables "
                "WHERE table_schema=%s AND table_name=%s", (db, tbl))
    row = cur.fetchone()
    if row and row[0]:
        return int(row[0]), "information_schema.data_length"

    cur.execute(f"SHOW DATA FROM `{db}`.`{tbl}`")
    vals = []
    for r in cur.fetchall():
        size = r[2] if len(r) > 2 else None
        if isinstance(size, str) and " " in size:
            num, unit = size.split()[:2]
            mul = {"B": 1, "KB": 1024, "MB": 1024 ** 2,
                   "GB": 1024 ** 3, "TB": 1024 ** 4}.get(unit.upper())
            if mul:
                try:
                    vals.append(float(num) * mul)
                except ValueError:
                    pass
    if vals:
        return int(max(vals)), "SHOW DATA(Total)"

    print("  WARN: 存储体积取不到（information_schema 与 SHOW DATA 均为空），data_mb 记 0")
    return 0, "unavailable"


def fe_conn():
    return pymysql.connect(host=SR_FE[0], port=SR_FE[1], user=SR_USER,
                           password=SR_PASS, charset="utf8",
                           connect_timeout=60, read_timeout=3600)


def time_once(cur, sql):
    t0 = time.perf_counter()
    cur.execute(sql)
    rows = cur.fetchall()
    return time.perf_counter() - t0, rows


def main():
    ap = argparse.ArgumentParser(description="StarRocks 侧查询性能基准")
    ap.add_argument("--only", default="",
                    help="只跑指定 qid（逗号分隔，如 Q7）。用于单条重测，不必跑全套。")
    ap.add_argument("--out", default="",
                    help="结果 JSON 路径，默认 bench_results/starrocks_bench.json。"
                         "用 --only 时必须显式给 --out，否则不写文件。")
    args = ap.parse_args()
    wanted = [s.strip().upper() for s in args.only.split(",") if s.strip()]
    qs = QUERIES
    if wanted:
        qs = [q for q in QUERIES if q[0].upper() in wanted]
        if not qs:
            raise SystemExit(f"--only 没匹配到任何查询：{wanted}")
        print(f"WARN: 单条重测：只跑 {[q[0] for q in qs]}"
              f"{'，未指定 --out → 不写文件' if not args.out else ''}\n")

    conn = fe_conn()
    cur = conn.cursor()
    cur.execute("SELECT VERSION()")
    mysql_compat_version = cur.fetchone()[0]      # 8.0.33：MySQL 协议兼容版本
    try:
        cur.execute("SELECT current_version()")   # StarRocks 真实版本
        version = cur.fetchone()[0]
    except Exception:
        version = mysql_compat_version
    cur.execute(f"SELECT COUNT(*) FROM {FACT}")
    total_rows = cur.fetchone()[0]
    data_bytes, data_src = fetch_storage(cur, DB, "dwd_event_fact")

    print(f"StarRocks {version}（MySQL 兼容版本号 {mysql_compat_version}）")
    print(f"{FACT} = {total_rows:,} 行 | 存储 {data_bytes/1024/1024:,.0f} MB（{data_src}）")
    print(f"方法: 预热 1 次 + 计时 {REPEAT} 次（取最优/平均）\n")

    results = []
    print(f"{'Q':<5}{'说明':<36}{'类型':<6}{'行数':>11}{'冷(ms)':>9}{'最优(ms)':>9}{'平均(ms)':>9}")
    print("-" * 90)
    for qid, desc, qtype, sql in qs:
        try:
            time_once(cur, sql)              # 预热
            cold, _ = time_once(cur, sql)
            times, rows_n = [], 0
            for _ in range(REPEAT):
                dt, rows = time_once(cur, sql)
                times.append(dt * 1000)
                rows_n = len(rows)
            # 行数闸门：语义变了则耗时不可对比
            exp = EXPECTED_ROWS.get(qid)
            if exp is not None and rows_n != exp:
                raise ValueError(f"结果行数 {rows_n:,} ≠ 预期 {exp:,}，语义可能已变，不可用于对比")
            best, avg = min(times), sum(times) / len(times)
            results.append({"qid": qid, "desc": desc, "type": qtype, "rows": rows_n,
                            "exp_rows": exp,
                            "cold_ms": round(cold * 1000, 1),
                            "best_ms": round(best, 1), "avg_ms": round(avg, 1)})
            print(f"{qid:<5}{desc:<36}{qtype:<6}{rows_n:>11,}{cold*1000:>9.1f}{best:>9.1f}{avg:>9.1f}")
        except Exception as e:
            results.append({"qid": qid, "desc": desc, "type": qtype, "error": str(e)[:80]})
            print(f"{qid:<5}{desc:<36}  FAIL: {str(e)[:60]}")

    # 真实扫描吞吐：基于 Q1；MB/s 按 price 列（DOUBLE=8B/行）原始体积估算，不按整表体积。
    q1 = next((r for r in results if r.get("qid") == "Q1"), None)
    if q1 and q1.get("best_ms"):
        best_s = q1["best_ms"] / 1000
        col_bytes = total_rows * 8  # price 列 DOUBLE
        rps = round(total_rows / best_s)
        mbps = round(col_bytes / 1024 / 1024 / best_s, 1)
        q1["rows_per_sec"] = rps
        q1["mb_per_sec"] = mbps
        print(f"\n真实扫描吞吐（Q1 SUM(price)，读 price 列 {total_rows/1e6:.1f}M×8B）:")
        print(f"   {rps:,} 行/s | {mbps:,.1f} MB/s（原始体积口径，最优 {q1['best_ms']:.1f}ms）")
    print("   （Q1m COUNT(*) 走元数据优化，不计入扫描吞吐）")

    conn.close()

    # 单条重测且未给 --out 时不落盘，避免整套结果被覆盖成一条
    if wanted and not args.out:
        print("\n（单条重测且未指定 --out，未写文件；要落盘请加 --out <路径>）")
        return

    out_path = Path(args.out) if args.out else (OUT_DIR / "starrocks_bench.json")
    out_path.parent.mkdir(parents=True, exist_ok=True)
    out = {"engine": "starrocks", "version": version,
           "mysql_compat_version": mysql_compat_version,
           "total_rows": total_rows,
           "data_mb": round(data_bytes / 1024 / 1024, 1),
           "data_source": data_src,
           "method": "warmup x1 + timed x3 (best/avg)", "queries": results}
    if wanted:
        out["partial"] = f"单条重测，仅包含 {[r['qid'] for r in results]}"
    out_path.write_text(json.dumps(out, ensure_ascii=False, indent=2), encoding="utf-8")
    print(f"\n结果已存: {out_path}")


if __name__ == "__main__":
    main()
