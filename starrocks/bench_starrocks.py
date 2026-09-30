#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
bench_starrocks.py — StarRocks 查询性能基准（BI 典型负载，真实扫描）
==============================================================================================
- 数据: dwd.dwd_event_fact 46,908,364 行 / 34 分区（存储 ~2.35GB）
- 方法: 每查询预热 1 次（warm cache）后计时 3 次，取最优 + 平均
- 覆盖: 真实列扫描 / COUNT 元数据优化（对照组）/ 分区裁剪 / 小基数+大基数分组 /
        精确去重 UV / 漏斗多条件去重 / JOIN / 窗口函数排名
- 吞吐: 基于真实扫描查询（SUM 列聚合）估算 行/s 与 MB/s，避免把元数据 COUNT 当扫描
- 输出: 控制台表格 + JSON（starrocks/bench_results/starrocks_bench.json）
- 用法: <项目根目录>/.venv/bin/python starrocks/bench_starrocks.py
"""

import json
import time
import os
from pathlib import Path

import pymysql

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
    ("Q2", "分区裁剪（单天 COUNT）", "agg", f"SELECT COUNT(*) FROM {FACT} WHERE event_date='2019-10-15'"),
    ("Q3", "小基数分组（event_type）", "agg", f"SELECT event_type, COUNT(*) FROM {FACT} GROUP BY event_type"),
    ("Q4", "按日聚合 GMV（34 组）", "agg", f"SELECT event_date, COUNT(*), SUM(price) FROM {FACT} GROUP BY event_date"),
    ("Q5", "UV 精确去重（全量 user_id）", "agg", f"SELECT COUNT(DISTINCT user_id) FROM {FACT}"),
    ("Q6", "大基数分组（product_id）", "agg", f"SELECT product_id, COUNT(*), SUM(price) FROM {FACT} GROUP BY product_id"),
    ("Q7", "漏斗 3 层 UV（单天）", "agg",
     f"SELECT COUNT(DISTINCT IF(event_type='view',user_id,NULL)),"
     f" COUNT(DISTINCT IF(event_type='cart',user_id,NULL)),"
     f" COUNT(DISTINCT IF(event_type='purchase',user_id,NULL))"
     f" FROM {FACT} WHERE event_date='2019-10-15'"),
    ("Q8", "JOIN 热榜×类目 + 排序 LIMIT", "agg",
     f"SELECT h.product_id, c.category_code, h.sales_amount"
     f" FROM {DB}.ads_product_hot_rank_daily h"
     f" LEFT JOIN {DB}.dim_category c USING (category_id)"
     f" WHERE h.event_date='2019-11-03' ORDER BY h.sales_amount DESC LIMIT 100"),
    ("Q9", "购买明细按用户分组（37 万组）", "agg",
     f"SELECT user_id, COUNT(*), SUM(price) FROM {FACT} WHERE event_type='purchase' GROUP BY user_id"),
    ("Q10", "窗口函数排名（热榜 Top100 模式）", "agg",
     f"SELECT * FROM (SELECT event_date, product_id, sales_amount,"
     f" ROW_NUMBER() OVER (PARTITION BY event_date ORDER BY sales_amount DESC) AS rn"
     f" FROM {DB}.dws_product_daily) t WHERE rn <= 100"),
]


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
    conn = fe_conn()
    cur = conn.cursor()
    cur.execute("SELECT VERSION()")
    version = cur.fetchone()[0]
    cur.execute(f"SELECT COUNT(*) FROM {FACT}")
    total_rows = cur.fetchone()[0]
    cur.execute(f"SELECT data_length FROM information_schema.tables "
                f"WHERE table_schema='{DB}' AND table_name='dwd_event_fact'")
    data_bytes = cur.fetchone()[0]

    print(f"StarRocks {version} | {FACT} = {total_rows:,} 行 | 存储 {data_bytes/1024/1024:,.0f} MB")
    print(f"方法: 预热 1 次 + 计时 {REPEAT} 次（取最优/平均）\n")

    results = []
    print(f"{'Q':<5}{'说明':<30}{'类型':<6}{'行数':>11}{'冷(ms)':>9}{'最优(ms)':>9}{'平均(ms)':>9}")
    print("-" * 84)
    for qid, desc, qtype, sql in QUERIES:
        try:
            time_once(cur, sql)              # 预热
            cold, _ = time_once(cur, sql)
            times, rows_n = [], 0
            for _ in range(REPEAT):
                dt, rows = time_once(cur, sql)
                times.append(dt * 1000)
                rows_n = len(rows)
            best, avg = min(times), sum(times) / len(times)
            results.append({"qid": qid, "desc": desc, "type": qtype, "rows": rows_n,
                            "cold_ms": round(cold * 1000, 1),
                            "best_ms": round(best, 1), "avg_ms": round(avg, 1)})
            print(f"{qid:<5}{desc:<30}{qtype:<6}{rows_n:>11,}{cold*1000:>9.1f}{best:>9.1f}{avg:>9.1f}")
        except Exception as e:
            results.append({"qid": qid, "desc": desc, "type": qtype, "error": str(e)[:80]})
            print(f"{qid:<5}{desc:<30}  ❌ {str(e)[:60]}")

    # 真实扫描吞吐：基于 Q1（SUM 列聚合，必须真实读列数据）。
    # MB/s 按被扫列（price, DOUBLE=8B/行）的原始体积估算，避免用整表体积虚高。
    q1 = next((r for r in results if r.get("qid") == "Q1"), None)
    if q1 and q1.get("best_ms"):
        best_s = q1["best_ms"] / 1000
        col_bytes = total_rows * 8  # price 列 DOUBLE
        rps = round(total_rows / best_s)
        mbps = round(col_bytes / 1024 / 1024 / best_s, 1)
        q1["rows_per_sec"] = rps
        q1["mb_per_sec"] = mbps
        print(f"\n📈 真实扫描吞吐（Q1 SUM(price)，读 price 列 46.9M×8B）:")
        print(f"   {rps:,} 行/s | {mbps:,.1f} MB/s（原始体积口径，最优 {q1['best_ms']:.1f}ms）")
    print("   （Q1m COUNT(*) 走元数据优化，不计入扫描吞吐）")

    conn.close()
    OUT_DIR.mkdir(parents=True, exist_ok=True)
    out = {"engine": "starrocks", "version": version, "total_rows": total_rows,
           "data_mb": round(data_bytes / 1024 / 1024, 1),
           "method": "warmup x1 + timed x3 (best/avg)", "queries": results}
    path = OUT_DIR / "starrocks_bench.json"
    path.write_text(json.dumps(out, ensure_ascii=False, indent=2), encoding="utf-8")
    print(f"\n📄 结果已存: {path}")


if __name__ == "__main__":
    main()
