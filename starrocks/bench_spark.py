#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
bench_spark.py — Spark 3.5.9 查询性能基准（与 StarRocks 同查询、同数据对比）
数据: HDFS dwd_event_fact 109,806,507 行 / 61 分区
配置: local[4]、driver 2500m、shuffle 48 分区
方法: 预热 1 次 + 计时 2 次取最优（StarRocks 侧为 3 次）
覆盖: Q1-Q7、Q9；Q8/Q10 依赖 StarRocks 内 DWS/ADS 表，无法对等执行
输出: bench_results/spark_bench.json；--mode sql 默认写 spark_bench_sql.json
用法: .venv/bin/python starrocks/bench_spark.py [--only Q7 --out ...] [--mode df|sql]
--only 未给 --out 时不写文件，避免覆盖整套结果；Q7 的 when 第二参数必须用 F.col("user_id")。
"""

import argparse
import json
import sys
import time
from pathlib import Path

from pyspark.sql import SparkSession, functions as F

HDFS = "hdfs://localhost:8020"
FACT_PATH = f"{HDFS}/home/lst/hadoop-data/warehouse/dwd_event_fact"
OUT_DIR = Path(__file__).resolve().parent / "bench_results"
REPEAT = 2

spark = (
    SparkSession.builder
    .master("local[4]")
    .appName("BenchSpark")
    .config("spark.driver.memory", "2500m")
    .config("spark.hadoop.fs.defaultFS", HDFS)
    .config("spark.sql.adaptive.enabled", "true")
    .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
    .config("spark.sql.session.timeZone", "UTC")
    .config("spark.sql.shuffle.partitions", "48")
    .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    .config("spark.ui.showConsoleProgress", "false")
    .getOrCreate()
)
spark.sparkContext.setLogLevel("ERROR")


def main():
    ap = argparse.ArgumentParser(description="Spark 侧查询性能基准（与 StarRocks 对等对比）")
    ap.add_argument("--only", default="",
                    help="只跑指定 qid（逗号分隔，如 Q7）。用于单条重测，不必跑全套。")
    ap.add_argument("--out", default="",
                    help="结果 JSON 路径，默认 bench_results/spark_bench.json。"
                         "用 --only 时必须显式给 --out，否则不写文件 —— 防止把整套结果覆盖成一条。")
    ap.add_argument("--mode", default="df", choices=["df", "sql"],
                    help="查询写法：df=DataFrame API（默认，历史口径）；sql=Spark SQL。"
                         "sql 模式不指定 --out 时默认写 spark_bench_sql.json，不会覆盖 spark_bench.json。")
    args = ap.parse_args()
    wanted = [s.strip().upper() for s in args.only.split(",") if s.strip()]

    df = spark.read.parquet(FACT_PATH)
    if wanted:
        total = None
        print(f"Spark {spark.version} | {FACT_PATH}（单条重测模式，跳过全表 count）")
    else:
        total = df.count()
        print(f"Spark {spark.version} | {FACT_PATH} = {total:,} 行")
    print(f"配置: {spark.sparkContext.master}, driver {spark.conf.get('spark.driver.memory')}")
    print(f"模式: --mode {args.mode}（{'DataFrame API' if args.mode == 'df' else 'Spark SQL'}）")
    print("方法: 预热 1 次 + 计时 2 次（取最优）\n")

    # (说明, 预期行数 exp_rows, 触发函数)；行数不等即抛错，该耗时不可用于对比。
    # df / sql 两套的 desc 与 exp_rows 必须一致，否则 A/B 结果无法对齐。
    df.createOrReplaceTempView("fact")

    QUERIES_DF = [
        ("Q1 真实列扫描 SUM(price)", 1,
         lambda: df.agg(F.sum("price")).collect()),
        ("Q1m COUNT(*)", 1,
         lambda: [(df.count(),)]),
        # Spark 侧为真分区裁剪（61 个目录）；StarRocks 侧无 PARTITION BY，靠分桶裁剪
        # 达到同等语义，两侧机制不同、语义相同。
        ("Q2 单天裁剪（date= 过滤 COUNT）", 1,
         lambda: [(df.filter(F.col("event_date") == "2019-10-15").count(),)]),
        ("Q3 小基数分组（event_type）", 3,
         lambda: df.groupBy("event_type").count().collect()),
        ("Q4 按日聚合 GMV（61 组）", 61,
         lambda: df.groupBy("event_date").agg(F.count("*").alias("c"),
                                              F.sum("price").alias("s")).collect()),
        ("Q5 UV 精确去重（全量 user_id）", 1,
         lambda: df.agg(F.countDistinct("user_id")).collect()),
        ("Q6 大基数分组（product_id，21 万组）", 206876,
         lambda: df.groupBy("product_id").agg(F.count("*").alias("c"),
                                              F.sum("price").alias("s")).collect()),
        ("Q7 漏斗 3 层 UV（单天）", 1,
         # 第二参数必须是 Column，传字符串会当字面量，去重恒为 1。
         lambda: df.filter(F.col("event_date") == "2019-10-15")
                   .agg(F.countDistinct(F.when(F.col("event_type") == "view", F.col("user_id"))),
                        F.countDistinct(F.when(F.col("event_type") == "cart", F.col("user_id"))),
                        F.countDistinct(F.when(F.col("event_type") == "purchase", F.col("user_id")))).collect()),
        ("Q9 购买明细按用户分组（71 万组）", 707466,
         lambda: df.filter(F.col("event_type") == "purchase")
                   .groupBy("user_id").agg(F.count("*").alias("c"),
                                           F.sum("price").alias("s")).collect()),
    ]

    # SQL 版与 StarRocks 侧写法对齐。event_date 为 DATE 列，等值过滤可下推；
    # 不要对 event_date 套函数（如 DATE_FORMAT），否则失去分区裁剪。
    QUERIES_SQL = [
        ("Q1 真实列扫描 SUM(price)", 1,
         lambda: spark.sql("SELECT SUM(price) FROM fact").collect()),
        ("Q1m COUNT(*)", 1,
         lambda: [(spark.sql("SELECT COUNT(*) FROM fact").first()[0],)]),
        ("Q2 单天裁剪（date= 过滤 COUNT）", 1,
         lambda: [(spark.sql("SELECT COUNT(*) FROM fact WHERE event_date='2019-10-15'").first()[0],)]),
        ("Q3 小基数分组（event_type）", 3,
         lambda: spark.sql("SELECT event_type, COUNT(*) AS c FROM fact GROUP BY event_type").collect()),
        ("Q4 按日聚合 GMV（61 组）", 61,
         lambda: spark.sql("SELECT event_date, COUNT(*) AS c, SUM(price) AS s"
                           " FROM fact GROUP BY event_date").collect()),
        ("Q5 UV 精确去重（全量 user_id）", 1,
         lambda: spark.sql("SELECT COUNT(DISTINCT user_id) AS uv FROM fact").collect()),
        ("Q6 大基数分组（product_id，21 万组）", 206876,
         lambda: spark.sql("SELECT product_id, COUNT(*) AS c, SUM(price) AS s"
                           " FROM fact GROUP BY product_id").collect()),
        ("Q7 漏斗 3 层 UV（单天）", 1,
         # 与 StarRocks 侧 COUNT(DISTINCT IF(...)) 写法一致。
         lambda: spark.sql("SELECT COUNT(DISTINCT IF(event_type='view',     user_id, NULL)) AS view_uv,"
                           "       COUNT(DISTINCT IF(event_type='cart',     user_id, NULL)) AS cart_uv,"
                           "       COUNT(DISTINCT IF(event_type='purchase', user_id, NULL)) AS purchase_uv"
                           " FROM fact WHERE event_date='2019-10-15'").collect()),
        ("Q9 购买明细按用户分组（71 万组）", 707466,
         lambda: spark.sql("SELECT user_id, COUNT(*) AS c, SUM(price) AS s"
                           " FROM fact WHERE event_type='purchase' GROUP BY user_id").collect()),
    ]

    QUERIES = QUERIES_DF if args.mode == "df" else QUERIES_SQL

    if wanted:
        QUERIES = [q for q in QUERIES if q[0].split()[0].upper() in wanted]
        if not QUERIES:
            raise SystemExit(f"--only 没匹配到任何查询：{wanted}")
        print(f"WARN: 单条重测：只跑 {[q[0].split()[0] for q in QUERIES]}"
              f"{'，未指定 --out → 不写文件' if not args.out else ''}\n")

    results = []
    print(f"{'Q':<5}{'说明':<36}{'行数':>11}{'冷(ms)':>11}{'最优(ms)':>11}{'平均(ms)':>11}")
    print("-" * 88)
    for desc, exp_rows, fn in QUERIES:
        qname = desc.split()[0]
        try:
            fn()                                   # 预热
            cold, rows = fn_timed(fn)
            times, n = [], len(rows)
            for _ in range(REPEAT):
                dt, rows = fn_timed(fn)
                times.append(dt)
                n = len(rows)
            # 行数闸门：语义不等价的结果不能用于对比，按失败处理
            if exp_rows is not None and n != exp_rows:
                raise ValueError(f"结果行数 {n:,} ≠ 预期 {exp_rows:,}，语义不等价，该行不可用于对比")
            best, avg = min(times), sum(times) / len(times)
            results.append({"qid": qname, "desc": desc, "rows": n, "exp_rows": exp_rows,
                            "cold_ms": round(cold, 1), "best_ms": round(best, 1),
                            "avg_ms": round(avg, 1)})
            print(f"{qname:<5}{desc:<36}{n:>11,}{cold:>11.1f}{best:>11.1f}{avg:>11.1f}")
        except Exception as e:
            results.append({"qid": qname, "desc": desc, "error": str(e)[:100]})
            print(f"{qname:<5}{desc:<36}  FAIL: {str(e)[:70]}")

    spark.stop()

    # 单条重测且未给 --out 时不落盘，避免整套结果被覆盖成一条
    if wanted and not args.out:
        print("\n（单条重测且未指定 --out，未写文件；要落盘请加 --out <路径>）")
        return

    # sql 模式默认写另一个文件，避免覆盖 df 口径的结果
    default_name = "spark_bench.json" if args.mode == "df" else "spark_bench_sql.json"
    out_path = Path(args.out) if args.out else (OUT_DIR / default_name)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    out = {"engine": "spark", "version": spark.version, "total_rows": total,
           "master": "local[4]", "driver_memory": "2500m", "mode": args.mode,
           "method": "warmup x1 + timed x2 (best)", "queries": results}
    if wanted:
        out["partial"] = f"单条重测，仅包含 {[r['qid'] for r in results]}"
    out_path.write_text(json.dumps(out, ensure_ascii=False, indent=2), encoding="utf-8")
    print(f"\n结果已存: {out_path}")


def fn_timed(fn):
    t0 = time.perf_counter()
    rows = fn()
    return (time.perf_counter() - t0) * 1000, rows


if __name__ == "__main__":
    main()
