#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
bench_spark.py — Spark 3.5.9 查询性能基准（与 StarRocks 同查询、同数据对比）
==============================================================================================
- 数据: HDFS dwd_event_fact 46,908,364 行 / 34 分区（与 StarRocks dwd_event_fact 同源）
- 配置: local[4]、driver 2500m、shuffle 48 分区（内存受限环境下适度调优，两边都合理配置后对比）
- 方法: 预热 1 次 + 计时 2 次取最优（StarRocks 侧为 3 次，报告中注明差异）
- 覆盖: Q1-Q7、Q9（仅基于事实表本身的查询；Q8/Q10 依赖 StarRocks 内 DWS/ADS 表，Spark 侧无法对等执行）
- 输出: starrocks/bench_results/spark_bench.json
- 用法: PYSPARK_PYTHON=<项目根目录>/.venv/bin/python \
        SPARK_HOME=/home/lst/apps/spark-3.5.9-bin-hadoop3 \
        <项目根目录>/.venv/bin/python starrocks/bench_spark.py
"""

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
    df = spark.read.parquet(FACT_PATH)
    total = df.count()
    print(f"Spark {spark.version} | {FACT_PATH} = {total:,} 行")
    print(f"配置: {spark.sparkContext.master}, driver {spark.conf.get('spark.driver.memory')}")
    print("方法: 预热 1 次 + 计时 2 次（取最优）\n")

    # (说明, 返回行数预期, 触发函数)
    QUERIES = [
        ("Q1 真实列扫描 SUM(price)", 1,
         lambda: df.agg(F.sum("price")).collect()),
        ("Q1m COUNT(*)", 1,
         lambda: [(df.count(),)]),
        ("Q2 分区裁剪（单天 COUNT）", 1,
         lambda: [(df.filter(F.col("event_date") == "2019-10-15").count(),)]),
        ("Q3 小基数分组（event_type）", 3,
         lambda: df.groupBy("event_type").count().collect()),
        ("Q4 按日聚合 GMV（34 组）", 34,
         lambda: df.groupBy("event_date").agg(F.count("*").alias("c"),
                                              F.sum("price").alias("s")).collect()),
        ("Q5 UV 精确去重（全量 user_id）", 1,
         lambda: df.agg(F.countDistinct("user_id")).collect()),
        ("Q6 大基数分组（product_id）", 169651,
         lambda: df.groupBy("product_id").agg(F.count("*").alias("c"),
                                              F.sum("price").alias("s")).collect()),
        ("Q7 漏斗 3 层 UV（单天）", 1,
         lambda: df.filter(F.col("event_date") == "2019-10-15")
                   .agg(F.countDistinct(F.when(F.col("event_type") == "view", "user_id")),
                        F.countDistinct(F.when(F.col("event_type") == "cart", "user_id")),
                        F.countDistinct(F.when(F.col("event_type") == "purchase", "user_id"))).collect()),
        ("Q9 购买明细按用户分组（37 万组）", 373611,
         lambda: df.filter(F.col("event_type") == "purchase")
                   .groupBy("user_id").agg(F.count("*").alias("c"),
                                           F.sum("price").alias("s")).collect()),
    ]

    results = []
    print(f"{'Q':<5}{'说明':<30}{'行数':>11}{'冷(ms)':>11}{'最优(ms)':>11}{'平均(ms)':>11}")
    print("-" * 82)
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
            best, avg = min(times), sum(times) / len(times)
            results.append({"qid": qname, "desc": desc, "rows": n,
                            "cold_ms": round(cold, 1), "best_ms": round(best, 1),
                            "avg_ms": round(avg, 1)})
            print(f"{qname:<5}{desc:<30}{n:>11,}{cold:>11.1f}{best:>11.1f}{avg:>11.1f}")
        except Exception as e:
            results.append({"qid": qname, "desc": desc, "error": str(e)[:100]})
            print(f"{qname:<5}{desc:<30}  ❌ {str(e)[:70]}")

    spark.stop()
    OUT_DIR.mkdir(parents=True, exist_ok=True)
    out = {"engine": "spark", "version": spark.version, "total_rows": total,
           "master": "local[4]", "driver_memory": "2500m",
           "method": "warmup x1 + timed x2 (best)", "queries": results}
    path = OUT_DIR / "spark_bench.json"
    path.write_text(json.dumps(out, ensure_ascii=False, indent=2), encoding="utf-8")
    print(f"\n📄 结果已存: {path}")


def fn_timed(fn):
    t0 = time.perf_counter()
    rows = fn()
    return (time.perf_counter() - t0) * 1000, rows


if __name__ == "__main__":
    main()
