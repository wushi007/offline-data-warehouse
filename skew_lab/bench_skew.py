#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
bench_skew.py —— 数据倾斜：复现、加盐两阶段、AQE 对照
场景：按 product_id 聚合商品销量，其中 1 个爆款商品占全表 45% 的行。
各方案需分进程执行：Session 缓存、AQE 学习与 JVM 预热会干扰同进程内的对比。
判据用 max/p50 的倍数而非总耗时，倾斜时总耗时会被大量已完成的 task 稀释。
用法：python skew_lab/bench_skew.py --mode naive|salted_all|salted_hot|rebalance
  其中 naive 复现倾斜，salted_all 全量加盐，salted_hot 定向加盐，rebalance 为 AQE 对照。
"""

import argparse
import sys
from pathlib import Path

_HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(_HERE.parent))
sys.path.insert(0, str(_HERE))

from pyspark.sql import functions as F  # noqa: E402

from common import (  # noqa: E402
    DATA_PATH, append_json, clear_event_logs, make_spark, measure,
    parse_task_metrics, print_result, summarize,
)

OUT = _HERE / "results" / "skew_bench.json"
SHUFFLE_PARTITIONS = 200   # 分区多，热点 key 的集中效应才看得出来
SALT_N = 64                # 加盐桶数：把爆款打散成 64 份
HOT_ROWS = 500_000         # 热点 key 判定阈值：行数超 50 万（总量的 1%）才算热点
SAMPLE_FRAC = 0.02         # 热点判定用 2% 采样，避免为治倾斜先跑一遍同样倾斜的作业

# 统计口径，四种方案保持一致
# 必须是函数而非模块级常量：F.sum("col") 需要已激活的 SparkSession
def aggs():
    return [
        F.sum("sales_amount").alias("gmv"),
        F.sum("quantity").alias("sales_cnt"),
        F.count("*").alias("row_cnt"),
    ]


def two_stage(salted):
    """两阶段聚合：先按 (key, salt) 局部聚合，再去盐合并。

    SUM / COUNT 都是可加可分解的，盐只影响分组粒度，不影响最终结果。
    """
    s1 = salted.groupBy("product_id", "salt").agg(
        F.sum("sales_amount").alias("gmv"),
        F.sum("quantity").alias("sales_cnt"),
        F.count("*").alias("row_cnt"))
    return s1.groupBy("product_id").agg(
        F.sum("gmv").alias("gmv"),
        F.sum("sales_cnt").alias("sales_cnt"),
        F.sum("row_cnt").alias("row_cnt"))


def run_naive(spark, df):
    """基线：直接 GROUP BY。热点 key 的 2250 万行全部落到同一个 reduce task。"""
    agg = df.groupBy("product_id").agg(*aggs())
    return measure(spark, "A. 原始 groupBy（复现倾斜）", lambda: len(agg.collect()))


def run_salted_all(spark, df):
    """全量加盐，反例。

    所有 key 都加 64 个盐桶：40 万个长尾商品（各约几十行）的中间结果
    从 40 万组膨胀到 2560 万组，热点被拆散但整体反而慢 3.3×。
    """
    salted = df.withColumn("salt", (F.rand(7) * SALT_N).cast("int"))
    return measure(spark, f"B. 全量加盐（{SALT_N} 桶 × 全部 key）",
                   lambda: len(two_stage(salted).collect()))


def run_salted_hot(spark, df):
    """定向加盐：只给超过阈值的少数热点 key 加盐。

    其余 key 的 salt 固定为 0，中间结果不膨胀；热点判定的成本计入总耗时。
    """
    def job():
        # 热点判定须用采样：全表 groupBy + count 同样按 product_id 分组，
        # 会把热点压到单个 task（改用采样后最慢 task 从 2113ms 降至 6xx ms）。
        sampled = df.sample(fraction=SAMPLE_FRAC, seed=1)
        hot = [r["product_id"] for r in
               sampled.groupBy("product_id").count()
                      .filter(F.col("count") > HOT_ROWS * SAMPLE_FRAC).collect()]
        salted = df.withColumn(
            "salt",
            F.when(F.col("product_id").isin(hot),
                   (F.rand(7) * SALT_N).cast("int")).otherwise(F.lit(0)))
        return len(two_stage(salted).collect())

    return measure(spark, f"C. 定向加盐（只对 >{HOT_ROWS:,} 行的 key 加 {SALT_N} 桶）", job)


def run_rebalance(spark, df):
    """AQE 对照：REBALANCE hint 让 AQE 在运行时切开倾斜分区。

    不能用 df.hint()：传字符串或 F.col 均报参数错误，须用 SQL 的 /*+ REBALANCE */。
    """
    df.createOrReplaceTempView("skew_sales")
    agg = spark.sql("""
        SELECT /*+ REBALANCE(product_id) */
               product_id,
               SUM(sales_amount) AS gmv,
               SUM(quantity)     AS sales_cnt,
               COUNT(*)          AS row_cnt
        FROM skew_sales
        GROUP BY product_id
    """)
    return measure(spark, "D. AQE REBALANCE hint（自动拆倾斜分区）",
                   lambda: len(agg.collect()))


MODES = {
    "naive": run_naive,
    "salted_all": run_salted_all,
    "salted_hot": run_salted_hot,
    "rebalance": run_rebalance,
}


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--mode", required=True, choices=list(MODES))
    args = ap.parse_args()

    clear_event_logs()
    # 控制变量：naive/salted 关闭 AQE 分区合并，否则 200 个分区会被并成约 9 个，
    # 三种方案不在同一起跑线上
    extra = {}
    if args.mode in ("naive", "salted_all", "salted_hot"):
        extra["spark.sql.adaptive.coalescePartitions.enabled"] = "false"
    if args.mode == "rebalance":
        # AQE 自动拆倾斜分区，需要 REBALANCE hint 配合
        extra = {
            "spark.sql.adaptive.optimizeSkewsInRebalancePartitions.enabled": "true",
            "spark.sql.adaptive.advisoryPartitionSizeInBytes": "16MB",
        }
    spark = make_spark(f"Skew_{args.mode}", memory="3g",
                       shuffle_partitions=SHUFFLE_PARTITIONS, extra=extra)

    df = spark.read.parquet(DATA_PATH)
    total = df.count()
    print(f"Spark {spark.version} | {DATA_PATH}")
    print(f"数据 {total:,} 行 | shuffle 分区 {SHUFFLE_PARTITIONS} | 方案 {args.mode}")

    if args.mode == "naive":
        print("\n热点 key 分布（Top6）:")
        for r in (df.groupBy("product_id").count()
                  .orderBy(F.desc("count")).limit(6).collect()):
            print(f"   product_id={r['product_id']:<10} {r['count']:>12,} 行"
                  f"  占 {r['count'] / total * 100:5.1f}%")

    result = MODES[args.mode](spark, df)
    spark.stop()   # 事件日志在 driver 退出时才写完，必须先 stop 再解析

    tasks = parse_task_metrics()
    result["all"] = summarize(tasks)
    result["reduce"] = summarize(tasks, only_shuffle_read=True)

    print_result(result)
    append_json(OUT, result)
    print(f"\n📄 已追加到 {OUT}")


if __name__ == "__main__":
    main()
