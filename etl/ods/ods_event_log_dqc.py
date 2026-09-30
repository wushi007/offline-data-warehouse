#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
ods_dqc.py — ODS 层接入校验（前置 DQC，第一道质量闸门）
========================================================
校验不通过以非 0 退出码阻断下游（Airflow 中失败即阻断）。

校验项：
  1. 完整性：分区非空；行数较近 7 日日均波动 ±50% 告警、±80% 阻断
  2. 格式合法性：核心字段非空率 100%；event_time 可解析；price 合法数值
职责边界：只校验物理完整性与格式合法性，不做业务规则过滤（业务清洗在 DWD）。

用法：
  python scripts/ods_dqc.py --dt 2019-11-01
"""

import argparse
import statistics
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent))

from config.config import (ODS_PATH, DQC_ROWS_7D_DAYS, DQC_ROWS_WARN,  # noqa: E402
                    DQC_ROWS_BLOCK)
from etl.utils import get_spark  # noqa: E402


def main(dt: str):
    from pyspark.sql import functions as F

    spark = get_spark("ODS_DQC")
    spark.sparkContext.setLogLevel("ERROR")
    target = f"{ODS_PATH}/event_date={dt}"
    blocked = []

    # ============ 1. 完整性：分区非空 ============
    try:
        n_today = spark.read.parquet(target).count()
    except Exception:
        n_today = 0
    print(f"🔍 [{dt}] 分区行数: {n_today:,}")
    if n_today <= 0:
        print("❌ BLOCK: 分区为空，源数据疑似漏传")
        blocked.append("empty_partition")
    else:
        # 行数波动：近 7 日日均
        hist = []
        for d in (spark.read.parquet(ODS_PATH)
                  .select("event_date").distinct().collect()):
            dd = str(d["event_date"])
            if dd >= dt:
                continue
            c = spark.read.parquet(f"{ODS_PATH}/event_date={dd}").count()
            hist.append(c)
        if hist:
            avg = statistics.mean(hist[-DQC_ROWS_7D_DAYS:])
            ratio = abs(n_today - avg) / avg
            lvl = "⚠️ WARN" if ratio > DQC_ROWS_WARN else "✅ OK"
            if ratio > DQC_ROWS_BLOCK:
                lvl = "❌ BLOCK"
                blocked.append("row_count_surge")
            print(f"   行数波动: 今日 {n_today:,} vs 近7日均 {avg:,.0f} "
                  f"偏差 {ratio:.0%} → {lvl}")

    # ============ 2. 格式合法性 ============
    if n_today > 0:
        df = spark.read.parquet(target)
        total = n_today
        for col in ["user_id", "product_id", "user_session", "event_time"]:
            null_cnt = df.filter(F.col(col).isNull()).count()
            rate = null_cnt / total

            if rate == 0:
                status = "✅ OK"
            else:
                status = "❌ BLOCK"

            if rate > 0:
                blocked.append(f"null_{col}")
            print(f"   字段 {col:14s} 非空率: {1-rate:.4%} → {status}")
####################################################
#还要加上报警分支

            # def check_null_dq(df, cols, max_rate=0.0):
            #     total = df.count()
            #     agg_exprs = [
            #         (F.avg((F.col(c).isNull() | (F.trim(F.col(c)) == "")).cast("double"))).alias(c)
            #         for c in cols
            #     ]
            #     stats = df.select(agg_exprs).first()

            #     blocked = []
            #     for c in cols:
            #         rate = stats[c]
            #         icon = "✅" if rate <= max_rate else "❌"
            #         label = "PASS" if rate <= max_rate else "BLOCK"
            #         if rate > max_rate:
            #             blocked.append(f"null_{c}")

            #         print(f"{icon} {label:5} | {pad(c, 14)} 非空率: {(1-rate):>8.4%}")

            #     if blocked:
            #         raise RuntimeError(f"DQ BLOCKED: {', '.join(blocked)}")
            #     print("✅ 数据质量校验通过")

            # # 调用
            # check_null_dq(df, ["user_id", "product_id", "user_session", "event_time"], max_rate=0.0)





        # price 合法数值（NaN/Inf 视为非法）
        bad_price = df.filter(
            F.col("price").isNull() |
            #F.isnan(F.col("price")) |
            F.col("price").cast("double").isNull()
        ).count()


# price_d = F.col("price").cast("double")

# stat = df.agg(
#     F.count("*").alias("total"),
#     F.sum(F.when(price_d.isNull(), 1)).alias("null_cnt"),
#     F.sum(F.when(F.isnan(price_d), 1)).alias("nan_cnt"),
#     F.sum(F.when(price_d.isNotNull() & (price_d <= 0), 1)).alias("neg_cnt")
# ).collect()[0]

# total = max(stat["total"], 1)
# print(f"price null率: {stat['null_cnt']/total:.4%}")
# print(f"price NaN率: {stat['nan_cnt']/total:.4%}")
# print(f"price ≤0率: {stat['neg_cnt']/total:.4%}")
#一共四项，是数，不是数，是不是null


        total = max(df.count(), 1)
        bad_rate = bad_price / total
        print(f"   字段 {'price':14s} 非法数值率: {bad_rate:.4%} → "
              f"{'✅ OK' if bad_rate < 0.01 else '❌ BLOCK'}")
        if bad_rate >= 0.01:
            blocked.append("price_invalid")

    spark.stop()
    if blocked:
        print(f"\n🚫 ODS DQC 未通过：{blocked}")
        sys.exit(1)
    print("\n✅ ODS DQC 全部通过")

#可以加一个try加finally，保证spark.stop()一定会执行


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--dt", required=True)
    args = ap.parse_args()
    main(args.dt)
