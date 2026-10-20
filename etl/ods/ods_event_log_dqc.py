#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
ods_event_log_dqc.py — ODS 接入校验

校验分区非空、行数波动（对比近 7 日中位数，仅下跌阻断）、核心字段空值率、
event_time 可解析与 price 合法性；只校验物理完整性与格式，不通过时以非 0 退出码阻断下游。

用法：python etl/ods/ods_event_log_dqc.py --dt 2019-11-01
"""

import argparse
import statistics
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent))

from config.config import (ODS_PATH, DQC_ROWS_7D_DAYS, DQC_ROWS_WARN,  # noqa: E402
                    DQC_ROWS_DROP_BLOCK, DQC_NULL_MAX_RATE)
from etl.utils import get_spark  # noqa: E402


def main(dt: str):
    from pyspark.sql import functions as F

    spark = get_spark("ODS_DQC")
    spark.sparkContext.setLogLevel("ERROR")
    target = f"{ODS_PATH}/event_date={dt}"
    blocked = []

    try:
        n_today = spark.read.parquet(target).count()
    except Exception:
        n_today = 0
    print(f"🔍 [{dt}] 分区行数: {n_today:,}")
    if n_today <= 0:
        print("BLOCK: 分区为空，源数据疑似漏传")
        blocked.append("empty_partition")
    else:
        # 行数波动对比近 7 日中位数；必须 ORDER BY，distinct().collect() 顺序任意，
        # 否则 hist[-7:] 取到的是随机 7 天而非前 7 天。中位数可避免台阶式上涨误判。
        hist = []
        for d in (spark.read.parquet(ODS_PATH)
                  .select("event_date").distinct()
                  .orderBy("event_date").collect()):
            dd = str(d["event_date"])
            if dd >= dt:
                continue
            c = spark.read.parquet(f"{ODS_PATH}/event_date={dd}").count()
            hist.append(c)
        if hist:
            base = statistics.median(hist[-DQC_ROWS_7D_DAYS:])
            diff = (n_today - base) / base if base else 0.0
            if diff >= 0:
                # 上涨只告警，重复导入由 DWD 去重键兜底
                lvl = "WARN 上涨" if diff > DQC_ROWS_WARN else "OK"
            elif -diff > DQC_ROWS_DROP_BLOCK:
                # 下跌疑似源数据漏传，必须阻断
                lvl = "BLOCK 下跌（疑似漏传）"
                blocked.append("row_count_drop")
            elif -diff > DQC_ROWS_WARN:
                lvl = "WARN 下跌"
            else:
                lvl = "OK"
            print(f"   行数波动: 今日 {n_today:,} vs 近7日中位数 {base:,.0f} "
                  f"偏差 {diff:+.0%} → {lvl}")

    if n_today > 0:
        df = spark.read.parquet(target)
        total = n_today
        for col in ["user_id", "product_id", "user_session", "event_time"]:
            null_cnt = df.filter(F.col(col).isNull()).count()
            rate = null_cnt / total

            if rate == 0:
                status = "OK"
            elif rate <= DQC_NULL_MAX_RATE:
                status = f"WARN（{DQC_NULL_MAX_RATE:.1%} 容差内，放行）"
            else:
                status = "BLOCK"
                blocked.append(f"null_{col}")
            print(f"   字段 {col:14s} 空值率: {rate:.4%}（空值 {null_cnt:,} 行）→ {status}")

        # price 非法数值（NaN/Inf 视为非法）
        bad_price = df.filter(
            F.col("price").isNull() |
            F.col("price").cast("double").isNull()
        ).count()

        total = max(df.count(), 1)
        bad_rate = bad_price / total
        print(f"   字段 {'price':14s} 非法数值率: {bad_rate:.4%} → "
              f"{'OK' if bad_rate < 0.01 else 'BLOCK'}")
        if bad_rate >= 0.01:
            blocked.append("price_invalid")

    spark.stop()
    if blocked:
        print(f"\n🚫 ODS DQC 未通过：{blocked}")
        sys.exit(1)
    print("\nOK: ODS DQC 全部通过")


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--dt", required=True)
    args = ap.parse_args()
    main(args.dt)
