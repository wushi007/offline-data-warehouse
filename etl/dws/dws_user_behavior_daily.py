#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
dws.dws_user_behavior_daily — 日 × 用户 行为汇总
=================================================
粒度：event_date + user_id（DAU / RFM 的基础）
口径：purchase_* 只统计 purchase 行为；purchase_amount 为当日购买金额。
幂等：静态分区 INSERT OVERWRITE，重跑只覆盖当天分区。

用法：
  python etl/dws/dws_user_behavior_daily.py --dt 2019-10-01
  python etl/dws/dws_user_behavior_daily.py --start 2019-10-01 --end 2019-10-31
"""

import argparse
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent))

from config.config import DBS, PARTITION_COL  # noqa: E402
from etl.utils import date_range, get_spark  # noqa: E402

TABLE = f"{DBS['dws']}.dws_user_behavior_daily"
FACT = f"{DBS['dwd']}.dwd_event_fact"
P = PARTITION_COL

SQL = f"""
    INSERT OVERWRITE TABLE {TABLE} PARTITION ({P} = '{{dt}}')
    SELECT
        user_id,
        COUNT(*)                                                                AS active_action_cnt,
        SUM(CASE WHEN is_purchase = 1 THEN price ELSE 0 END)                     AS purchase_amount,
        COUNT(DISTINCT CASE WHEN is_purchase = 1 THEN product_id END)            AS purchase_product_cnt,
        SUM(is_purchase)                                                         AS purchase_cnt
    FROM {FACT}
    WHERE {P} = '{{dt}}'
    GROUP BY user_id
"""


def build(spark, dt):
    spark.sql(SQL.format(dt=dt))
    st = spark.sql(f"""
        SELECT COUNT(*) c, SUM(purchase_cnt) p, SUM(purchase_amount) a
        FROM {TABLE} WHERE {P}='{dt}'
    """).collect()[0]
    print(f"  [{dt}] {TABLE} {st['c']:,} 行（当日活跃用户）/ 购买次数 {st['p']:,} / 金额 {st['a']:,.2f}")
    return st["c"]


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--dt")
    ap.add_argument("--start")
    ap.add_argument("--end")
    args = ap.parse_args()

    days = [args.dt] if args.dt else date_range(args.start, args.end)
    spark = get_spark("DWS_UserBehaviorDaily")
    spark.sparkContext.setLogLevel("ERROR")
    print(f"📊 {TABLE} 汇总 {len(days)} 天：{days[0]} ~ {days[-1]}")
    for d in days:
        build(spark, d)
    print("✅ 完成")
    spark.stop()


if __name__ == "__main__":
    main()
