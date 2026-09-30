#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
dws.dws_traffic_hour_daily — 日 × 小时 分时流量汇总
====================================================
粒度：event_date + event_hour
口径：分时分析唯一入口（明细 1.37M 行/天 压到 24 行/天）。
幂等：静态分区 INSERT OVERWRITE，重跑只覆盖当天分区。

用法：
  python etl/dws/dws_traffic_hour_daily.py --dt 2019-10-01
  python etl/dws/dws_traffic_hour_daily.py --start 2019-10-01 --end 2019-10-31
"""

import argparse
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent))

from config.config import DBS, PARTITION_COL  # noqa: E402
from etl.utils import date_range, get_spark  # noqa: E402

TABLE = f"{DBS['dws']}.dws_traffic_hour_daily"
FACT = f"{DBS['dwd']}.dwd_event_fact"
P = PARTITION_COL

SQL = f"""
    INSERT OVERWRITE TABLE {TABLE} PARTITION ({P} = '{{dt}}')
    SELECT
        event_hour,
        COUNT(*)                                                                AS pv,
        COUNT(DISTINCT user_id)                                                 AS uv,
        SUM(CASE WHEN event_type = 'view'     THEN 1 ELSE 0 END)                AS view_pv,
        SUM(CASE WHEN event_type = 'cart'     THEN 1 ELSE 0 END)                AS cart_cnt,
        SUM(is_purchase)                                                        AS purchase_cnt,
        COUNT(DISTINCT CASE WHEN is_purchase = 1 THEN user_id END)              AS purchase_uv,
        SUM(CASE WHEN is_purchase = 1 THEN price ELSE 0 END)                    AS gmv,
        COUNT(DISTINCT user_session)                                            AS session_cnt
    FROM {FACT}
    WHERE {P} = '{{dt}}'
    GROUP BY event_hour
"""


def build(spark, dt):
    spark.sql(SQL.format(dt=dt))
    st = spark.sql(f"""
        SELECT COUNT(*) c, SUM(pv) pv, SUM(purchase_cnt) p
        FROM {TABLE} WHERE {P}='{dt}'
    """).collect()[0]
    print(f"  [{dt}] {TABLE} {st['c']} 行（小时）/ PV {st['pv']:,} / 购买 {st['p']:,}")
    return st["c"]


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--dt")
    ap.add_argument("--start")
    ap.add_argument("--end")
    args = ap.parse_args()

    days = [args.dt] if args.dt else date_range(args.start, args.end)
    spark = get_spark("DWS_TrafficHourDaily")
    spark.sparkContext.setLogLevel("ERROR")
    print(f"📊 {TABLE} 汇总 {len(days)} 天：{days[0]} ~ {days[-1]}")
    for d in days:
        build(spark, d)
    print("✅ 完成")
    spark.stop()


if __name__ == "__main__":
    main()
