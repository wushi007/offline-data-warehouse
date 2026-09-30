#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
ads.ads_session_behavior_daily — 会话行为日报
==============================================
来源 dws_user_session_daily；粒度：每天 1 行。
指标：会话数 / 平均会话时长 / 平均行为数 / 加购转化率。

用法：
  python etl/ads/ads_session_behavior_daily.py --dt 2019-10-01
  python etl/ads/ads_session_behavior_daily.py --start 2019-10-01 --end 2019-10-31
"""

import argparse
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent))

from config.config import DBS, PARTITION_COL  # noqa: E402
from etl.utils import date_range, get_spark  # noqa: E402

TABLE = f"{DBS['ads']}.ads_session_behavior_daily"
DWS_SESSION = f"{DBS['dws']}.dws_user_session_daily"
P = PARTITION_COL

SQL = f"""
    INSERT OVERWRITE TABLE {TABLE} PARTITION ({P} = '{{dt}}')
    SELECT
        COUNT(*)                                                       AS session_cnt,
        AVG(session_duration_sec)                                      AS avg_duration_sec,
        AVG(view_cnt + cart_cnt + purchase_cnt + remove_cart_cnt)       AS avg_action_cnt,
        CASE WHEN SUM(cart_cnt) > 0 THEN SUM(purchase_cnt) / SUM(cart_cnt) ELSE 0 END AS cart_purchase_rate
    FROM {DWS_SESSION} WHERE {P} = '{{dt}}'
"""


def build(spark, dt):
    spark.sql(SQL.format(dt=dt))
    s = spark.sql(f"SELECT * FROM {TABLE} WHERE {P}='{dt}'").collect()[0]
    print(f"  [{dt}] {TABLE} 会话 {s['session_cnt']:,} / 均时长 {s['avg_duration_sec']:.1f}s"
          f" / 均行为 {s['avg_action_cnt']:.2f} / 加购转化 {s['cart_purchase_rate'] * 100:.2f}%")
    return s["session_cnt"]


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--dt")
    ap.add_argument("--start")
    ap.add_argument("--end")
    args = ap.parse_args()

    days = [args.dt] if args.dt else date_range(args.start, args.end)
    spark = get_spark("ADS_SessionBehaviorDaily")
    spark.sparkContext.setLogLevel("ERROR")
    print(f"📈 {TABLE} {len(days)} 天：{days[0]} ~ {days[-1]}")
    for d in days:
        build(spark, d)
    print("✅ 完成")
    spark.stop()


if __name__ == "__main__":
    main()
