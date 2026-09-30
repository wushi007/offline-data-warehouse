#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
ads.ads_traffic_hour_daily — 分时大盘（运营时段表）
===================================================
来源 dws_traffic_hour_daily；粒度：event_date × event_hour（31 天 = 744 行）。
在 DWS 分时表上加"报表语义"：时段分桶 / PV 与订单占比 / 峰值小时标记 / 分时转化率。

用法：
  python etl/ads/ads_traffic_hour_daily.py --dt 2019-10-01
  python etl/ads/ads_traffic_hour_daily.py --start 2019-10-01 --end 2019-10-31
"""

import argparse
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent))

from config.config import DBS, PARTITION_COL  # noqa: E402
from etl.utils import date_range, get_spark  # noqa: E402

TABLE = f"{DBS['ads']}.ads_traffic_hour_daily"
DWS_HOUR = f"{DBS['dws']}.dws_traffic_hour_daily"
P = PARTITION_COL

SQL = f"""
    INSERT OVERWRITE TABLE {TABLE} PARTITION ({P} = '{{dt}}')
    SELECT
        event_hour,
        CASE WHEN event_hour <= 6  THEN '凌晨(0-6)'
             WHEN event_hour <= 12 THEN '上午(7-12)'
             WHEN event_hour <= 18 THEN '下午(13-18)'
             ELSE '晚间(19-23)' END                                AS time_bucket,
        pv,
        uv,
        purchase_cnt,
        purchase_uv,
        gmv,
        ROUND(pv / SUM(pv) OVER () * 100, 4)                       AS pv_share,
        ROUND(purchase_cnt / SUM(purchase_cnt) OVER () * 100, 4)    AS order_share,
        CASE WHEN uv > 0 THEN ROUND(purchase_uv / uv * 100, 4) END  AS purchase_rate,
        CASE WHEN pv = MAX(pv) OVER () THEN 1 ELSE 0 END            AS is_peak_hour
    FROM {DWS_HOUR}
    WHERE {P} = '{{dt}}'
"""


def build(spark, dt):
    spark.sql(SQL.format(dt=dt))
    peak = spark.sql(f"""
        SELECT event_hour, pv, pv_share FROM {TABLE}
        WHERE {P}='{dt}' AND is_peak_hour = 1 ORDER BY pv DESC LIMIT 1
    """).collect()[0]
    n = spark.sql(f"SELECT COUNT(*) c FROM {TABLE} WHERE {P}='{dt}'").collect()[0]["c"]
    print(f"  [{dt}] {TABLE} {n} 行 | 峰值小时 {peak['event_hour']} 点"
          f"（PV {peak['pv']:,}，占全天 {peak['pv_share']:.2f}%）")
    return n


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--dt")
    ap.add_argument("--start")
    ap.add_argument("--end")
    args = ap.parse_args()

    days = [args.dt] if args.dt else date_range(args.start, args.end)
    spark = get_spark("ADS_TrafficHourDaily")
    spark.sparkContext.setLogLevel("ERROR")
    print(f"📈 {TABLE} {len(days)} 天：{days[0]} ~ {days[-1]}")
    for d in days:
        build(spark, d)
    print("✅ 完成")
    spark.stop()


if __name__ == "__main__":
    main()
