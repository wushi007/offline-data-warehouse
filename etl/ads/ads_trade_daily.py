#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
ads.ads_trade_daily — 交易日报（运营大盘）
==========================================
来源 dws_user_behavior_daily；粒度：每天 1 行。
指标：GMV / 订单数 / 购买用户数 / 客单价 / ARPPU。

用法：
  python etl/ads/ads_trade_daily.py --dt 2019-10-01
  python etl/ads/ads_trade_daily.py --start 2019-10-01 --end 2019-10-31
"""

import argparse
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent))

from config.config import (  # noqa: E402
    DBS, DQC_GMV_DAYS, DQC_GMV_VOLATILITY, PARTITION_COL,
)
from etl.utils import date_range, get_spark  # noqa: E402

TABLE = f"{DBS['ads']}.ads_trade_daily"
DWS_USER = f"{DBS['dws']}.dws_user_behavior_daily"
P = PARTITION_COL

SQL = f"""
    INSERT OVERWRITE TABLE {TABLE} PARTITION ({P} = '{{dt}}')
    SELECT
        gmv,
        order_cnt,
        buyer_cnt,
        CASE WHEN order_cnt > 0 THEN gmv / order_cnt ELSE 0 END AS avg_order_value,
        CASE WHEN buyer_cnt > 0 THEN gmv / buyer_cnt ELSE 0 END AS arppu
    FROM (
        SELECT
            SUM(purchase_amount)                                  AS gmv,
            SUM(purchase_cnt)                                     AS order_cnt,
            SUM(CASE WHEN purchase_cnt > 0 THEN 1 ELSE 0 END)     AS buyer_cnt
        FROM {DWS_USER} WHERE {P} = '{{dt}}'
    ) t
"""


def build(spark, dt):
    spark.sql(SQL.format(dt=dt))
    t = spark.sql(f"SELECT * FROM {TABLE} WHERE {P}='{dt}'").collect()[0]
    print(f"  [{dt}] {TABLE} GMV {t['gmv']:,.2f} / 订单 {t['order_cnt']:,} / "
          f"买家 {t['buyer_cnt']:,} / 客单价 {t['avg_order_value']:.2f} / ARPPU {t['arppu']:.2f}")
    return t["gmv"]


def check_gmv_volatility(spark, dt, days=DQC_GMV_DAYS, limit=DQC_GMV_VOLATILITY):
    """GMV 波动校验：今日 vs 最近 days 天均值（不含今日）。返回 (是否通过, 说明)。

    这是**指标异常**校验，不是正确性闸门 —— 数据照写，只是把异常暴露出来。
    历史不足 days 天时**跳过**（判不了就别误报）：月初第 1~7 天、以及补历史的头几天都会跳过。
    """
    hist = spark.sql(f"""
        SELECT gmv FROM {TABLE} WHERE {P} < '{dt}' ORDER BY {P} DESC LIMIT {days}
    """).collect()
    if len(hist) < days:
        return True, f"历史仅 {len(hist)} 天（不足 {days} 天），跳过 GMV 波动校验"

    avg = sum(r["gmv"] for r in hist) / len(hist)
    today = spark.sql(f"SELECT gmv FROM {TABLE} WHERE {P} = '{dt}'").collect()[0]["gmv"]
    if not avg or avg <= 0:
        return True, "近 7 日均值为 0，跳过 GMV 波动校验"

    dev = abs(today - avg) / avg
    msg = (f"GMV 波动：今日 {today:,.2f} vs 近 {days} 日均 {avg:,.2f} → "
           f"偏差 {dev:.1%}（阈值 {limit:.0%}）")
    return dev <= limit, msg


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--dt")
    ap.add_argument("--start")
    ap.add_argument("--end")
    args = ap.parse_args()

    days = [args.dt] if args.dt else date_range(args.start, args.end)
    spark = get_spark("ADS_TradeDaily")
    spark.sparkContext.setLogLevel("ERROR")
    print(f"📈 {TABLE} {len(days)} 天：{days[0]} ~ {days[-1]}")

    blocked = []
    for d in days:
        build(spark, d)
        ok, msg = check_gmv_volatility(spark, d)
        print(f"  {'✅' if ok else '❌'} [{d}] {msg}")
        if not ok:
            blocked.append(f"{d}({msg.split('偏差 ')[-1]})")

    spark.stop()
    if blocked:
        print(f"\n❌ GMV 波动超阈值：{'、'.join(blocked)}")
        sys.exit(1)          # 非 0 → Airflow 任务失败 → 触发 quality_warn 告警
    print("✅ 完成")


if __name__ == "__main__":
    main()
