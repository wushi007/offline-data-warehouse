#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
ads.ads_trade_daily — 交易日报（运营大盘）
来源 dws_user_behavior_daily；粒度每天 1 行，指标为 GMV、订单数、买家数、客单价、ARPPU。
用法：python etl/ads/ads_trade_daily.py --dt 2019-10-01，或 --start/--end 指定区间。
"""

import argparse
import statistics
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent))

from config.config import (  # noqa: E402
    DBS, DQC_GMV_DAYS, DQC_GMV_DROP_BLOCK, DQC_GMV_WARN, PARTITION_COL,
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


def check_gmv_volatility(spark, dt, days=DQC_GMV_DAYS,
                         warn=DQC_GMV_WARN, drop_block=DQC_GMV_DROP_BLOCK):
    """GMV 波动校验：今日与近 days 天 GMV 中位数比较，返回 (是否阻断, 说明)。

    中位数不受单日极值影响；历史不足 days 天时跳过。
    下跌超 drop_block 视为购买事件漏传并阻断，上涨超 warn 仅告警。
    """
    hist = spark.sql(f"""
        SELECT gmv FROM {TABLE} WHERE {P} < '{dt}' ORDER BY {P} DESC LIMIT {days}
    """).collect()
    if len(hist) < days:
        return False, f"历史仅 {len(hist)} 天（不足 {days} 天），跳过 GMV 波动校验"

    base = statistics.median([r["gmv"] for r in hist])
    today = spark.sql(f"SELECT gmv FROM {TABLE} WHERE {P} = '{dt}'").collect()[0]["gmv"]
    if today is None:
        return True, "今日 GMV 为空（疑似上游漏数）→ 阻断"
    if not base or base <= 0:
        return False, "近 7 日中位数为 0，跳过 GMV 波动校验"

    diff = (today - base) / base
    head = (f"GMV 波动：今日 {today:,.2f} vs 近 {days} 日中位数 {base:,.2f} → "
            f"偏差 {diff:+.1%}")

    if diff >= 0:
        note = f"（超过 {warn:.0%} 告警线，上涨不阻断）" if diff > warn else ""
        return False, head + note
    if -diff > drop_block:
        return True, head + f"（下跌超过 {drop_block:.0%}，疑似购买事件漏传 → 阻断）"
    return False, head + (f"（超过 {warn:.0%} 告警线）" if -diff > warn else "")


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--dt")
    ap.add_argument("--start")
    ap.add_argument("--end")
    args = ap.parse_args()

    days = [args.dt] if args.dt else date_range(args.start, args.end)
    spark = get_spark("ADS_TradeDaily")
    spark.sparkContext.setLogLevel("ERROR")
    print(f"{TABLE} {len(days)} 天：{days[0]} ~ {days[-1]}")

    blocked = []
    for d in days:
        build(spark, d)
        is_block, msg = check_gmv_volatility(spark, d)
        print(f"  {'FAIL' if is_block else 'OK'} [{d}] {msg}")
        if is_block:
            blocked.append(d)

    spark.stop()
    if blocked:
        print(f"\nFAIL: GMV 下跌超阈值（疑似购买事件漏传）：{'、'.join(blocked)}")
        sys.exit(1)          # 非 0 退出码使 Airflow 任务失败并触发告警
    print("OK: 完成")


if __name__ == "__main__":
    main()
