#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
ads.ads_conversion_funnel_daily — 转化漏斗日报
==============================================
来源 dwd_event_fact（**回 DWD 而非用 DWS**：漏斗 UV 必须跨商品精确去重，
不能拿商品级 UV 累加）；粒度：每天 1 行。

口径提醒：cart_to_purchase_rate 可能 >100%——用户可直接下单绕过加购，
属真实业务特征，不做截断。因此漏斗不变量用 `view_uv >= cart_uv 且 >= purchase_uv`。

用法：
  python etl/ads/ads_conversion_funnel_daily.py --dt 2019-10-01
  python etl/ads/ads_conversion_funnel_daily.py --start 2019-10-01 --end 2019-10-31
"""

import argparse
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent))

from config.config import DBS, PARTITION_COL  # noqa: E402
from etl.utils import date_range, get_spark  # noqa: E402

TABLE = f"{DBS['ads']}.ads_conversion_funnel_daily"
FACT = f"{DBS['dwd']}.dwd_event_fact"
P = PARTITION_COL

SQL = f"""
    INSERT OVERWRITE TABLE {TABLE} PARTITION ({P} = '{{dt}}')
    SELECT
        view_uv, cart_uv, purchase_uv,
        CASE WHEN view_uv > 0 THEN cart_uv     / view_uv ELSE 0 END AS view_to_cart_rate,
        CASE WHEN view_uv > 0 THEN purchase_uv / view_uv ELSE 0 END AS view_to_purchase_rate,
        CASE WHEN cart_uv > 0 THEN purchase_uv / cart_uv ELSE 0 END AS cart_to_purchase_rate
    FROM (
        SELECT
            COUNT(DISTINCT CASE WHEN event_type = 'view'     THEN user_id END) AS view_uv,
            COUNT(DISTINCT CASE WHEN event_type = 'cart'     THEN user_id END) AS cart_uv,
            COUNT(DISTINCT CASE WHEN event_type = 'purchase' THEN user_id END) AS purchase_uv
        FROM {FACT} WHERE {P} = '{{dt}}'
    ) t
"""


def build(spark, dt):
    spark.sql(SQL.format(dt=dt))
    f = spark.sql(f"SELECT * FROM {TABLE} WHERE {P}='{dt}'").collect()[0]
    inv = "✅" if f["view_uv"] >= f["cart_uv"] and f["view_uv"] >= f["purchase_uv"] else "❌"
    print(f"  [{dt}] {TABLE} 漏斗 UV {f['view_uv']:,}→{f['cart_uv']:,}→{f['purchase_uv']:,} {inv}"
          f" | 浏览→加购 {f['view_to_cart_rate'] * 100:.2f}%"
          f" / 浏览→购买 {f['view_to_purchase_rate'] * 100:.2f}%"
          f" / 加购→购买 {f['cart_to_purchase_rate'] * 100:.2f}%")
    return f["view_uv"]


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--dt")
    ap.add_argument("--start")
    ap.add_argument("--end")
    args = ap.parse_args()

    days = [args.dt] if args.dt else date_range(args.start, args.end)
    spark = get_spark("ADS_ConversionFunnelDaily")
    spark.sparkContext.setLogLevel("ERROR")
    print(f"📈 {TABLE} {len(days)} 天：{days[0]} ~ {days[-1]}")
    for d in days:
        build(spark, d)
    print("✅ 完成")
    spark.stop()


if __name__ == "__main__":
    main()
