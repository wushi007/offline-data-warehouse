#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
ads.ads_product_hot_rank_daily — 商品热榜 Top100（逐日）
========================================================
来源 dws_product_daily；粒度：每天 Top100（31 天 = 3,100 行）。
排名口径：销售额降序，同额按销量降序。

用法：
  python etl/ads/ads_product_hot_rank_daily.py --dt 2019-10-01
  python etl/ads/ads_product_hot_rank_daily.py --start 2019-10-01 --end 2019-10-31
"""

import argparse
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent))

from config.config import DBS, PARTITION_COL  # noqa: E402
from etl.utils import date_range, get_spark  # noqa: E402

TABLE = f"{DBS['ads']}.ads_product_hot_rank_daily"
DWS_PRODUCT = f"{DBS['dws']}.dws_product_daily"
P = PARTITION_COL
TOP_N = 100

SQL = f"""
    INSERT OVERWRITE TABLE {TABLE} PARTITION ({P} = '{{dt}}')
    SELECT `rank`, product_id, category_id, brand, sales_cnt, sales_amount, exposure_uv
    FROM (
        SELECT
            ROW_NUMBER() OVER (ORDER BY sales_amount DESC, sales_cnt DESC) AS `rank`,
            product_id, category_id, brand, sales_cnt, sales_amount, exposure_uv
        FROM {DWS_PRODUCT} WHERE {P} = '{{dt}}'
    ) t WHERE `rank` <= {TOP_N}
"""


def build(spark, dt):
    spark.sql(SQL.format(dt=dt))
    rows = spark.sql(f"""
        SELECT COUNT(*) c, SUM(sales_amount) a FROM {TABLE} WHERE {P}='{dt}'
    """).collect()[0]
    top1 = spark.sql(f"""
        SELECT product_id, brand, sales_amount FROM {TABLE}
        WHERE {P}='{dt}' AND `rank` = 1
    """).collect()[0]
    print(f"  [{dt}] {TABLE} {rows['c']} 行 / 榜内销售额 {rows['a']:,.2f}"
          f" | Top1 商品 {top1['product_id']}（{top1['brand']}）{top1['sales_amount']:,.2f}")
    return rows["c"]


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--dt")
    ap.add_argument("--start")
    ap.add_argument("--end")
    args = ap.parse_args()

    days = [args.dt] if args.dt else date_range(args.start, args.end)
    spark = get_spark("ADS_ProductHotRankDaily")
    spark.sparkContext.setLogLevel("ERROR")
    print(f"📈 {TABLE} {len(days)} 天：{days[0]} ~ {days[-1]}")
    for d in days:
        build(spark, d)
    print("✅ 完成")
    spark.stop()


if __name__ == "__main__":
    main()
