#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
dws.dws_product_daily — 日 × 商品 行为汇总（星型宽表）
粒度 event_date + product_id；类目/品牌取快照值，聚合不 JOIN 维度。
口径：各 *_uv 为事件去重用户数，sales_* 只统计 purchase；静态分区覆盖写，重跑幂等。
用法：python etl/dws/dws_product_daily.py --dt 2019-10-01，或 --start/--end 指定区间。
"""

import argparse
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent))

from config.config import DBS, PARTITION_COL  # noqa: E402
from etl.utils import date_range, get_spark  # noqa: E402

TABLE = f"{DBS['dws']}.dws_product_daily"
FACT = f"{DBS['dwd']}.dwd_event_fact"
P = PARTITION_COL

SQL = f"""
    INSERT OVERWRITE TABLE {TABLE} PARTITION ({P} = '{{dt}}')
    SELECT
        product_id,
        -- 类目/品牌取代表值后再聚合：按 (product_id, category_id, brand) 分组会因
        -- brand 空值把同一商品拆成多行，破坏主键并重复计数去重指标。
        MAX(category_id) AS category_id,
        MAX(brand)       AS brand,
        COUNT(DISTINCT CASE WHEN event_type = 'view'     THEN user_id END) AS exposure_uv,
        COUNT(DISTINCT CASE WHEN event_type = 'cart'     THEN user_id END) AS cart_uv,
        COUNT(DISTINCT CASE WHEN event_type = 'purchase' THEN user_id END) AS purchase_uv,
        SUM(CASE WHEN event_type = 'purchase' THEN 1 ELSE 0 END)            AS sales_cnt,
        SUM(CASE WHEN event_type = 'purchase' THEN price ELSE 0 END)        AS sales_amount
    FROM {FACT}
    WHERE {P} = '{{dt}}'
    GROUP BY product_id
"""


def build(spark, dt):
    spark.sql(SQL.format(dt=dt))
    st = spark.sql(f"""
        SELECT COUNT(*) c, SUM(sales_cnt) p, SUM(sales_amount) a
        FROM {TABLE} WHERE {P}='{dt}'
    """).collect()[0]
    print(f"  [{dt}] {TABLE} {st['c']:,} 行 / 销量 {st['p']:,} / 销售额 {st['a']:,.2f}")
    return st["c"]


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--dt")
    ap.add_argument("--start")
    ap.add_argument("--end")
    args = ap.parse_args()

    days = [args.dt] if args.dt else date_range(args.start, args.end)
    spark = get_spark("DWS_ProductDaily")
    spark.sparkContext.setLogLevel("ERROR")
    print(f"{TABLE} 汇总 {len(days)} 天：{days[0]} ~ {days[-1]}")
    for d in days:
        build(spark, d)
    print("OK: 完成")
    spark.stop()


if __name__ == "__main__":
    main()
