#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
dws.dws_user_session_daily — 日 × 用户 × 会话 行为汇总
======================================================
粒度：event_date + user_id + user_session
口径：不 JOIN 维度，直接对事实表 GROUP BY；会话时长 = GREATEST(MAX-MIN, 0) 秒。
幂等：静态分区 INSERT OVERWRITE，重跑只覆盖当天分区。

用法：
  python etl/dws/dws_user_session_daily.py --dt 2019-10-01
  python etl/dws/dws_user_session_daily.py --start 2019-10-01 --end 2019-10-31
"""

import argparse
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent))

from config.config import DBS, PARTITION_COL  # noqa: E402
from etl.utils import date_range, get_spark  # noqa: E402

TABLE = f"{DBS['dws']}.dws_user_session_daily"
FACT = f"{DBS['dwd']}.dwd_event_fact"
P = PARTITION_COL

SQL = f"""
    INSERT OVERWRITE TABLE {TABLE} PARTITION ({P} = '{{dt}}')
    SELECT
        user_id,
        user_session,
        SUM(CASE WHEN event_type = 'view'             THEN 1 ELSE 0 END) AS view_cnt,
        SUM(CASE WHEN event_type = 'cart'             THEN 1 ELSE 0 END) AS cart_cnt,
        SUM(CASE WHEN event_type = 'purchase'         THEN 1 ELSE 0 END) AS purchase_cnt,
        SUM(CASE WHEN event_type = 'remove_from_cart' THEN 1 ELSE 0 END) AS remove_cart_cnt,
        CAST(GREATEST(UNIX_TIMESTAMP(MAX(event_time)) - UNIX_TIMESTAMP(MIN(event_time)), 0) AS BIGINT)
                                                                        AS session_duration_sec
    FROM {FACT}
    WHERE {P} = '{{dt}}'
    GROUP BY user_id, user_session
"""


def build(spark, dt):
    spark.sql(SQL.format(dt=dt))
    st = spark.sql(f"SELECT COUNT(*) c, SUM(purchase_cnt) p FROM {TABLE} WHERE {P}='{dt}'").collect()[0]
    print(f"  [{dt}] {TABLE} {st['c']:,} 行 / 购买次数 {st['p']:,}")
    return st["c"]


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--dt")
    ap.add_argument("--start")
    ap.add_argument("--end")
    args = ap.parse_args()

    days = [args.dt] if args.dt else date_range(args.start, args.end)
    spark = get_spark("DWS_UserSessionDaily")
    spark.sparkContext.setLogLevel("ERROR")
    print(f"📊 {TABLE} 汇总 {len(days)} 天：{days[0]} ~ {days[-1]}")
    for d in days:
        build(spark, d)
    print("✅ 完成")
    spark.stop()


if __name__ == "__main__":
    main()
