#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
ads.ads_user_retention_daily — 用户留存日报
来源 dws_user_behavior_daily；粒度每个基准日 D0 一行（分区 = 基准日）。
口径：D0 活跃用户在 D+1/D+3/D+7 仍活跃的比例，超出数据区间输出 NULL 而非 0。
用法：python etl/ads/ads_user_retention_daily.py --start 2019-10-01 --end 2019-10-31
"""

import argparse
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent))

from config.config import DBS, PARTITION_COL  # noqa: E402
from etl.utils import date_range, get_spark  # noqa: E402

TABLE = f"{DBS['ads']}.ads_user_retention_daily"
DWS_USER = f"{DBS['dws']}.dws_user_behavior_daily"
P = PARTITION_COL

RETENTION_OFFSETS = [1, 3, 7]


def _plus(dt, n):
    """日期 +n 天，返回 'YYYY-MM-DD'。"""
    from datetime import date as _d, timedelta
    return (_d.fromisoformat(dt) + timedelta(days=n)).isoformat()


def retention_sql(dt, end):
    """生成基准日 dt 的留存 SQL；end 为数据区间末日，用于判断右删失窗口。"""
    valid = [n for n in RETENTION_OFFSETS if _plus(dt, n) <= end]
    ctes = "".join(
        f",\n    d{n} AS (SELECT user_id FROM {DWS_USER} WHERE {P} = '{_plus(dt, n)}')"
        for n in valid
    )
    # 列顺序须与建表 DDL 一致，并显式给出列名列表：仅按位置 INSERT 会串位。
    uv_cols = ",\n".join(
        f"        (SELECT COUNT(*) FROM base b JOIN d{n} USING (user_id)) AS d{n}_uv"
        if n in valid else f"        CAST(NULL AS BIGINT) AS d{n}_uv"
        for n in RETENTION_OFFSETS
    )
    out_cols = ",\n".join(
        f"        d{n}_uv,\n"
        f"        CASE WHEN d0_uv > 0 AND d{n}_uv IS NOT NULL "
        f"THEN ROUND(d{n}_uv / d0_uv * 100, 4) END AS d{n}_rate"
        for n in RETENTION_OFFSETS
    )
    insert_cols = "d0_uv, " + ", ".join(
        f"d{n}_uv, d{n}_rate" for n in RETENTION_OFFSETS
    )
    return f"""
    INSERT OVERWRITE TABLE {TABLE} PARTITION ({P} = '{dt}')
    ({insert_cols})
    WITH base AS (SELECT user_id FROM {DWS_USER} WHERE {P} = '{dt}'){ctes},
    c AS (
        SELECT
        (SELECT COUNT(*) FROM base) AS d0_uv,
{uv_cols}
    )
    SELECT
        d0_uv,
{out_cols}
    FROM c
    """


def build(spark, dt, end):
    """计算单天基准日的留存；end 用于判断右删失窗口。"""
    spark.sql(retention_sql(dt, end))
    r = spark.sql(f"SELECT * FROM {TABLE} WHERE {P} = '{dt}'").collect()[0]
    return r


def build_range(spark, start, end):
    days = date_range(start, end)
    for d in days:
        build(spark, d, end)
    n_null = spark.sql(f"""
        SELECT COUNT(*) AS c FROM {TABLE}
        WHERE {P} BETWEEN '{start}' AND '{end}' AND d7_uv IS NULL
    """).collect()[0]["c"]
    mid = days[len(days) // 2]
    r = spark.sql(f"SELECT * FROM {TABLE} WHERE {P} = '{mid}'").collect()[0]
    print(f"  {TABLE}: {len(days)} 天（其中 {n_null} 天 D+7 超出数据区间 → NULL）")
    print(f"    {mid} 基准日：D0 {r['d0_uv']:,} 用户 → "
          f"D+1 {r['d1_rate']}% / D+3 {r['d3_rate']}% / D+7 {r['d7_rate']}%")


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--start", required=True)
    ap.add_argument("--end", required=True)
    args = ap.parse_args()

    spark = get_spark("ADS_UserRetentionDaily")
    spark.sparkContext.setLogLevel("ERROR")
    print(f"📈 {TABLE} {args.start} ~ {args.end}")
    build_range(spark, args.start, args.end)
    print("OK: 完成")
    spark.stop()


if __name__ == "__main__":
    main()
