#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
dim.dim_user — 用户基础维度（含时间属性）
==========================================
时间属性：first_seen_date / last_seen_date / active_days / first_purchase_date / purchase_days，
支撑新老客识别与用户生命周期分析。

两种运行方式：
  --start D --end D   全量重建：扫**全部 DWD 分区**（不限窗口）——
                      "首次活跃日"跨全量历史才有意义，不该随 SCD2 的窗口变化
  --dt D              增量：只读当天分区 + 小维表快照，不再全扫 DWD

增量幂等：MIN/MAX 语义天然幂等；累加字段（active_days / purchase_days）必须加守卫——
只有 dt 比已记录的 last_seen 更晚才累加，否则重跑同一天会重复 +1。

用法：
  python etl/dim/dim_user.py --dt 2019-11-01
  python etl/dim/dim_user.py --start 2019-10-01 --end 2019-10-31
"""

import argparse
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent))

from config.config import DATA_START as DATE_START, DBS, WAREHOUSE  # noqa: E402
from etl.dim.staging import FACT, inc_dim  # noqa: E402
from etl.utils import get_spark  # noqa: E402

TABLE = f"{DBS['dim']}.dim_user"
STAGING = f"{WAREHOUSE}/dim/_staging_user"
COLS = ("user_id, first_seen_date, last_seen_date, active_days, "
        "first_purchase_date, purchase_days")

REBUILD_SQL = f"""
    INSERT OVERWRITE TABLE {TABLE}
    SELECT
        user_id,
        MIN(event_date)                                        AS first_seen_date,
        MAX(event_date)                                        AS last_seen_date,
        COUNT(DISTINCT event_date)                             AS active_days,
        MIN(CASE WHEN is_purchase = 1 THEN event_date END)      AS first_purchase_date,
        COUNT(DISTINCT CASE WHEN is_purchase = 1 THEN event_date END) AS purchase_days
    FROM {FACT}
    WHERE user_id IS NOT NULL
    GROUP BY user_id
"""

INC_SQL = """
WITH today AS (
    SELECT user_id,
           MIN({p}) AS min_d, MAX({p}) AS max_d,
           COUNT(DISTINCT {p}) AS days_delta,
           MIN(CASE WHEN is_purchase = 1 THEN {p} END) AS fp,
           COUNT(DISTINCT CASE WHEN is_purchase = 1 THEN {p} END) AS pd_delta
    FROM {fact} WHERE {p} = '{dt}' AND user_id IS NOT NULL
    GROUP BY user_id
)
SELECT
    COALESCE(o.user_id, t.user_id)                                        AS user_id,
    -- MIN/MAX 语义天然幂等：重跑同一天结果不变
    LEAST(COALESCE(o.first_seen_date, t.min_d), COALESCE(t.min_d, o.first_seen_date))
                                                                         AS first_seen_date,
    GREATEST(COALESCE(o.last_seen_date, t.max_d), COALESCE(t.max_d, o.last_seen_date))
                                                                         AS last_seen_date,
    -- 累加字段必须加幂等守卫：只有 dt 比已记录的 last_seen 更晚才累加，
    -- 否则重跑同一天会把活跃天数重复 +1（增量最容易踩的坑）
    CASE WHEN o.last_seen_date IS NULL OR o.last_seen_date < DATE '{dt}'
         THEN COALESCE(o.active_days, 0) + t.days_delta
         ELSE o.active_days END                                          AS active_days,
    LEAST(COALESCE(o.first_purchase_date, t.fp), COALESCE(t.fp, o.first_purchase_date))
                                                                         AS first_purchase_date,
    CASE WHEN o.last_seen_date IS NULL OR o.last_seen_date < DATE '{dt}'
         THEN COALESCE(o.purchase_days, 0) + t.pd_delta
         ELSE o.purchase_days END                                        AS purchase_days
FROM t_snapshot o FULL OUTER JOIN today t ON o.user_id = t.user_id
"""


def rebuild(spark):
    spark.sql(REBUILD_SQL)
    st = spark.sql(f"""
        SELECT COUNT(*) AS users,
               SUM(CASE WHEN first_purchase_date IS NOT NULL THEN 1 ELSE 0 END) AS buyers,
               SUM(CASE WHEN first_purchase_date = '{DATE_START}' THEN 1 ELSE 0 END) AS cum_new
        FROM {TABLE}
    """).collect()[0]
    print(f"  dim_user: {st['users']:,} 用户（购买用户 {st['buyers']:,}）；"
          f"首购日落在数据首日 {DATE_START} 的 {st['cum_new']:,} 人属【左删失】"
          f"（更早的历史不在本次数据里，不等于当日新增）")
    return st


def build_inc(spark, dt):
    inc_dim(spark, dt, TABLE, INC_SQL, COLS, STAGING, "dim_user")


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--dt", help="增量：只处理这一天")
    ap.add_argument("--start", help="全量重建：区间起（实际扫全部 DWD 分区）")
    ap.add_argument("--end", help="全量重建：区间止")
    args = ap.parse_args()

    if not args.dt and not (args.start and args.end):
        raise SystemExit("用法: --dt D（增量） | --start D --end D（全量重建）")

    spark = get_spark("Dim_User")
    spark.sparkContext.setLogLevel("ERROR")
    if args.dt:
        print(f"🏗  {TABLE} 增量 {args.dt}")
        build_inc(spark, args.dt)
    else:
        print(f"🏗  {TABLE} 全量重建")
        rebuild(spark)
    print("✅ 完成")
    spark.stop()


if __name__ == "__main__":
    main()
