#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
dim.dim_category — 品类基础维度（自 category_code 拆层级）
===========================================================
两种运行方式：
  --start D --end D   全量重建：扫全部 DWD 分区
  --dt D              增量：只读当天分区 + 小维表快照；品类编码变了以当天为准（覆盖语义）

用法：
  python etl/dim/dim_category.py --dt 2019-11-01
  python etl/dim/dim_category.py --start 2019-10-01 --end 2019-10-31
"""

import argparse
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent))

from config.config import DBS, WAREHOUSE  # noqa: E402
from etl.dim.staging import FACT, inc_dim  # noqa: E402
from etl.utils import get_spark  # noqa: E402

TABLE = f"{DBS['dim']}.dim_category"
STAGING = f"{WAREHOUSE}/dim/_staging_category"
COLS = "category_id, category_code, category_l1, category_l2, category_l3"

REBUILD_SQL = f"""
    INSERT OVERWRITE TABLE {TABLE}
    SELECT
        category_id,
        MAX(category_code)                        AS category_code,
        MAX(SPLIT(category_code, '\\\\.')[0])       AS category_l1,
        MAX(SPLIT(category_code, '\\\\.')[1])       AS category_l2,
        MAX(SPLIT(category_code, '\\\\.')[2])       AS category_l3
    FROM {FACT}
    WHERE category_id IS NOT NULL
    GROUP BY category_id
"""

INC_SQL = """
WITH today AS (
    SELECT category_id, MAX(category_code) AS category_code
    FROM {fact} WHERE {p} = '{dt}' AND category_id IS NOT NULL
    GROUP BY category_id
),
t2 AS (
    SELECT category_id, category_code,
           SPLIT(category_code, '\\\\.')[0] AS l1,
           SPLIT(category_code, '\\\\.')[1] AS l2,
           SPLIT(category_code, '\\\\.')[2] AS l3
    FROM today
)
SELECT
    COALESCE(o.category_id, t2.category_id)                              AS category_id,
    -- 品类编码变了就以当天为准（覆盖语义，重跑同一天结果一致）
    COALESCE(t2.category_code, o.category_code)                          AS category_code,
    COALESCE(t2.l1, o.category_l1)                                       AS category_l1,
    COALESCE(t2.l2, o.category_l2)                                       AS category_l2,
    COALESCE(t2.l3, o.category_l3)                                       AS category_l3
FROM t_snapshot o FULL OUTER JOIN t2 ON o.category_id = t2.category_id
"""


def rebuild(spark):
    spark.sql(REBUILD_SQL)
    n = spark.sql(f"SELECT COUNT(*) AS c FROM {TABLE}").collect()[0]["c"]
    print(f"  dim_category: {n:,} 品类")
    return n


def build_inc(spark, dt):
    inc_dim(spark, dt, TABLE, INC_SQL, COLS, STAGING, "dim_category")


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--dt", help="增量：只处理这一天")
    ap.add_argument("--start", help="全量重建：区间起")
    ap.add_argument("--end", help="全量重建：区间止")
    args = ap.parse_args()

    if not args.dt and not (args.start and args.end):
        raise SystemExit("用法: --dt D（增量） | --start D --end D（全量重建）")

    spark = get_spark("Dim_Category")
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
