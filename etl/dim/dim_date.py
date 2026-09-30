#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
dim.dim_date — 日期维度（覆盖 2019 全年）
=========================================
静态表：一次性生成，不参与日调度（增量任务里不跑）。
is_workday 按周一~周五（无节假日日历，口径已在列注释说明）。

用法：
  python etl/dim/dim_date.py
"""

import argparse
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent))

from config.config import DBS  # noqa: E402
from etl.utils import get_spark  # noqa: E402

TABLE = f"{DBS['dim']}.dim_date"


def rebuild(spark):
    spark.sql(f"""
        INSERT OVERWRITE TABLE {TABLE}
        SELECT
            d AS date_id,
            CAST(YEAR(d) AS INT)                AS `year`,
            CAST(MONTH(d) AS INT)               AS `month`,
            CAST(DAY(d) AS INT)                 AS `day`,
            CAST(WEEKOFYEAR(d) AS INT)          AS week_of_year,
            CAST(DAYOFWEEK(d) AS INT)           AS day_of_week,
            CAST(CASE WHEN DAYOFWEEK(d) IN (1, 7) THEN 0 ELSE 1 END AS INT) AS is_workday
        FROM (SELECT EXPLODE(SEQUENCE(DATE '2019-01-01', DATE '2019-12-31', INTERVAL 1 DAY)) AS d)
    """)
    n = spark.sql(f"SELECT COUNT(*) AS c FROM {TABLE}").collect()[0]["c"]
    print(f"  dim_date: {n} 天（2019-01-01 ~ 2019-12-31）")
    return n


def main():
    argparse.ArgumentParser().parse_args()
    spark = get_spark("Dim_Date")
    spark.sparkContext.setLogLevel("ERROR")
    print(f"🏗  {TABLE} 生成")
    rebuild(spark)
    print("✅ 完成")
    spark.stop()


if __name__ == "__main__":
    main()
