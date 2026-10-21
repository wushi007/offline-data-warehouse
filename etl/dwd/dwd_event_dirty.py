#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
dwd.dwd_event_dirty — 清洗被拦下的脏数据隔离表

脏行留痕而不丢弃，保证 ODS = dwd_event_fact（去重后）+ dwd_event_dirty + 去重行数。
dirty_reason 定义见 etl/dwd/rules.py。

用法：python etl/dwd/dwd_event_dirty.py --dt D | --start D --end D
"""

import argparse
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent))

from config.config import DBS, DIRTY_REASON_COL, ODS_PATH  # noqa: E402
from etl.dwd.rules import DIRTY_CLASSIFY_SQL  # noqa: E402
from etl.utils import date_range, get_spark  # noqa: E402

TABLE = f"{DBS['dwd']}.dwd_event_dirty"

DIRTY_COLS = ("event_time, event_type, product_id, category_id, category_code, "
              "brand, price, user_id, user_session, dirty_reason")


def build(spark, dt):
    """把当天被拦下的脏数据落到隔离表。返回行数。"""
    src = spark.read.parquet(f"{ODS_PATH}/event_date={dt}")
    src.createOrReplaceTempView("t_src")

    spark.sql(f"""
        SELECT *, {DIRTY_CLASSIFY_SQL} AS {DIRTY_REASON_COL} FROM t_src
    """).createOrReplaceTempView("t_tagged")

    spark.sql(f"""
        INSERT OVERWRITE TABLE {TABLE} PARTITION (event_date = '{dt}')
        SELECT {DIRTY_COLS} FROM t_tagged WHERE dirty_reason IS NOT NULL
    """)
    cnt = spark.sql(
        "SELECT COUNT(*) AS c FROM t_tagged WHERE dirty_reason IS NOT NULL"
    ).collect()[0]["c"]

    if cnt:
        detail = spark.sql(f"""
            SELECT dirty_reason, COUNT(*) AS c FROM t_tagged
            WHERE dirty_reason IS NOT NULL GROUP BY dirty_reason ORDER BY c DESC
        """).collect()
        detail_s = "，".join(f"{r['dirty_reason']}={r['c']:,}" for r in detail)
    else:
        detail_s = "无"
    print(f"  [{dt}] {TABLE} {cnt:,} 行（{detail_s}）")
    return cnt


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--dt")
    ap.add_argument("--start")
    ap.add_argument("--end")
    args = ap.parse_args()

    if args.dt:
        days = [args.dt]
    elif args.start and args.end:
        days = date_range(args.start, args.end)
    else:
        raise SystemExit("用法: --dt D | --start D --end D")

    spark = get_spark("DWD_EventDirty")
    spark.sparkContext.setLogLevel("ERROR")
    print(f"{TABLE} 隔离 {len(days)} 天：{days[0]} ~ {days[-1]}")

    total = 0
    for d in days:
        total += build(spark, d)
    print(f"OK: {TABLE} 合计 {total:,} 行")
    spark.stop()


if __name__ == "__main__":
    main()
