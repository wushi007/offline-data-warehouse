#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
dwd.dwd_event_fact — ODS 清洗后的事实表

过滤空值键、非法枚举与负价格，按去重键保留一条，派生 behavior_type /
is_purchase / event_hour；脏行由 dwd_event_dirty.py 隔离，判定规则见 rules.py。
静态分区 INSERT OVERWRITE，重跑只覆盖目标 event_date。
用法：python etl/dwd/dwd_event_fact.py --dt D | --start D --end D
"""

import argparse
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent))

from config.config import (  # noqa: E402
    DBS, DEDUP_KEYS, DIRTY_REASON_COL, DWD_FACT_PATH, FACT_COLUMNS, ODS_PATH,
)
from etl.dwd.rules import BEHAVIOR_CASE_SQL, DIRTY_CLASSIFY_SQL  # noqa: E402
from etl.utils import date_range, get_spark, hdfs_partition_count  # noqa: E402

TABLE = f"{DBS['dwd']}.dwd_event_fact"


def build(spark, dt, skip_existing=False):
    """清洗单天并落事实表，返回行数；skip_existing 命中时返回 None。"""
    if skip_existing and hdfs_partition_count(
            spark, f"{DWD_FACT_PATH}/event_date={dt}", row=False) > 0:
        print(f"  [{dt}] DWD 分区已存在，跳过（幂等续跑）")
        return None

    src = spark.read.parquet(f"{ODS_PATH}/event_date={dt}")
    src.createOrReplaceTempView("t_src")
    ods_cnt = spark.sql("SELECT COUNT(*) AS c FROM t_src").collect()[0]["c"]
    if ods_cnt == 0:
        raise SystemExit(f"FAIL: {dt} ODS 分区为空，中止（先跑 ods 导入）")

    # 打标：与脏数据表共用判定规则
    spark.sql(f"""
        SELECT *, {DIRTY_CLASSIFY_SQL} AS {DIRTY_REASON_COL} FROM t_src
    """).createOrReplaceTempView("t_tagged")

    # 去重键相同则只留一条；组内 event_time 相同，需追加其余列做确定性排序，否则重跑结果会漂移。
    dedup_keys = ", ".join(DEDUP_KEYS)
    tiebreak = ", ".join(["event_time"] + [c for c in FACT_COLUMNS
                                           if c not in DEDUP_KEYS + ["behavior_type",
                                                                     "is_purchase", "event_hour"]])
    spark.sql(f"""
        SELECT * FROM (
            SELECT *, ROW_NUMBER() OVER (PARTITION BY {dedup_keys}
                                         ORDER BY {tiebreak}) AS rn
            FROM t_tagged WHERE dirty_reason IS NULL
        ) t WHERE rn = 1
    """).createOrReplaceTempView("t_dedup")

    spark.sql(f"""
        INSERT OVERWRITE TABLE {TABLE} PARTITION (event_date = '{dt}')
        SELECT
            event_time,
            event_type,
            {BEHAVIOR_CASE_SQL}                                        AS behavior_type,
            product_id,
            category_id,
            category_code,
            brand,
            price,
            user_id,
            user_session,
            CAST(CASE WHEN event_type = 'purchase' THEN 1 ELSE 0 END AS INT) AS is_purchase,
            CAST(HOUR(event_time) AS INT)                              AS event_hour
        FROM t_dedup
    """)
    fact_cnt = spark.sql("SELECT COUNT(*) AS c FROM t_dedup").collect()[0]["c"]
    print(f"  [{dt}] ODS {ods_cnt:,} → dwd_event_fact {fact_cnt:,}（去重后）")
    return fact_cnt


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--dt")
    ap.add_argument("--start")
    ap.add_argument("--end")
    ap.add_argument("--skip-existing", action="store_true", help="分区已存在则跳过（断点续跑）")
    args = ap.parse_args()

    if args.dt:
        days = [args.dt]
    elif args.start and args.end:
        days = date_range(args.start, args.end)
    else:
        raise SystemExit("用法: --dt D | --start D --end D")

    spark = get_spark("DWD_EventFact")
    spark.sparkContext.setLogLevel("ERROR")
    print(f"{TABLE} 清洗 {len(days)} 天：{days[0]} ~ {days[-1]}")

    total = 0
    for d in days:
        n = build(spark, d, skip_existing=args.skip_existing)
        if n:
            total += n
    print(f"OK: {TABLE} 合计 {total:,} 行")
    spark.stop()


if __name__ == "__main__":
    main()
