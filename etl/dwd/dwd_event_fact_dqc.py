#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
dwd_dqc.py — DWD 层质量闸门（校验不过以非 0 退出码阻断下游）
============================================================
校验项（方案 §九）：
  1. 完整性    分区非空；DWD 行数 + 脏数据行数 + 去重行数 == ODS 行数（**账必须对平**）
  2. 主键唯一性 去重键 (user_id, event_time, product_id, event_type) 在分区内唯一
  3. 空值率     核心字段 event_time/user_id/product_id/event_type 空值率为 0
  4. 枚举校验   event_type 只能落在 4 类枚举内
  5. 价格非负   price < 0 的行数为 0（异常值已在清洗拦截）
  6. 时间一致性 event_date 分区值必须等于事件时间的 UTC 日期（时区偏移会在这里现形）

职责边界：只校验，不修复。脏数据已在 DWD 清洗时隔离到 dwd_event_dirty，可追溯。

用法：
  python etl/dwd/dwd_event_fact_dqc.py --dt 2019-10-01
"""

import argparse
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent))

from config.config import (  # noqa: E402
    DBS, DEDUP_KEYS, ODS_PATH, PARTITION_COL as P, VALID_EVENT_TYPES,
)
from etl.utils import get_spark  # noqa: E402

FACT = f"{DBS['dwd']}.dwd_event_fact"
DIRTY = f"{DBS['dwd']}.dwd_event_dirty"


def check_day(spark, dt):
    blocked = []
    print(f"🔍 [{dt}] DWD 质量校验")

    # 1. 完整性 / 账对平
    ods_cnt = spark.read.parquet(f"{ODS_PATH}/event_date={dt}").count()
    d = spark.sql(f"""
        SELECT COUNT(*) AS fact_cnt,
               SUM(CASE WHEN {P} <> TO_DATE(event_time) THEN 1 ELSE 0 END) AS tz_bad,
               SUM(CASE WHEN price < 0 THEN 1 ELSE 0 END) AS neg_price,
               SUM(CASE WHEN event_type NOT IN ({",".join(f"'{e}'" for e in VALID_EVENT_TYPES)})
                        THEN 1 ELSE 0 END) AS bad_enum,
               SUM(CASE WHEN event_time IS NULL OR user_id IS NULL OR product_id IS NULL
                             OR event_type IS NULL THEN 1 ELSE 0 END) AS null_key,
               -- 用 DATE_FORMAT 转成字符串再取：session tz=UTC 下渲染即真实 UTC 时间。
               -- 直接 collect 时间戳会经 py4j 按**Python 进程本地时区**渲染（本机 +8），
               -- 打印出来是 08:00~次日07:59，看着像分区混了次日数据，纯属误读来源。
               DATE_FORMAT(MIN(event_time), 'yyyy-MM-dd HH:mm:ss') AS min_t,
               DATE_FORMAT(MAX(event_time), 'yyyy-MM-dd HH:mm:ss') AS max_t
        FROM {FACT} WHERE {P} = '{dt}'
    """).collect()[0]
    dirty_cnt = spark.sql(f"SELECT COUNT(*) AS c FROM {DIRTY} WHERE {P} = '{dt}'").collect()[0]["c"]
    fact_cnt = d["fact_cnt"] or 0
    print(f"  行数：ODS {ods_cnt:,} = DWD {fact_cnt:,} + 脏 {dirty_cnt:,} + 去重 "
          f"{ods_cnt - fact_cnt - dirty_cnt:,}")
    if fact_cnt == 0:
        blocked.append("empty_partition")
    if fact_cnt + dirty_cnt > ods_cnt:
        blocked.append("count_overflow")   # 只可能少不可能多，多了说明写重了

    # 2. 主键唯一性
    dup = spark.sql(f"""
        SELECT COUNT(*) AS c FROM (
            SELECT {", ".join(DEDUP_KEYS)}, COUNT(*) n FROM {FACT}
            WHERE {P} = '{dt}' GROUP BY {", ".join(DEDUP_KEYS)} HAVING n > 1
        ) t
    """).collect()[0]["c"]
    print(f"  {'✅' if dup == 0 else '❌'} 去重键唯一性：{dup} 组重复")
    if dup: blocked.append("dup_key")

    # 3. 空值率
    print(f"  {'✅' if d['null_key'] == 0 else '❌'} 核心字段空值：{d['null_key']}")
    if d["null_key"]: blocked.append("null_core_field")

    # 4. 枚举
    print(f"  {'✅' if d['bad_enum'] == 0 else '❌'} event_type 枚举越界：{d['bad_enum']}")
    if d["bad_enum"]: blocked.append("bad_event_type")

    # 5. 价格非负
    print(f"  {'✅' if d['neg_price'] == 0 else '❌'} price 负值：{d['neg_price']}")
    if d["neg_price"]: blocked.append("negative_price")

    # 6. 时区一致性（分区日必须等于事件 UTC 日期，否则数据被挪日）
    print(f"  {'✅' if d['tz_bad'] == 0 else '❌'} 分区/UTC 日期一致性：{d['tz_bad']} 行错位"
          f"（事件时间 {d['min_t']} ~ {d['max_t']}）")
    if d["tz_bad"]: blocked.append("timezone_offset")

    return blocked


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--dt", required=True)
    args = ap.parse_args()

    spark = get_spark("DWD_DQC")
    spark.sparkContext.setLogLevel("ERROR")
    blocked = check_day(spark, args.dt)
    spark.stop()

    if blocked:
        print(f"\n❌ BLOCK [{args.dt}]：{', '.join(blocked)} → 阻断下游")
        sys.exit(1)
    print(f"✅ PASS [{args.dt}]")


if __name__ == "__main__":
    main()
