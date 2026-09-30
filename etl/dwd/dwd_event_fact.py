#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
dwd.dwd_event_fact — 清洗后的事实表（ODS → 干净明细）
======================================================
清洗规则（方案 §3.3，脏数据隔离而非静默丢弃，脏行由 etl/dwd/dwd_event_dirty.py 落隔离表）：
  1. 空值过滤   event_time / user_id / product_id 任一为空  → 不进事实表
  2. 枚举过滤   event_type 不在 view/cart/remove_from_cart/purchase → 不进事实表
  3. 异常值过滤 price < 0                                    → 不进事实表
  4. 事件去重   同 (user_id, event_time, product_id, event_type) 重复上报只留一条（沿用旧表口径）
  5. 时间标准化 拆出 event_date 分区列 + event_hour 派生列
  6. 口径衍生   behavior_type(中文) / is_purchase(0/1)，与旧表 dwd_ecom_behavior 完全一致

判定规则与脏数据表共用 etl/dwd/rules.py，保证"干净 + 脏 + 去重 = ODS"对得平。

幂等语义：**静态分区 INSERT OVERWRITE**，重跑只覆盖目标 event_date，其余分区不动。

用法：
  python etl/dwd/dwd_event_fact.py --dt 2019-10-01                 # 单天（Airflow 用）
  python etl/dwd/dwd_event_fact.py --start 2019-10-01 --end 2019-10-31   # 区间，单会话内循环
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
    """清洗单天并落事实表。返回行数；skip_existing 命中时返回 None。"""
    if skip_existing and hdfs_partition_count(
            spark, f"{DWD_FACT_PATH}/event_date={dt}", row=False) > 0:
        print(f"  [{dt}] DWD 分区已存在，跳过（幂等续跑）")
        return None

    src = spark.read.parquet(f"{ODS_PATH}/event_date={dt}")
    src.createOrReplaceTempView("t_src")
    ods_cnt = spark.sql("SELECT COUNT(*) AS c FROM t_src").collect()[0]["c"]
    if ods_cnt == 0:
        raise SystemExit(f"❌ {dt} ODS 分区为空，中止（先跑 ods 导入）")

    # 打标（判定规则与脏数据表共用）→ 只取干净行
    spark.sql(f"""
        SELECT *, {DIRTY_CLASSIFY_SQL} AS {DIRTY_REASON_COL} FROM t_src
    """).createOrReplaceTempView("t_tagged")

    # 去重：沿用旧表去重键，同键多上报保留一条。
    # 排序必须**完全确定**：去重组内 event_time 必然相同，只用它排序时保留哪条取决于
    # 扫描顺序 → 重跑结果会漂移（会话数/类目快照跟着变）。故追加其余列做兜底排序。
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

    # 派生列 + 落事实表（13 列口径：FACT_COLUMNS + event_date 分区列）
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
    print(f"🧹 {TABLE} 清洗 {len(days)} 天：{days[0]} ~ {days[-1]}")

    total = 0
    for d in days:
        n = build(spark, d, skip_existing=args.skip_existing)
        if n:
            total += n
    print(f"✅ {TABLE} 合计 {total:,} 行")
    spark.stop()


if __name__ == "__main__":
    main()
