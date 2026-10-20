#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
dwd_event_fact_dqc.py — DWD 层质量闸门（单条 UNION ALL SQL 判定）

校验账对平、分区非空、去重键唯一、核心字段空值、枚举、价格、时区共 7 项；
每项输出 (seq, code, item, status, detail)。只校验不修复，BLOCK 时退出码 1。

用法：python etl/dwd/dwd_event_fact_dqc.py --dt 2019-10-01
"""

import argparse
import os
import sys
from pathlib import Path

# 仓库根默认由本文件位置推导，可用 DW_PROJECT_ROOT 覆盖
_ROOT = Path(os.environ.get("DW_PROJECT_ROOT") or Path(__file__).resolve().parent.parent.parent)
sys.path.insert(0, str(_ROOT))

from config.config import (  # noqa: E402
    DBS, DEDUP_KEYS, ODS_PATH, PARTITION_COL as P, VALID_EVENT_TYPES,
)
from etl.utils import get_spark  # noqa: E402

FACT = f"{DBS['dwd']}.dwd_event_fact"
DIRTY = f"{DBS['dwd']}.dwd_event_dirty"


def dqc_sql(dt):
    """当日质量校验 SQL：每项一行 (seq, code, item, status, detail)。"""
    enums = ",".join(f"'{e}'" for e in VALID_EVENT_TYPES)
    keys = ", ".join(DEDUP_KEYS)
    return f"""
WITH m AS (
    -- 事实表只扫一遍；时间戳先 DATE_FORMAT 成字符串，避免 collect 时按本地时区渲染
    SELECT COUNT(*)                                                     AS fact_cnt,
           SUM(CASE WHEN {P} <> TO_DATE(event_time) THEN 1 ELSE 0 END)   AS tz_bad,
           SUM(CASE WHEN price < 0 THEN 1 ELSE 0 END)                   AS neg_price,
           SUM(CASE WHEN event_type NOT IN ({enums}) THEN 1 ELSE 0 END)  AS bad_enum,
           SUM(CASE WHEN event_time IS NULL OR user_id IS NULL
                     OR product_id IS NULL OR event_type IS NULL
                    THEN 1 ELSE 0 END)                                  AS null_key,
           DATE_FORMAT(MIN(event_time), 'yyyy-MM-dd HH:mm:ss')           AS min_t,
           DATE_FORMAT(MAX(event_time), 'yyyy-MM-dd HH:mm:ss')           AS max_t
    FROM {FACT}
    WHERE {P} = '{dt}'
),
dup AS (
    -- 去重键重复的分组数
    SELECT COUNT(*) AS dup_cnt FROM (
        SELECT {keys}
        FROM {FACT} WHERE {P} = '{dt}'
        GROUP BY {keys}
        HAVING COUNT(*) > 1
    ) t
),
base AS (
    -- SUM 用 COALESCE 兜底：空分区时 SUM 为 NULL，比较会落入 ELSE 误判
    SELECT (SELECT COUNT(*) FROM t_ods)                          AS ods_cnt,
           (SELECT COUNT(*) FROM {DIRTY} WHERE {P} = '{dt}')      AS dirty_cnt,
           COALESCE(m.fact_cnt,  0) AS fact_cnt,
           COALESCE(m.tz_bad,    0) AS tz_bad,
           COALESCE(m.neg_price, 0) AS neg_price,
           COALESCE(m.bad_enum,  0) AS bad_enum,
           COALESCE(m.null_key,  0) AS null_key,
           m.min_t,
           m.max_t,
           dup.dup_cnt
    FROM m CROSS JOIN dup
)
SELECT seq, code, item, status, detail FROM (
    SELECT 1 AS seq, 'count_overflow' AS code, '账对平' AS item,
           CASE WHEN b.ods_cnt >= b.fact_cnt + b.dirty_cnt THEN 'PASS' ELSE 'BLOCK' END AS status,
           CONCAT('ODS=', FORMAT_NUMBER(b.ods_cnt, 0),
                  ' = DWD=', FORMAT_NUMBER(b.fact_cnt, 0),
                  ' + 脏=', FORMAT_NUMBER(b.dirty_cnt, 0),
                  ' + 去重=', FORMAT_NUMBER(b.ods_cnt - b.fact_cnt - b.dirty_cnt, 0)) AS detail
    FROM base b
    UNION ALL
    SELECT 2, 'empty_partition', '分区非空',
           CASE WHEN b.fact_cnt > 0 THEN 'PASS' ELSE 'BLOCK' END,
           CONCAT('DWD 行数=', FORMAT_NUMBER(b.fact_cnt, 0))
    FROM base b
    UNION ALL
    SELECT 3, 'dup_key', '去重键唯一',
           CASE WHEN b.dup_cnt = 0 THEN 'PASS' ELSE 'BLOCK' END,
           CONCAT('重复组=', FORMAT_NUMBER(b.dup_cnt, 0))
    FROM base b
    UNION ALL
    SELECT 4, 'null_core_field', '核心字段空值',
           CASE WHEN b.null_key = 0 THEN 'PASS' ELSE 'BLOCK' END,
           CONCAT('空值行=', FORMAT_NUMBER(b.null_key, 0))
    FROM base b
    UNION ALL
    SELECT 5, 'bad_event_type', '枚举越界',
           CASE WHEN b.bad_enum = 0 THEN 'PASS' ELSE 'BLOCK' END,
           CONCAT('越界行=', FORMAT_NUMBER(b.bad_enum, 0))
    FROM base b
    UNION ALL
    SELECT 6, 'negative_price', '价格非负',
           CASE WHEN b.neg_price = 0 THEN 'PASS' ELSE 'BLOCK' END,
           CONCAT('负值行=', FORMAT_NUMBER(b.neg_price, 0))
    FROM base b
    UNION ALL
    SELECT 7, 'timezone_offset', '时区一致',
           CASE WHEN b.tz_bad = 0 THEN 'PASS' ELSE 'BLOCK' END,
           -- 空分区时 min_t/max_t 为 NULL，CONCAT 返回 NULL，用 COALESCE 兜成可读文案
           COALESCE(
               CONCAT('错位行=', FORMAT_NUMBER(b.tz_bad, 0),
                      ' | 事件时间 ', b.min_t, ' ~ ', b.max_t),
               '无数据（分区为空）') AS detail
    FROM base b
) c
ORDER BY seq
"""


def check_day(spark, dt):
    """跑一次 SQL，打印每项结果，返回被阻断项的 code 列表。"""
    # ODS 走路径读而非表名：MSCK 静默失败时表读会得到 0 行并误报，路径读无此风险
    spark.read.parquet(f"{ODS_PATH}/event_date={dt}").createOrReplaceTempView("t_ods")

    rows = spark.sql(dqc_sql(dt)).collect()

    print(f"🔍 [{dt}] DWD 质量校验")
    blocked = []
    for r in rows:
        ok = r["status"] == "PASS"
        print(f"  {'OK' if ok else 'FAIL'} {r['item']}：{r['detail']}")
        if not ok:
            blocked.append(r["code"])
    return blocked


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--dt", required=True, help="校验日期 YYYY-MM-DD")
    args = ap.parse_args()

    spark = get_spark("DWD_DQC")
    spark.sparkContext.setLogLevel("ERROR")
    blocked = check_day(spark, args.dt)
    spark.stop()

    if blocked:
        print(f"\nBLOCK [{args.dt}]：{', '.join(blocked)} → 阻断下游")
        sys.exit(1)
    print(f"OK: PASS [{args.dt}]")


if __name__ == "__main__":
    main()
