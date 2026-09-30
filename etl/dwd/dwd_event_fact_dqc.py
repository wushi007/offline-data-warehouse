#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
dwd_event_fact_dqc.py — DWD 层质量闸门（纯 SQL 判定版）
========================================================
与 Python 判定版的区别：**所有判定逻辑集中在下一条 SQL 里**。
每个检查项通过 UNION ALL 输出一行 (seq, code, item, status, detail)，
Python 只负责「跑 SQL → 打印 → 依 status 决定退出码」。

收益：
  · 判定口径集中一处，一眼看全；加检查项 = 加一个 UNION ALL 分支
  · 这条 SQL 可单独拷到 beeline / Spark SQL 里跑，不依赖 Python
  · 输出自描述（item/code/detail 三列），日志可被 grep 或被告警消费

校验项（7 项，code 沿用具名短码，便于告警检索）：
  1 账对平        count_overflow   DWD + 脏 ≤ ODS（只可能少不可能多，多了说明写重）
  2 分区非空      empty_partition  DWD 行数 > 0
  3 去重键唯一    dup_key          去重键在分区内不重复
  4 核心字段空值  null_core_field  四个核心字段无空值
  5 枚举越界      bad_event_type   event_type 落在 4 类枚举内
  6 价格非负      negative_price   price 无负值
  7 时区一致      timezone_offset  event_date == 事件时间的 UTC 日期

★ 改这条 SQL 前请先读这三条设计要点 ★
--------------------------------------------------------------
① ODS 行数走**临时视图 t_ods**（按 HDFS 路径读），**不要**改成
   `FROM ods.ods_event_log`。表读依赖 MSCK REPAIR 成功，而 ODS 导入里的
   MSCK 是 try/except 只警告（etl/ods/ods_event_log.py:55），一旦静默失败
   会读到 0 行 → 误报「分区非空」为 BLOCK，且看起来像数据问题，很难查。
   路径读没有这个风险，且与 dwd_event_fact.py / dwd_event_dirty.py 保持一致。

② **所有 SUM 都包 COALESCE(...,0)**。空分区时 SUM 返回 NULL，而
   `NULL = 0` 的结果是 NULL（既不真也不假）→ CASE WHEN 会落到 ELSE
   把空分区误判成 BLOCK。原 Python 版在**打印**上就踩了这个坑：
   `None == 0` 为假，于是空分区时会打出 4 个误导性的 ❌
   （核心字段空值 / 枚举越界 / 价格负值 / 时区一致 全显示 ❌ 和 None），
   虽然它的 blocked 列表没被污染（`if None:` 是假值），但输出会让人
   误以为这 4 项也失败了。本版用 COALESCE 后只标真正失败的那一项。

③ **时间戳用 DATE_FORMAT 转字符串再取**。直接 collect 时间戳会经 py4j
   按 **Python 进程本地时区**渲染（本机 +8），打印成 08:00 ~ 次日 07:59，
   看着像分区混进了次日数据 —— 纯属误读来源，数据本身没错。

职责边界：只校验，不修复。脏数据已在 DWD 清洗时隔离到 dwd_event_dirty，可追溯。

用法：
  python etl/dwd/dwd_event_fact_dqc.py --dt 2019-10-01
  ./run.sh dwd-dqc --dt 2019-10-01          # 经 run.sh（含串行闸门）

退出码：
  0 = 全部 PASS
  1 = 存在 BLOCK —— Airflow 据此阻断下游，并把该任务归类为
      「数据质量不达标，已阻断下游」（而非「脚本崩了」），见 DAG 的 QUALITY_TASKS
"""

import argparse
import os
import sys
from pathlib import Path

# 项目根目录：默认按「本文件在 <repo>/etl/dwd/ 下」推导（与仓内其他文件一致）。
# 若想把本文件放在别处试跑，用 DW_PROJECT_ROOT 指定仓库根：
#   DW_PROJECT_ROOT=~/ecom-warehouse python /path/to/dwd_event_fact_dqc_pure_sql.py --dt 2019-10-01
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
    -- 事实表只扫一遍，算出全部行级指标
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
    -- 汇总口径。SUM 一律 COALESCE 兜底空分区（见文件头 ② ）
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
           -- 空分区时 min_t/max_t 为 NULL，CONCAT 整体返回 NULL → 用 COALESCE 兜成可读文案，
           -- 否则 detail 打印成 "None"（判定是对的，但看起来像出了问题）
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
    # ODS 按 HDFS 路径读（不依赖 metastore 分区注册），包成临时视图供 SQL 使用 —— 见文件头 ①
    spark.read.parquet(f"{ODS_PATH}/event_date={dt}").createOrReplaceTempView("t_ods")

    rows = spark.sql(dqc_sql(dt)).collect()

    print(f"🔍 [{dt}] DWD 质量校验")
    blocked = []
    for r in rows:
        ok = r["status"] == "PASS"
        print(f"  {'✅' if ok else '❌'} {r['item']}：{r['detail']}")
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
        print(f"\n❌ BLOCK [{args.dt}]：{', '.join(blocked)} → 阻断下游")
        sys.exit(1)
    print(f"✅ PASS [{args.dt}]")


if __name__ == "__main__":
    main()
