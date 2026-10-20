#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
dim.dim_product_scd2 — 商品维度 SCD2 拉链表

源无商品主数据，按天取各商品主导属性组合，属性变化即开新版本，连续同版本
日期折叠为区间；当前版本 dw_end_date = 9999-12-31；--dt 增量，--start/--end 全量重建。

用法：python etl/dim/dim_product_scd2.py --dt D | --start D --end D
"""

import argparse
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent))

from config.config import DBS, PARTITION_COL, WAREHOUSE  # noqa: E402
from etl.dim.staging import FACT, rewrite_via_staging  # noqa: E402
from etl.utils import drop_path, get_spark  # noqa: E402

TABLE = f"{DBS['dim']}.dim_product_scd2"
STAGING = f"{WAREHOUSE}/dim/_staging_scd2"
SCD2_COLS = ("dim_product_sk, product_id, category_id, category_code, brand, "
             "dw_start_date, dw_end_date, dw_is_current")

# 全量重建：窗口内重算拉链
SCD2_SQL = """
WITH daily AS (
    SELECT {p} AS event_date, product_id, category_id, category_code, brand, COUNT(*) AS cnt
    FROM {fact}
    WHERE {p} BETWEEN '{start}' AND '{end}'
    GROUP BY {p}, product_id, category_id, category_code, brand
),
dominant AS (                         -- 同票数按 category_id 兜底，保证确定性
    SELECT event_date, product_id, category_id, category_code, brand,
           ROW_NUMBER() OVER (PARTITION BY event_date, product_id
                              ORDER BY cnt DESC, category_id) AS rn
    FROM daily
),
d AS (SELECT * FROM dominant WHERE rn = 1),
marked AS (
    SELECT *,
           COALESCE(category_id, -1) || '|' || COALESCE(category_code, '') || '|' || COALESCE(brand, '') AS combo,
           LAG(COALESCE(category_id, -1) || '|' || COALESCE(category_code, '') || '|' || COALESCE(brand, ''))
               OVER (PARTITION BY product_id ORDER BY event_date) AS prev_combo
    FROM d
),
versioned AS (
    SELECT *,
           SUM(CASE WHEN prev_combo IS NULL OR combo <> prev_combo THEN 1 ELSE 0 END)
               OVER (PARTITION BY product_id ORDER BY event_date
                     ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS version_no
    FROM marked
),
spans AS (
    SELECT product_id, category_id, category_code, brand,
           MIN(event_date) AS dw_start_date,
           MAX(event_date) AS last_seen
    FROM versioned
    GROUP BY product_id, version_no, category_id, category_code, brand
),
ranked AS (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY product_id
                                 ORDER BY dw_start_date DESC) AS rn_desc
    FROM spans
)
SELECT
    ROW_NUMBER() OVER (ORDER BY product_id, dw_start_date) AS dim_product_sk,
    product_id, category_id, category_code, brand,
    dw_start_date,
    -- 非当前版本 end = 末次出现日 + 1（开区间），当前版本为 9999-12-31；
    -- "当前"按每商品最后一个版本判定，缺观测不等于属性变更。
    CASE WHEN rn_desc = 1 THEN DATE '9999-12-31'
         ELSE DATE_ADD(last_seen, 1) END  AS dw_end_date,
    CASE WHEN rn_desc = 1 THEN 1 ELSE 0 END AS dw_is_current
FROM ranked
"""

# 增量：当日 delta 与当前版本比对
SCD2_INCREMENTAL_SQL = """
WITH daily AS (
    SELECT product_id, category_id, category_code, brand, COUNT(*) AS cnt
    FROM {fact}
    WHERE {p} = '{dt}'
    GROUP BY product_id, category_id, category_code, brand
),
dominant AS (                         -- 同票数按 category_id 兜底
    SELECT product_id, category_id, category_code, brand,
           ROW_NUMBER() OVER (PARTITION BY product_id ORDER BY cnt DESC, category_id) AS rn
    FROM daily
),
today AS (
    SELECT product_id, category_id, category_code, brand,
           COALESCE(category_id, -1) || '|' || COALESCE(category_code, '') || '|' || COALESCE(brand, '') AS combo
    FROM dominant WHERE rn = 1
),
cur AS (                              -- 当前开放版本来自快照，避免读写同一张表
    SELECT dim_product_sk, product_id, category_id, category_code, brand, dw_start_date,
           COALESCE(category_id, -1) || '|' || COALESCE(category_code, '') || '|' || COALESCE(brand, '') AS combo
    FROM t_scd2_snapshot WHERE dw_is_current = 1
),
closed AS (                           -- 闭链：属性变化，旧版本 end = 变更日
    SELECT c.dim_product_sk, c.product_id, c.category_id, c.category_code, c.brand,
           c.dw_start_date, DATE '{dt}' AS dw_end_date, CAST(0 AS INT) AS dw_is_current
    FROM cur c JOIN today t ON c.product_id = t.product_id
    WHERE c.combo <> t.combo
),
opened AS (                           -- 开链：属性变化或全新商品
    SELECT t.product_id, t.category_id, t.category_code, t.brand
    FROM today t LEFT JOIN cur c ON t.product_id = c.product_id
    WHERE c.product_id IS NULL OR c.combo <> t.combo
),
new_sk AS (                           -- 代理键只追加，接现有 max+1
    SELECT (COALESCE((SELECT MAX(dim_product_sk) FROM t_scd2_snapshot), 0)
            + ROW_NUMBER() OVER (ORDER BY product_id)) AS dim_product_sk,
           product_id, category_id, category_code, brand
    FROM opened
),
kept AS (                             -- 保留未被闭链的版本
    SELECT s.* FROM t_scd2_snapshot s
    LEFT JOIN closed c ON s.dim_product_sk = c.dim_product_sk
    WHERE c.dim_product_sk IS NULL
)
SELECT dim_product_sk, product_id, category_id, category_code, brand,
       dw_start_date, dw_end_date, dw_is_current FROM kept
UNION ALL
SELECT dim_product_sk, product_id, category_id, category_code, brand,
       dw_start_date, dw_end_date, dw_is_current FROM closed
UNION ALL
SELECT dim_product_sk, product_id, category_id, category_code, brand,
       DATE '{dt}', DATE '9999-12-31', CAST(1 AS INT) FROM new_sk
"""


def _validate(spark, dt=None):
    """拉链不变量校验：每商品恰好一个当前版本；区间无重叠；同一商品同日不重复起始。"""
    bad_current = spark.sql(f"""
        SELECT COUNT(*) AS c FROM (
            SELECT product_id FROM {TABLE} GROUP BY product_id HAVING SUM(dw_is_current) <> 1
        ) t
    """).collect()[0]["c"]
    bad_range = spark.sql(f"""
        SELECT COUNT(*) AS c FROM (
            SELECT a.product_id FROM {TABLE} a
            JOIN {TABLE} b
              ON a.product_id = b.product_id AND a.dim_product_sk <> b.dim_product_sk
             AND a.dw_start_date < b.dw_end_date AND b.dw_start_date < a.dw_end_date
        ) t
    """).collect()[0]["c"]
    dup_start = spark.sql(f"""
        SELECT COUNT(*) AS c FROM (
            SELECT product_id, dw_start_date FROM {TABLE}
            GROUP BY product_id, dw_start_date HAVING COUNT(*) > 1
        ) t
    """).collect()[0]["c"]
    flag = "OK" if bad_current == 0 and bad_range == 0 and dup_start == 0 else "FAIL"
    print(f"  {flag} 拉链校验：当前版本数≠1 的商品 {bad_current} / 区间重叠 {bad_range}"
          f" / 同商品同日重复起始 {dup_start}（均须为 0）")


def rebuild(spark, start, end):
    """全量重建（窗口内重算）。"""
    sql = SCD2_SQL.format(fact=FACT, p=PARTITION_COL, start=start, end=end)
    spark.sql(f"INSERT OVERWRITE TABLE {TABLE} {sql}")

    st = spark.sql(f"""
        SELECT COUNT(*) AS version_cnt,
               COUNT(DISTINCT product_id) AS product_cnt,
               SUM(CASE WHEN dw_is_current = 1 THEN 1 ELSE 0 END) AS current_cnt,
               SUM(CASE WHEN dw_is_current = 0 THEN 1 ELSE 0 END) AS history_cnt
        FROM {TABLE}
    """).collect()[0]
    print(f"  dim_product_scd2: {st['version_cnt']:,} 版本 / {st['product_cnt']:,} 商品 "
          f"（当前版本 {st['current_cnt']:,}，历史版本 {st['history_cnt']:,}）；"
          f"平均每商品 {st['version_cnt'] / max(st['product_cnt'], 1):.3f} 个版本")
    _validate(spark)
    return st


def build_inc(spark, dt):
    """SCD2 单日增量。

    Spark 不允许写入的目标表同时被读取，故先落快照，再经临时路径写回。
    """
    snap_path = f"{STAGING}_snap"
    drop_path(spark, snap_path)
    spark.table(TABLE).write.mode("overwrite").option("compression", "snappy").parquet(snap_path)
    snap_cnt = spark.read.parquet(snap_path).count()
    spark.read.parquet(snap_path).createOrReplaceTempView("t_scd2_snapshot")

    new_state = spark.sql(SCD2_INCREMENTAL_SQL.format(fact=FACT, p=PARTITION_COL, dt=dt))
    rewrite_via_staging(spark, TABLE, new_state, SCD2_COLS, STAGING)
    drop_path(spark, snap_path)

    st = spark.sql(f"""
        SELECT COUNT(*) AS total,
               SUM(CASE WHEN dw_is_current = 1 THEN 1 ELSE 0 END) AS cur_cnt,
               SUM(CASE WHEN dw_start_date = DATE '{dt}' THEN 1 ELSE 0 END) AS new_ver,
               SUM(CASE WHEN dw_end_date = DATE '{dt}' THEN 1 ELSE 0 END) AS closed_ver
        FROM {TABLE}
    """).collect()[0]
    print(f"  [{dt}] SCD2 增量：快照 {snap_cnt:,} 行 → 当日开链版本 {st['new_ver']:,}"
          f"（含全新商品）/ 闭链 {st['closed_ver']:,} / 总版本 {st['total']:,}"
          f"（当前 {st['cur_cnt']:,}）")
    _validate(spark, dt)
    return st


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--dt", help="增量：只处理这一天")
    ap.add_argument("--start", help="全量重建：区间起")
    ap.add_argument("--end", help="全量重建：区间止")
    args = ap.parse_args()

    if not args.dt and not (args.start and args.end):
        raise SystemExit("用法: --dt D（增量） | --start D --end D（全量重建）")

    spark = get_spark("Dim_Product_SCD2")
    spark.sparkContext.setLogLevel("ERROR")
    if args.dt:
        print(f"🏗  {TABLE} 增量 {args.dt}")
        build_inc(spark, args.dt)
    else:
        print(f"🏗  {TABLE} 全量重建 {args.start} ~ {args.end}")
        drop_path(spark, STAGING)      # 重建前清旧文件（外部表 DROP 不删文件）
        rebuild(spark, args.start, args.end)
    print("OK: 完成")
    spark.stop()


if __name__ == "__main__":
    main()
