#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
dim.dim_session — 会话维度（会话到用户的归属）

user_session 存在少量跨用户复用，归属取 MAX(user_id) 以保证主键唯一。
--start/--end 全量重建；--dt 增量只追加新会话，MIN/MAX 语义幂等。

用法：python etl/dim/dim_session.py --dt D | --start D --end D
"""

import argparse
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent))

from config.config import DBS, PARTITION_COL, WAREHOUSE  # noqa: E402
from etl.dim.staging import FACT  # noqa: E402
from etl.utils import get_spark  # noqa: E402

TABLE = f"{DBS['dim']}.dim_session"
STAGING = f"{WAREHOUSE}/dim/_staging_session"
SNAP = f"{STAGING}_snap"          # 增量用的已有会话主键快照路径
COLS = "user_session, user_id, session_start_time, session_end_time"

REBUILD_SQL = f"""
    INSERT OVERWRITE TABLE {TABLE}
    SELECT
        user_session,
        MAX(user_id)      AS user_id,             -- 会话归属用户（跨用户复用时取大值兜底）
        MIN(event_time)   AS session_start_time,
        MAX(event_time)   AS session_end_time
    FROM {FACT}
    WHERE user_session IS NOT NULL
    GROUP BY user_session
"""

# 增量只追加新会话，不重写整表：dim_session 有 924 万行，整表读写会导致 OOM。
# 会话按首次出现定稿，跨天续期不延长 end_time；需精确值时跑全量重建，反连接保证重跑幂等。
INC_SQL = """
INSERT INTO TABLE {table}
WITH today AS (
    SELECT user_session, MAX(user_id) AS user_id,
           MIN(event_time) AS st, MAX(event_time) AS et
    FROM {fact} WHERE {p} = '{dt}' AND user_session IS NOT NULL
    GROUP BY user_session
)
SELECT t.user_session, t.user_id, t.st, t.et
FROM today t
LEFT ANTI JOIN t_session_snapshot s ON t.user_session = s.user_session
"""


def rebuild(spark):
    spark.sql(REBUILD_SQL)
    # 从建好的表取数，保证汇报值与落表值一致
    n = spark.sql(f"SELECT COUNT(*) AS c FROM {TABLE}").collect()[0]["c"]
    multi = spark.sql(f"""
        SELECT SUM(CASE WHEN u > 1 THEN 1 ELSE 0 END) AS multi_user
        FROM (SELECT user_session, COUNT(DISTINCT user_id) u
              FROM {FACT} GROUP BY user_session) t
    """).collect()[0]["multi_user"]
    print(f"  dim_session: {n:,} 会话；其中 {multi} 个会话ID被多用户复用"
          f"（{multi / max(n, 1) * 100:.4f}%，属源数据噪声，归属已按 MAX(user_id) 归一）")
    return n


def build_inc(spark, dt):
    """只追加新会话：快照只读主键列，反连接后 INSERT INTO。"""
    from etl.utils import drop_path

    drop_path(spark, SNAP)
    spark.table(TABLE).select("user_session") \
        .write.mode("overwrite").option("compression", "snappy").parquet(SNAP)
    spark.read.parquet(SNAP).createOrReplaceTempView("t_session_snapshot")

    before = spark.sql(f"SELECT COUNT(*) AS c FROM {TABLE}").collect()[0]["c"]
    spark.sql(INC_SQL.format(table=TABLE, fact=FACT, p=PARTITION_COL, dt=dt))
    drop_path(spark, SNAP)

    after = spark.sql(f"SELECT COUNT(*) AS c FROM {TABLE}").collect()[0]["c"]
    print(f"  [{dt}] dim_session 增量：{before:,} → {after:,} 行（追加新会话 {after - before:,}）")


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--dt", help="增量：只处理这一天")
    ap.add_argument("--start", help="全量重建：区间起")
    ap.add_argument("--end", help="全量重建：区间止")
    args = ap.parse_args()

    if not args.dt and not (args.start and args.end):
        raise SystemExit("用法: --dt D（增量） | --start D --end D（全量重建）")

    spark = get_spark("Dim_Session")
    spark.sparkContext.setLogLevel("ERROR")
    if args.dt:
        print(f"{TABLE} 增量 {args.dt}")
        build_inc(spark, args.dt)
    else:
        print(f"{TABLE} 全量重建")
        rebuild(spark)
    print("OK: 完成")
    spark.stop()


if __name__ == "__main__":
    main()
