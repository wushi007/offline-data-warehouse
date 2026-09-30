#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
dim.dim_session — 会话基础维度（会话→用户归属）
================================================
user_session 理论上是 per-user UUID，但源里存在极少量跨用户复用，
归属按 MAX(user_id) 兜底归一（一对多时无法还原真实归属，只保证维度主键唯一）。

两种运行方式：
  --start D --end D   全量重建：扫全部 DWD 分区
  --dt D              增量：只读当天分区 + 小维表快照，MIN/MAX 语义天然幂等

用法：
  python etl/dim/dim_session.py --dt 2019-11-01
  python etl/dim/dim_session.py --start 2019-10-01 --end 2019-10-31
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
SNAP = f"{STAGING}_snap"          # 增量用的"已有会话主键"快照路径
COLS = "user_session, user_id, session_start_time, session_end_time"   # 仅供全量重建路径参考

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

# 增量 = **只追加新会话**（反连接），不重写整表。
#
# 为什么不用"快照 + FULL OUTER JOIN + 整表重写"：dim_session 有 924 万行，
# 那条路径要读 924 万 + join + 再写 924 万，本机（7.6G，基础设施常驻 ~2.5G）
# 连续被内核 OOM killer 杀掉（dmesg 实锤 Killed process (java)）。
#
# 语义取舍（诚实说明）：会话按"首次出现即定稿"处理——同一 user_session 若跨天续期，
# 其 session_end_time 不会在后续天被延长。会话维表的用途是"会话→用户归属"，
# end_time 是次要信息；需要精确 end_time 时跑一次全量重建（--start/--end）即可校正。
#
# 幂等：反连接保证重跑同一天不再追加（新会话已在表里）。
INC_SQL = """
INSERT INTO TABLE dim.dim_session
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
    # 会话数从**建好的表**取（而不是再算一遍 FACT），保证汇报值与落表值一致
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
    """只追加新会话：快照**只读主键一列** → 反连接 → INSERT INTO 追加。

    924 万行的表读一列 vs 读整表再整表重写，内存/IO 差一个量级——这是这张表
    能在这台机器上跑通的关键。
    """
    from etl.utils import drop_path

    drop_path(spark, SNAP)
    spark.table(TABLE).select("user_session") \
        .write.mode("overwrite").option("compression", "snappy").parquet(SNAP)
    spark.read.parquet(SNAP).createOrReplaceTempView("t_session_snapshot")

    before = spark.sql(f"SELECT COUNT(*) AS c FROM {TABLE}").collect()[0]["c"]
    spark.sql(INC_SQL.format(fact=FACT, p=PARTITION_COL, dt=dt))
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
        print(f"🏗  {TABLE} 增量 {args.dt}")
        build_inc(spark, args.dt)
    else:
        print(f"🏗  {TABLE} 全量重建")
        rebuild(spark)
    print("✅ 完成")
    spark.stop()


if __name__ == "__main__":
    main()
