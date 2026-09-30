# -*- coding: utf-8 -*-
"""
DIM 层增量写回助手（本层多张表共用）
====================================
原 build_dims.py 里的 _rewrite_via_staging / _inc_dim，原文搬来供各维度表脚本共用。
"""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent))

from config.config import DBS, PARTITION_COL  # noqa: E402
from etl.utils import drop_path  # noqa: E402

FACT = f"{DBS['dwd']}.dwd_event_fact"


def rewrite_via_staging(spark, table, new_state, cols, staging):
    """把 new_state 落到临时路径，再从临时路径 INSERT OVERWRITE 回表。

    绕开 Spark 的守卫：目标表不能出现在自己的写查询里（见 dim_product_scd2 增量注释）。
    维表都是小表（17 万行级），多一次物化的代价可以忽略。
    """
    drop_path(spark, staging)
    new_state.write.mode("overwrite").option("compression", "snappy").parquet(staging)
    spark.read.parquet(staging).createOrReplaceTempView("t_staged")
    spark.sql(f"INSERT OVERWRITE TABLE {table} ({cols}) SELECT {cols} FROM t_staged")
    drop_path(spark, staging)


def inc_dim(spark, dt, table, sql, cols, staging, label):
    """通用增量：快照落 parquet → 算新状态 → 经临时路径写回。

    快照**不 cache 进堆内存**：dim_session 有 924 万行，cache 之后 driver 直接被
    OOM killer 干掉（踩过：`dmesg` 里 `Killed process (java)`）。落 parquet 走磁盘，
    顺带也解决了"目标表不能出现在自己的写查询里"的问题——读的是文件路径，不是表。
    """
    snap_path = f"{staging}_snap"
    drop_path(spark, snap_path)
    spark.table(table).write.mode("overwrite").option("compression", "snappy").parquet(snap_path)
    before = spark.read.parquet(snap_path).count()
    spark.read.parquet(snap_path).createOrReplaceTempView("t_snapshot")

    new_state = spark.sql(sql.format(fact=FACT, p=PARTITION_COL, dt=dt))
    rewrite_via_staging(spark, table, new_state, cols, staging)
    drop_path(spark, snap_path)

    after = spark.sql(f"SELECT COUNT(*) AS c FROM {table}").collect()[0]["c"]
    print(f"  [{dt}] {label} 增量：{before:,} → {after:,} 行（+{after - before:,}）")
