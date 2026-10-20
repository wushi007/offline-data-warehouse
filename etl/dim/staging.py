# -*- coding: utf-8 -*-
"""
DIM 层增量写回助手，供各维度表脚本共用。
"""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent))

from config.config import DBS, PARTITION_COL  # noqa: E402
from etl.utils import drop_path  # noqa: E402

FACT = f"{DBS['dwd']}.dwd_event_fact"


def rewrite_via_staging(spark, table, new_state, cols, staging):
    """经临时路径 INSERT OVERWRITE 回表，绕开 Spark 目标表不可自读的限制。"""
    drop_path(spark, staging)
    new_state.write.mode("overwrite").option("compression", "snappy").parquet(staging)
    spark.read.parquet(staging).createOrReplaceTempView("t_staged")
    spark.sql(f"INSERT OVERWRITE TABLE {table} ({cols}) SELECT {cols} FROM t_staged")
    drop_path(spark, staging)


def inc_dim(spark, dt, table, sql, cols, staging, label):
    """通用增量：快照落 parquet → 算新状态 → 经临时路径写回。

    快照不 cache 进堆内存：大表 cache 会导致 driver OOM。
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
