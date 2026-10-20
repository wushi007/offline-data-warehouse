#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
ods_event_log.py — ODS 按天接入

从源 CSV 按日期切分，零修改落盘到 ODS（Parquet + event_date 分区），
业务清洗在 DWD 层完成；分区已存在且非空时跳过。批量导入见 ods_event_log_bulk.py。

用法：python etl/ods/ods_event_log.py --dt 2019-11-01
"""

import argparse
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent))

from config.config import DBS, SRC_CSV_2019NOV, SRC_SCHEMA, ODS_PATH  # noqa: E402
from etl.utils import get_spark, hdfs_partition_count  # noqa: E402

# MSCK REPAIR 目标表，必须为库名.表名；DataFrame 直写不会登记 metastore 分区。
TABLE = f"{DBS['ods']}.ods_event_log"


def import_day(spark, dt: str):
    from pyspark.sql import functions as F

    target = f"{ODS_PATH}/event_date={dt}"
    if hdfs_partition_count(spark, target, row=False) > 0:
        print(f"  {dt} 分区已存在，跳过导入")
        return 0

    print(f"  读取源 CSV：{SRC_CSV_2019NOV.split('/')[-1]}")
    raw = spark.read.csv(SRC_CSV_2019NOV, schema=SRC_SCHEMA,
                         header=True, sep=",", timestampFormat="yyyy-MM-dd HH:mm:ss z")

    day = raw.filter(F.to_date(F.col("event_time")) == F.lit(dt).cast("date"))
    n = day.count()
    print(f"    {dt} 行数: {n:,}")
    if n == 0:
        raise SystemExit(f"{dt} 无数据，中止导入")

    day.withColumn("event_date", F.lit(dt).cast("date")) \
       .coalesce(4) \
       .write.mode("overwrite") \
       .partitionBy("event_date") \
       .option("compression", "snappy") \
       .parquet(ODS_PATH)

    # 补注册分区；失败仅告警，按路径读不受影响
    try:
        spark.sql(f"MSCK REPAIR TABLE {TABLE}")
        n_parts = spark.sql(f"SHOW PARTITIONS {TABLE}").count()
        print(f"  OK: 分区已注册（metastore 现有 {n_parts} 个分区）")
    except Exception as e:
        print(f"  WARN: MSCK REPAIR 失败（不致命，按路径读不受影响）：{str(e)[:100]}")

    print(f"  ODS 导入完成：{ODS_PATH}/event_date={dt} ({n:,} 行)")
    return n


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--dt", required=True)
    args = ap.parse_args()
    spark = get_spark()
    import_day(spark, args.dt)
    spark.stop()
