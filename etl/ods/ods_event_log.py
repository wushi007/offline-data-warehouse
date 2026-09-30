#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
ods_import.py — ODS 层接入
==========================
从源 CSV 按日期切分，零修改落盘到 ODS（Parquet + event_date 分区）。

原则：ODS 零修改，保留原始字段，仅提取 event_date 作为分区键；
     所有业务清洗统一收敛至 DWD 层。

用法：
  python scripts/ods_import.py --dt 2019-11-01
幂等：目标分区已存在且非空时跳过（不重复扫描 9GB CSV）。
"""

import argparse
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent))

from config.config import HDFS_ROOT, SRC_CSV_2019NOV, SRC_SCHEMA, ODS_PATH  # noqa: E402
from etl.utils import get_spark, hdfs_partition_count  # noqa: E402


def import_day(spark, dt: str):
    from pyspark.sql import functions as F

    # 幂等：分区已存在且非空 → 跳过
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

    # ★ 补注册分区：DataFrame 直写路径不会更新 Hive metastore，需 MSCK REPAIR
    #   （否则物理目录有了、metastore 里看不到；按路径读的作业不受影响）
    try:
        spark.sql(f"MSCK REPAIR TABLE {TABLE}")
    except Exception as e:
        print(f"  ⚠️ MSCK REPAIR 失败（不致命）：{str(e)[:80]}")

    print(f"  ODS 导入完成：{ODS_PATH}/event_date={dt} ({n:,} 行)")
    return n


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--dt", required=True)
    args = ap.parse_args()
    spark = get_spark()
    import_day(spark, args.dt)
    spark.stop()
