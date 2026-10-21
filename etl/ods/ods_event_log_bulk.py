#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
ods_event_log_bulk.py — ODS 批量导入

单次扫描整份 CSV，按 event_date 写出全部分区，避免按天重复扫描大文件；
已有分区在 overwrite 下保持不变。与 ods_event_log.py 口径一致。

用法：python etl/ods/ods_event_log_bulk.py --csv <源 CSV 的 HDFS 路径>
"""

import argparse
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent))

from config.config import DBS, ODS_PATH, SRC_SCHEMA  # noqa: E402
from etl.utils import get_spark  # noqa: E402

# MSCK REPAIR 目标表，必须为库名.表名
TABLE = f"{DBS['ods']}.ods_event_log"


def import_csv(spark, csv_path: str):
    from pyspark.sql import functions as F

    print(f"读取源 CSV：{csv_path}")
    raw = spark.read.csv(csv_path, schema=SRC_SCHEMA,
                         header=True, sep=",", timestampFormat="yyyy-MM-dd HH:mm:ss z")

    df = raw.withColumn("event_date", F.to_date(F.col("event_time")))

    total = df.count()
    dates = sorted(str(r[0]) for r in
                   df.select("event_date").distinct().collect())
    print(f"   源总行数: {total:,}")
    print(f"   覆盖日期 {len(dates)} 天: {dates[0]} ~ {dates[-1]}")

    df.write \
      .mode("overwrite") \
      .partitionBy("event_date") \
      .option("compression", "snappy") \
      .parquet(ODS_PATH)

    register_partitions(spark)

    print(f"OK: ODS 批量导入完成：{ODS_PATH}（{len(dates)} 个分区，dynamic 覆盖不影响已有分区）")
    return len(dates)


def register_partitions(spark):
    """MSCK REPAIR 登记 HDFS 新增分区；DataFrame 直写不会自动更新 metastore。"""
    try:
        spark.sql(f"MSCK REPAIR TABLE {TABLE}")
        n = spark.sql(f"SHOW PARTITIONS {TABLE}").count()
        print(f"  OK: 分区已注册（metastore 现有 {n} 个分区）")
    except Exception as e:  # 不致命，按路径读仍可用
        print(f"  WARN: MSCK REPAIR 失败（不致命，按路径读不受影响）：{str(e)[:100]}")


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--csv", required=True, help="源 CSV 的 HDFS 路径")
    args = ap.parse_args()
    spark = get_spark("ODS_Import_Bulk")
    spark.sparkContext.setLogLevel("ERROR")
    try:
        import_csv(spark, args.csv)
    finally:
        spark.stop()
