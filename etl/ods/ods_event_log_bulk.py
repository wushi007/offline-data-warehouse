#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
ods_import_bulk.py — ODS 批量导入（单次扫描整份 CSV，切出全部日期分区）
========================================================================
与 ods_import.py 同口径（零修改、UTC 切分区），但一次读完整份 CSV、
按 event_date 全量分区写出，避免按天重复扫描数 GB 源文件。

用于把 2019-Oct.csv 的 31 天一次性落盘到 ods_event_log。
已存在的分区在 dynamic overwrite 下保持不变（不影响 11 月已建分区）。

用法：
  python etl/ods_import_bulk.py --csv hdfs://localhost:8020/home/lst/hadoop-data/eCommerce_behavior/2019-Oct.csv
"""

import argparse
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent))

from config.config import ODS_PATH, SRC_SCHEMA  # noqa: E402
from etl.utils import get_spark  # noqa: E402


def import_csv(spark, csv_path: str):
    from pyspark.sql import functions as F

    print(f"📥 读取源 CSV：{csv_path}")
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

    # ★ 补注册分区：这里是 **DataFrame 直写路径**，不是 INSERT INTO TABLE，
    #   所以 Hive metastore **不会**自动登记这些分区（踩过：物理 61 个分区、
    #   metastore 只有 34 个）。按路径读的作业不受影响，但 Hive/SQL 侧会看不到新分区。
    register_partitions(spark)

    print(f"✅ ODS 批量导入完成：{ODS_PATH}（{len(dates)} 个分区，dynamic 覆盖不影响已有分区）")
    return len(dates)


def register_partitions(spark):
    """MSCK REPAIR 把 HDFS 上新增的分区目录登记进 metastore（目录与注册必须一起动）。"""
    try:
        spark.sql(f"MSCK REPAIR TABLE {TABLE}")
        n = spark.sql(f"SHOW PARTITIONS {TABLE}").count()
        print(f"  ✅ 分区已注册（metastore 现有 {n} 个分区）")
    except Exception as e:  # 表还没建等情况：不致命，按路径读仍可用
        print(f"  ⚠️ MSCK REPAIR 失败（不致命，按路径读不受影响）：{str(e)[:100]}")


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
