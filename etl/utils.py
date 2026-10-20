# -*- coding: utf-8 -*-
"""数仓脚本共享工具：SparkSession 工厂 + HDFS 分区统计。"""

import os
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from config.config import HDFS_ROOT  # noqa: E402


def get_spark(app_name="WarehouseTask", memory=None, shuffle_partitions=100):
    """创建 SparkSession，默认连 standalone 集群（spark://localhost:7077）。

    可用环境变量覆盖：SPARK_MASTER / SPARK_DRIVER_MEMORY /
    SPARK_EXECUTOR_MEMORY / SPARK_EXECUTOR_CORES / SPARK_EXECUTOR_INSTANCES。
    切回本地模式：SPARK_MASTER=local[6]。
    """
    memory = memory or os.environ.get("SPARK_DRIVER_MEMORY", "1g")
    from pyspark.sql import SparkSession
    return (
        SparkSession.builder
        .master(os.environ.get("SPARK_MASTER", "spark://localhost:7077"))
        .appName(app_name)
        .config("spark.driver.memory", memory)
        .config("spark.executor.memory", os.environ.get("SPARK_EXECUTOR_MEMORY", "1g"))
        .config("spark.executor.cores", os.environ.get("SPARK_EXECUTOR_CORES", "2"))
        .config("spark.executor.instances", os.environ.get("SPARK_EXECUTOR_INSTANCES", "4"))
        .config("spark.hadoop.fs.defaultFS", HDFS_ROOT)
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
        # 源时间戳为 UTC；会话用 +8 会把每日 16:00 后的数据挪到次日分区
        .config("spark.sql.session.timeZone", "UTC")
        # INSERT OVERWRITE 只覆盖目标 dt 分区，保留历史分区
        .config("spark.sql.sources.partitionOverwriteMode", "dynamic")
        .config("spark.sql.shuffle.partitions", str(shuffle_partitions))
        .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
        .config("spark.ui.showConsoleProgress", "false")
        .getOrCreate()
    )


def drop_path(spark, path, keep_dir=True):
    """清空 HDFS 目录并保留空目录，用于全量重建前清理旧分区。
    外部表 DROP 不删文件，残留分区会导致列数不匹配；LOCATION 缺失报
    PATH_NOT_FOUND，故删除后需 mkdirs。
    """
    conf = spark.sparkContext._jsc.hadoopConfiguration()
    jpath = spark._jvm.org.apache.hadoop.fs.Path(path)
    fs = jpath.getFileSystem(conf)
    if fs.exists(jpath):
        fs.delete(jpath, True)
        print(f"  🗑  已清空 {path}")
    else:
        print(f"  (跳过) {path} 不存在")
    if keep_dir:
        fs.mkdirs(jpath)
    return True


def date_range(start, end):
    """闭区间日期列表 ['2019-10-01', ...]。"""
    from datetime import date as _date, timedelta
    s, e = _date.fromisoformat(start), _date.fromisoformat(end)
    out, d = [], s
    while d <= e:
        out.append(d.isoformat())
        d += timedelta(days=1)
    return out


def hdfs_partition_count(spark, path, row=True, recursive=True):
    """统计分区目录的行数（row=True）或文件数（row=False）。
    row=False 用于存在性判断，避免读取全量数据。
    """
    if row:
        try:
            return spark.read.parquet(path).count()
        except Exception:
            return 0
    try:
        conf = spark.sparkContext._jsc.hadoopConfiguration()
        jpath = spark._jvm.org.apache.hadoop.fs.Path(path)
        fs = jpath.getFileSystem(conf)
        it = fs.listStatus(jpath)
        files = [s for s in it if not s.isDirectory()]
        return len(files)
    except Exception:
        return 0
