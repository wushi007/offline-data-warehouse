# -*- coding: utf-8 -*-
"""数仓脚本共享工具：SparkSession 工厂 + HDFS 分区统计。"""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from config.config import HDFS_ROOT  # noqa: E402


def get_spark(app_name="WarehouseTask", memory="4g", shuffle_partitions=100):
    """统一 SparkSession：Kryo + AQE + Snappy，与旧项目配置一致。"""
    from pyspark.sql import SparkSession
    return (
        SparkSession.builder
        .master("local[6]")
        .appName(app_name)
        .config("spark.driver.memory", memory)
        .config("spark.hadoop.fs.defaultFS", HDFS_ROOT)
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
        # 源 CSV 时间戳明确标注 UTC，会话必须用 UTC，否则 to_date 在 +8 下会把
        # 每天 16:00-23:59 UTC 挪到次日分区（严重污染按日分区）
        .config("spark.sql.session.timeZone", "UTC")
        # 动态分区覆盖：INSERT OVERWRITE 只覆盖目标 dt 分区，保留其他历史分区（增量语义关键）
        .config("spark.sql.sources.partitionOverwriteMode", "dynamic")
        .config("spark.sql.shuffle.partitions", str(shuffle_partitions))
        .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
        .config("spark.ui.showConsoleProgress", "false")
        .getOrCreate()
    )


def drop_path(spark, path, keep_dir=True):
    """清空 HDFS 目录（全量重建前清旧文件），**删完保留空目录**。

    外部表 DROP TABLE 不会删 HDFS 文件，旧实现的残留目录（如旧 RFM 的 as_of_dt 分区）
    会被 Spark 推断进重建的表 → INSERT_COLUMN_ARITY_MISMATCH，所以重建前要清目录。

    但目录本身必须留着：表还注册在 metastore 上，LOCATION 不存在会直接报
    `AnalysisException: [PATH_NOT_FOUND]`（Spark 建表读取时校验路径），
    所以删完立刻 mkdirs 补回空目录。
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
    """统计 HDFS 路径（分区目录）的行数或文件数。

    row=True    : 返回该目录 parquet 行数（读元数据不可靠，走 Spark count）
    row=False   : 返回文件数（轻量，用于存在性判断）
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
