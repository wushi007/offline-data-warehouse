import argparse
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from config.config import HDFS_ROOT, SRC_CSV_2019NOV, SRC_SCHEMA, ODS_PATH  # noqa: E402
from utils import get_spark, hdfs_partition_count

spark = get_spark()
spark.sparkContext.setLogLevel("ERROR")
# 1. 切换数据库
spark.sql("USE default")
# 2. 查询表列表
df = spark.sql("SHOW TABLES")
df.show()

spark.stop()