import argparse
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from config.config import HDFS_ROOT, SRC_CSV_2019NOV, SRC_SCHEMA, ODS_PATH  # noqa: E402
from utils import get_spark, hdfs_partition_count

spark = get_spark()
spark.sparkContext.setLogLevel("ERROR")
spark.sql("USE default")
df = spark.sql("SHOW TABLES")
df.show()

spark.stop()