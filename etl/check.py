"""兼容旧诊断入口；跨层对账请使用 ./run.sh check。"""
from pathlib import Path
import sys
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from etl.utils import get_spark


def main():
    spark = get_spark("Warehouse_Tables")
    try:
        spark.sql("SHOW TABLES IN default").show()
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
