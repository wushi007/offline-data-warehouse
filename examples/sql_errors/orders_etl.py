"""独立进程执行 ETL 的一个阶段，使用本地 Parquet 交接数据。"""

import argparse
from pyspark.sql import SparkSession


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("stage", choices=["extract", "transform", "load"])
    parser.add_argument("--input")
    parser.add_argument("--output")
    parser.add_argument("--fix-sql", action="store_true")
    args = parser.parse_args()
    if args.stage != "extract" and not args.input:
        parser.error("transform/load 需要 --input")
    if args.stage != "load" and not args.output:
        parser.error("extract/transform 需要 --output")
    spark = (
        SparkSession.builder.master("local[1]")
        .appName(f"Orders_Learning_{args.stage}")
        .config("spark.sql.catalogImplementation", "in-memory")
        .config("spark.eventLog.enabled", "false")
        .config("spark.sql.shuffle.partitions", "1")
        .getOrCreate()
    )
    try:
        if args.stage == "extract":
            print(f"[EXTRACT] 三行订单 → {args.output}", flush=True)
            spark.createDataFrame(
                [(1, "alice", 100), (2, "alice", 50), (3, "bob", 80)],
                "order_id INT, customer STRING, amount INT",
            ).write.mode("overwrite").parquet(args.output)
        elif args.stage == "transform":
            spark.read.parquet(args.input).createOrReplaceTempView("demo_orders")
            sql = "SELECT customer, SUM(amount) AS total_amount\nFROM demo_orders\nGROUP BY"
            if args.fix_sql:
                sql += " customer"
            print(f"[TRANSFORM] input={args.input}\nSQL:\n{sql}", flush=True)
            spark.sql(sql).write.mode("overwrite").parquet(args.output)
        else:
            print(f"[LOAD] 读取 {args.input}，展示结果（不写业务表）", flush=True)
            spark.read.parquet(args.input).orderBy("customer").show()
        print(f"[DONE] {args.stage} 成功", flush=True)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
