# -*- coding: utf-8 -*-
"""
skew_lab 共享工具：SparkSession（带事件日志）、任务级指标、造数据
pip 版 PySpark 未暴露 SparkListener，故解析 driver 事件日志中的
SparkListenerTaskEnd（自包含，无需 History Server）。
指标按 task 粒度统计，判据为 max/p50 倍数，总耗时会被早完成的 task 稀释。
"""

import json
import os
import shutil
import statistics
import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from pyspark.sql import SparkSession, functions as F  # noqa: E402

EVENT_LOG_DIR = "/tmp/spark-skew-events"
DATA_PATH = "hdfs://localhost:8020/home/warehouse/hadoop-data/skew_lab/sales_detail"

HOT_PRODUCT = 9000001
WARM_PRODUCTS = [9000002, 9000003, 9000004, 9000005]
TAIL_MIN, TAIL_SPAN = 1000000, 400000
DEFAULT_ROWS = 20_000_000


def make_spark(app_name, memory=None, shuffle_partitions=200, extra=None):
    """建会话并开启事件日志（事件日志是后面拿 task 指标的唯一途径）。"""
    memory = memory or os.environ.get("SPARK_DRIVER_MEMORY", "3g")
    Path(EVENT_LOG_DIR).mkdir(parents=True, exist_ok=True)
    b = (SparkSession.builder
         .master(os.environ.get("SPARK_MASTER", "local[6]"))
         .appName(app_name)
         .config("spark.driver.memory", memory)
         .config("spark.hadoop.fs.defaultFS", "hdfs://localhost:8020")
         .config("spark.sql.adaptive.enabled", "true")
         .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
         .config("spark.sql.session.timeZone", "UTC")
         .config("spark.sql.sources.partitionOverwriteMode", "dynamic")
         .config("spark.sql.shuffle.partitions", str(shuffle_partitions))
         .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
         .config("spark.ui.showConsoleProgress", "false")
         .config("spark.eventLog.enabled", "true")
         .config("spark.eventLog.dir", f"file://{EVENT_LOG_DIR}"))
    for k, v in (extra or {}).items():
        b = b.config(k, v)
    spark = b.getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    return spark


# 事件日志解析
def clear_event_logs():
    """跑之前清空，保证解析到的永远是本次的记录（driver 退出时才落盘）。"""
    d = Path(EVENT_LOG_DIR)
    if d.exists():
        shutil.rmtree(d)
    d.mkdir(parents=True, exist_ok=True)


def parse_task_metrics():
    """解析最新一个事件日志，返回所有 task 的耗时与 shuffle 读写。

    事件日志在 driver 退出时才写完，所以必须在 spark.stop() 之后调用。
    """
    # 文件名是 appId，local 模式下形如 local-1790773008980，不是 app-*
    files = sorted((p for p in Path(EVENT_LOG_DIR).glob("*") if p.is_file()),
                   key=lambda p: p.stat().st_mtime)
    if not files:
        return []
    tasks = []
    with files[-1].open(encoding="utf-8") as fh:
        for line in fh:
            try:
                e = json.loads(line)
            except Exception:
                continue
            if e.get("Event") != "SparkListenerTaskEnd":
                continue
            info = e.get("Task Info") or {}
            m = e.get("Task Metrics") or {}
            sr = m.get("Shuffle Read Metrics") or {}
            sw = m.get("Shuffle Write Metrics") or {}
            # Task Info 无 Duration 字段，用 Finish Time - Launch Time 计算（毫秒）
            dur = (info.get("Finish Time") or 0) - (info.get("Launch Time") or 0)
            if dur <= 0:
                continue          # 没跑完 / 时间戳缺失的 task 不计入
            tasks.append({
                "stage": e.get("Stage ID"),
                "duration_ms": dur,
                # 事件日志无 Total Bytes Read，已拆为 Local / Remote（本地模式几乎全为 Local）
                "shuffle_read": (sr.get("Local Bytes Read") or 0) + (sr.get("Remote Bytes Read") or 0),
                "shuffle_records": sr.get("Total Records Read") or 0,
                "shuffle_write": sw.get("Shuffle Bytes Written") or 0,
                "input": (m.get("Input Metrics") or {}).get("Bytes Read") or 0,
            })
    return tasks


def summarize(tasks, only_shuffle_read=False):
    """汇总成倾斜指标。only_shuffle_read=True 时只看 reduce 侧 task（倾斜体现在这里）。"""
    ts = [t for t in tasks if (t["shuffle_read"] > 0 if only_shuffle_read else True)]
    if not ts:
        return {}
    d = sorted(t["duration_ms"] for t in ts)
    r = sorted(t["shuffle_read"] for t in ts)
    n = len(d)
    p50 = statistics.median(d)
    p50b = statistics.median(r)
    return {
        "tasks": n,
        "dur_max_ms": d[-1],
        "dur_p50_ms": round(p50),
        "dur_min_ms": d[0],
        "dur_sum_ms": sum(d),
        # 倾斜判据：最慢 task 相对中位数的倍数
        "skew_ratio": round(d[-1] / p50, 2) if p50 else None,
        "shuffle_read_max_mb": round(r[-1] / 1024 / 1024, 1),
        "shuffle_read_p50_mb": round(p50b / 1024 / 1024, 1),
        "shuffle_read_skew": round(r[-1] / p50b, 2) if p50b else None,
    }


def measure(spark, label, action):
    """跑一个 action，返回总耗时 + 结果行数。task 指标由调用方在 spark.stop() 后解析。"""
    t0 = time.time()
    rows = action()
    return {"label": label, "elapsed_sec": round(time.time() - t0, 2), "result_rows": rows}


def print_result(r):
    print(f"\n{'=' * 78}")
    print(f"  {r['label']}")
    print(f"{'=' * 78}")
    print(f"  总耗时          : {r['elapsed_sec']} s")
    print(f"  结果行数        : {r['result_rows']:,}")
    if not r.get("all"):
        print("  （未解析到 task 指标）")
        return
    a, s = r["all"], r.get("reduce", {})
    print(f"  ── 全部 task（{a['tasks']} 个）")
    print(f"     最慢 / 中位 / 最快 : {a['dur_max_ms']:,} / {a['dur_p50_ms']:,} / {a['dur_min_ms']:,} ms")
    print(f"     倾斜倍数        : {a['skew_ratio']}×  (最慢 ÷ 中位数)")
    if s:
        print(f"  ── reduce 侧 task（{s['tasks']} 个，倾斜体现在这里）")
        print(f"     最慢 / 中位        : {s['dur_max_ms']:,} / {s['dur_p50_ms']:,} ms")
        print(f"     倾斜倍数        : {s['skew_ratio']}×")
        print(f"     shuffle 读 最大/中位: {s['shuffle_read_max_mb']:,} / {s['shuffle_read_p50_mb']:,} MB")
        print(f"     数据倾斜倍数     : {s['shuffle_read_skew']}×")


def append_json(path, record):
    p = Path(path)
    p.parent.mkdir(parents=True, exist_ok=True)
    data = json.loads(p.read_text()) if p.exists() else []
    data.append(record)
    p.write_text(json.dumps(data, ensure_ascii=False, indent=2))


# 造数
def gen_data(spark, rows=DEFAULT_ROWS):
    """造一张商品销量明细：1 个爆款 45% + 4 个腰部各 3% + 长尾 43%。"""
    print(f"造数：{rows:,} 行，爆款 {HOT_PRODUCT} 占 45%")
    base = (spark.range(rows, numPartitions=64)
            .withColumn("r1", F.rand(1)).withColumn("r2", F.rand(2))
            .withColumn("r3", F.rand(3)).withColumn("r4", F.rand(4)))
    df = (base
          .withColumn("product_id", F.when(F.col("r1") < 0.45, F.lit(HOT_PRODUCT))
                      .when(F.col("r1") < 0.48, F.lit(WARM_PRODUCTS[0]))
                      .when(F.col("r1") < 0.51, F.lit(WARM_PRODUCTS[1]))
                      .when(F.col("r1") < 0.54, F.lit(WARM_PRODUCTS[2]))
                      .when(F.col("r1") < 0.57, F.lit(WARM_PRODUCTS[3]))
                      .otherwise((F.col("r2") * TAIL_SPAN + TAIL_MIN).cast("long")))
          .withColumn("user_id", (F.col("r3") * 3_000_000).cast("long"))
          .withColumn("price", F.round(F.col("r4") * 490 + 10, 2))
          .withColumn("quantity", (F.col("r3") * 3 + 1).cast("int"))
          .withColumn("sales_amount", F.round(F.col("price") * F.col("quantity"), 2))
          .select("product_id", "user_id", "price", "quantity", "sales_amount"))
    df.write.mode("overwrite").option("compression", "snappy").parquet(DATA_PATH)
    print(f"   OK: 已写入 {DATA_PATH}")
