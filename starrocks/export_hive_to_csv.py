#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
export_hive_to_csv.py —— 把 Hive 里已有的数仓表导出为 CSV，供 StarRocks Stream Load 灌入
======================================================================================
设计前提：**不重算**。只读取 Spark 已经跑出来的结果（DWS / ADS / DIM 的现有分区），
原样搬到 StarRocks，行数与内容应与 Hive 侧完全一致。

为什么先导出成 CSV 再灌，而不是让 StarRocks 直连 Hive：
  · 本机 16G 内存、WSL2 只分到 7.6G，StarRocks 容器 + Spark 驱动同时跑必 OOM
  · 拆成「导出 → 起容器 → 灌数据」两步，两个重活**串行**执行，内存互不挤占
  · Stream Load 是 StarRocks 原生的批量导入通道（走 BE 8040），比 JDBC 逐行插快几个量级

CSV 格式约定（必须与 Stream Load 的解析参数对得上，见 load_to_starrocks.sh）：
  · 无表头            —— 列名由 Stream Load 的 columns 参数显式指定
  · 字段分隔 ','      —— 与 Stream Load 的 column_separator 一致
  · NULL 写成 \\N     —— StarRocks 默认 NULL 标记，避免空字符串被当成空值
  · timestamp 写成 'yyyy-MM-dd HH:mm:ss' —— Spark 默认 ISO 格式带 T 和时区，StarRocks 解析不了
  · 每表一个文件（coalesce(1)），方便一次 PUT 灌完

用法：
  python starrocks/export_hive_to_csv.py                      # 导出全部 16 张表
  python starrocks/export_hive_to_csv.py --tables dws.dws_traffic_hour_daily
  python starrocks/export_hive_to_csv.py --out /tmp/sr_export # 换输出目录
"""

import argparse
import json
import os
import shutil
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from etl.utils import get_spark  # noqa: E402

# ---------------------------------------------------------------------------
# 待导出的表：db.table -> (分区列名 or None)
# 顺序按「先小后大」，这样即使中途出问题，也已经把最有价值的汇总表落盘了。
# ---------------------------------------------------------------------------
TABLES = [
    # ---- ADS：小表，行数几十到几百 ----
    ("ads.ads_trade_daily",             "event_date"),
    ("ads.ads_conversion_funnel_daily", "event_date"),
    ("ads.ads_session_behavior_daily",  "event_date"),
    ("ads.ads_user_retention_daily",    "event_date"),
    ("ads.ads_traffic_hour_daily",      "event_date"),
    ("ads.ads_product_hot_rank_daily",  "event_date"),
    ("ads.ads_user_rfm_snapshot",       "as_of_dt"),
    # ---- DWS ----
    ("dws.dws_traffic_hour_daily",      "event_date"),
    ("dws.dws_product_daily",           "event_date"),
    ("dws.dws_user_behavior_daily",     "event_date"),
    ("dws.dws_user_session_daily",      "event_date"),
    # ---- DIM：全量表（无分区列）----
    ("dim.dim_date",                    None),
    ("dim.dim_category",                None),
    ("dim.dim_product_scd2",            None),
    ("dim.dim_user",                    None),
    ("dim.dim_session",                 None),
]

CSV_OPTS = {
    "header": "false",
    "sep": ",",
    "nullValue": r"\N",                     # StarRocks 默认 NULL 标记
    "timestampFormat": "yyyy-MM-dd HH:mm:ss",   # Spark 默认的 ISO 格式 SR 解析不了
    "dateFormat": "yyyy-MM-dd",
    "encoding": "UTF-8",
    "quote": '"',
    "escape": "\\",
    "emptyValue": "",
}


def export_one(spark, table, part_col, out_dir):
    """导出单表，返回 manifest 条目。"""
    db, tbl = table.split(".")
    df = spark.table(table)

    # 分区列放在最后，方便 Stream Load 的 columns 参数与建表顺序对齐
    if part_col and part_col in df.columns:
        df = df.select([c for c in df.columns if c != part_col] + [part_col])

    cols = df.columns
    n = df.count()          # 先数一遍，用于事后核对（也顺带触发一次真实的读）

    tmp = out_dir / f".tmp_{tbl}"
    final = out_dir / f"{tbl}.csv"
    if tmp.exists():
        shutil.rmtree(tmp)
    if final.exists():
        final.unlink()

    # ★ 必须写成 file:// 的绝对 URI。
    #   get_spark() 里设了 spark.hadoop.fs.defaultFS=hdfs://...，裸的本机路径
    #   （如 /home/lst/sr_export/...）会被当成 HDFS 路径，文件写进 HDFS 而不是本地磁盘，
    #   后面 Stream Load 自然找不到。用 as_uri() 显式指定 scheme 才安全。
    df.coalesce(1).write.mode("overwrite").options(**CSV_OPTS).csv(tmp.resolve().as_uri())

    # Spark 写出的是 part-xxxxx-<uuid>.csv，挪成稳定的 <表名>.csv
    parts = sorted(p for p in tmp.glob("part-*.csv"))
    if not parts:
        raise RuntimeError(f"{table}: 没找到写出的 part 文件")
    shutil.move(str(parts[0]), str(final))
    shutil.rmtree(tmp)

    return {
        "db": db,
        "table": tbl,
        "partition_col": part_col,
        "columns": cols,
        "rows": n,
        "csv": str(final),
        "bytes": final.stat().st_size,
    }


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--tables", nargs="*", help="只导出指定的表（db.table），默认全部")
    ap.add_argument("--out", default=str(Path.home() / "sr_export"), help="CSV 输出目录")
    ap.add_argument("--memory", default="3g", help="driver 内存（本机内存紧张，别给太大）")
    args = ap.parse_args()

    out_dir = Path(args.out)
    out_dir.mkdir(parents=True, exist_ok=True)

    todo = TABLES
    if args.tables:
        want = set(args.tables)
        todo = [t for t in TABLES if t[0] in want]
        missing = want - {t[0] for t in todo}
        if missing:
            raise SystemExit(f"未知的表：{sorted(missing)}")

    spark = get_spark("ExportHiveToCSV", memory=args.memory)
    spark.sparkContext.setLogLevel("ERROR")

    print(f"📤 导出 {len(todo)} 张表 → {out_dir}")
    print(f"   CSV 约定：无表头 / 逗号分隔 / NULL=\\N / timestamp=yyyy-MM-dd HH:mm:ss")
    print()

    manifest = []
    for table, part_col in todo:
        try:
            m = export_one(spark, table, part_col, out_dir)
            manifest.append(m)
            mb = m["bytes"] / 1024 / 1024
            print(f"  ✅ {table:<34s} {m['rows']:>12,d} 行   {mb:>8.1f} MB")
        except Exception as e:
            print(f"  ❌ {table:<34s} {type(e).__name__}: {e}")
            manifest.append({"db": table.split(".")[0], "table": table.split(".")[1],
                             "partition_col": part_col, "error": str(e)})
    spark.stop()

    mf = out_dir / "manifest.json"
    mf.write_text(json.dumps(manifest, ensure_ascii=False, indent=2), encoding="utf-8")

    ok = [m for m in manifest if "rows" in m]
    print()
    print(f"✅ 完成：{len(ok)}/{len(todo)} 张表，共 {sum(m['rows'] for m in ok):,} 行")
    print(f"   清单：{mf}")


if __name__ == "__main__":
    main()
