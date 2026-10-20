#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
gen_data.py —— 第 1 步：造一份刻意倾斜的商品销量明细
分布：9000001 占 45%（爆款，热点 key）；9000002~9000005 各 3%；其余 40 万商品 43%。
使用合成数据是因为热点占比可控（改 common.gen_data 的 0.45 可复现不同倾斜度）。
用法：
  python skew_lab/gen_data.py                  # 默认 2000 万行
  python skew_lab/gen_data.py --rows 50000000  # 换规模
"""

import argparse
import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
sys.path.insert(0, str(Path(__file__).resolve().parent))

from pyspark.sql import functions as F  # noqa: E402

from common import DATA_PATH, gen_data, make_spark  # noqa: E402


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--rows", type=int, default=20_000_000)
    args = ap.parse_args()

    spark = make_spark("SkewGenData", memory="3g")
    t0 = time.time()
    gen_data(spark, rows=args.rows)

    df = spark.read.parquet(DATA_PATH)
    n = df.count()
    print(f"   OK: 读回校验 {n:,} 行，耗时 {time.time() - t0:.1f}s")

    print("\n实际分布（Top6）:")
    for r in (df.groupBy("product_id").count()
              .orderBy(F.desc("count")).limit(6).collect()):
        print(f"   {r['product_id']:<10} {r['count']:>12,} 行  占 {r['count'] / n * 100:5.1f}%")
    spark.stop()


if __name__ == "__main__":
    main()
