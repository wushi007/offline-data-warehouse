#!/usr/bin/env bash
# =====================================================================
# skew_lab 一键实验：造数据 → 复现倾斜 → 加盐两阶段 → AQE 对照
# 用法:  bash skew_lab/run_all.sh [行数，默认 50000000]
# 各方案必须分进程执行：Session 缓存、AQE 学习与 JVM 预热会干扰同进程对比。
# =====================================================================
set -euo pipefail
cd "$(dirname "$0")/.."

ROWS="${1:-50000000}"
export JAVA_HOME="${JAVA_HOME:-/usr/lib/jvm/java-17-openjdk-amd64}"
export SPARK_HOME="${SPARK_HOME:-$HOME/apps/spark-3.5.9-bin-hadoop3}"
export HADOOP_HOME="${HADOOP_HOME:-$HOME/apps/hadoop-3.4.3}"
export HADOOP_CONF_DIR="${HADOOP_CONF_DIR:-$HADOOP_HOME/etc/hadoop}"

rm -f skew_lab/results/skew_bench.json

echo "########## 1/4 造数据（$ROWS 行）##########"
./run.sh table skew_lab/gen_data.py --rows "$ROWS"

for mode in naive salted_all salted_hot rebalance; do
    echo
    echo "########## 方案 $mode ##########"
    ./run.sh table skew_lab/bench_skew.py --mode "$mode"
done

echo
echo "########## 汇总 ##########"
.venv/bin/python - <<'PY'
import json
from pathlib import Path
rs = json.loads(Path("skew_lab/results/skew_bench.json").read_text())
print(f"{'方案':<34}{'总耗时(s)':>10}{'task数':>8}{'最慢(ms)':>10}{'中位(ms)':>10}{'倾斜倍数':>10}")
print("-" * 82)
for r in rs:
    a = r.get("all") or {}
    print(f"{r['label']:<34}{r['elapsed_sec']:>10}"
          f"{a.get('tasks', 0):>8}{a.get('dur_max_ms', 0):>10}"
          f"{a.get('dur_p50_ms', 0):>10}{str(a.get('skew_ratio')) + '×':>10}")
PY
