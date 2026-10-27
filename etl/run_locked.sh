#!/usr/bin/env bash
# run_locked.sh — 串行闸门：同一时刻只允许一个 Spark 任务运行。
#
# 调度器的并发限制只覆盖本 DAG，手动任务仍会并存；多个 SparkSession 并跑
# 可能触发 OOM killer。flock 在执行层串行，抢不到锁则等待（默认 7200 秒），
# 超时以非 0 退出，由 Airflow 按 retries 重试。
#
# 用法：etl/run_locked.sh <python> etl/dim/dim_user.py --dt 2019-11-01
# 环境变量：SPARK_LOCK 锁文件路径，SPARK_LOCK_WAIT 等锁秒数。
set -euo pipefail

# 敏感配置统一来自项目根目录 .env
REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
source "$REPO_ROOT/scripts/env.sh"

LOCK="${SPARK_LOCK:-/tmp/my-spark-warehouse.lock}"
WAIT="${SPARK_LOCK_WAIT:-7200}"

if [ "$#" -lt 1 ]; then
    echo "用法: $0 <命令> [参数...]" >&2
    exit 2
fi

# 拿到锁后执行命令，进程退出即释放锁
exec flock -w "$WAIT" "$LOCK" "$@"
