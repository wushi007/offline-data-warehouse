#!/usr/bin/env bash
# ============================================================
# run_locked.sh — 串行闸门：同一时刻只允许一个 Spark 任务在跑
#
# 为什么需要它（而不是只靠 Airflow 的 max_active_tasks=1）：
#   · DAG 级并发设置只管本 DAG，机器上还有别的 DAG / 手动跑的表脚本，
#     它们各自会起自己的 SparkSession，几个顶在一起就被内核 OOM killer 干掉
#     （本机 7.6G，StarRocks/HDFS/Kafka 常驻约 2.5G，实测被 OOM 杀过多次）。
#   · flock 锁在"执行 Spark"这一层做串行，与调度器配置无关：
#     Airflow 任务之间、手动跑单表之间、两者交叉，都会排队而不是并跑。
#
# 行为：抢不到锁就**等待**（默认最多等 2 小时），而不是直接失败；
#       等超时以非 0 退出，Airflow 会标记失败并按 retries 重试。
#
# 用法：
#   etl/run_locked.sh <项目根目录>/.venv/bin/python etl/dim/dim_user.py --dt 2019-11-01
# 可用环境变量覆盖：
#   SPARK_LOCK        锁文件路径（默认 /tmp/my-spark-warehouse.lock）
#   SPARK_LOCK_WAIT   等锁秒数（默认 7200）
# ============================================================
set -euo pipefail

LOCK="${SPARK_LOCK:-/tmp/my-spark-warehouse.lock}"
WAIT="${SPARK_LOCK_WAIT:-7200}"

if [ "$#" -lt 1 ]; then
    echo "用法: $0 <命令> [参数...]" >&2
    exit 2
fi

# -w WAIT：最多等 WAIT 秒；拿到锁后执行命令，进程退出即释放
exec flock -w "$WAIT" "$LOCK" "$@"
