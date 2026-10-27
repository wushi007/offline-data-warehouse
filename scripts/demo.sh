#!/usr/bin/env bash
# 使用 pip Spark 和隔离的本地 Hive metastore，不连接已有 HDFS/Metastore。
set -euo pipefail
ROOT=$(cd "$(dirname "$0")/.." && pwd -P)
PY="${PYTHON:-$ROOT/.venv/bin/python}"
[ -x "$PY" ] || { echo '缺少 Python 环境，请先安装 requirements.txt' >&2; exit 1; }
DEMO_ROOT=$(mktemp -d "${TMPDIR:-/tmp}/ecom-warehouse-demo.XXXXXXXX")
export DW_ENV_FILE=/dev/null DW_LOCAL_DEMO=1 DW_DEMO_ROOT="$DEMO_ROOT"
export DW_FS_URI=file:/// DW_WAREHOUSE_URI="file://$DEMO_ROOT/warehouse"
export DW_DB_PREFIX=demo_ SPARK_MASTER='local[2]' SPARK_DRIVER_MEMORY=1g
export PYSPARK_PYTHON="$PY" PYSPARK_DRIVER_PYTHON="$PY" SPARK_LOCAL_IP=127.0.0.1
unset SPARK_HOME SPARK_CONF_DIR HADOOP_CONF_DIR PYSPARK_SUBMIT_ARGS
# 目录独立、每次一个 metastore，保留输出以便检查；不会自动删除已有目录。
cd "$DEMO_ROOT"
export PYTHONPATH="$ROOT${PYTHONPATH:+:$PYTHONPATH}"
exec "$PY" "$ROOT/scripts/demo.py" "$@"
