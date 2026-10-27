#!/usr/bin/env bash
set -euo pipefail
ROOT=$(cd "$(dirname "$0")/.." && pwd -P)
: "${AIRFLOW_HOME:?请显式设置 AIRFLOW_HOME}"
case "${1:-}" in ''|--with-examples) ;; *) echo '用法：install_dags.sh [--with-examples]' >&2;exit 2;; esac
mkdir -p "$AIRFLOW_HOME/dags"
link_dag() {
    local src="$1" dst="$2"
    if [ -e "$dst" ] || [ -L "$dst" ]; then
        [ "$(readlink -f "$dst")" = "$src" ] || { echo "拒绝覆盖既有 DAG：$dst" >&2; exit 1; }
    else
        ln -s "$src" "$dst"
    fi
    echo "已安装：$dst"
}
link_dag "$ROOT/scheduler/incremental_warehouse_dag.py" "$AIRFLOW_HOME/dags/incremental_warehouse_dag.py"
if [ "${1:-}" = --with-examples ]; then
    link_dag "$ROOT/examples/sql_errors/dag.py" "$AIRFLOW_HOME/dags/sql_error_learning_dag.py"
fi
