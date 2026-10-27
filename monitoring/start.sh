#!/usr/bin/env bash
# 前台管理 UI 与本次启动的采集器；关闭 UI 后一并清理。
set -euo pipefail
ROOT=$(cd "$(dirname "$0")/.." && pwd -P)
STATE="${ETL_MONITOR_STATE:-$HOME/.local/state/ecom-etl-monitor}"
PY="${PYTHON:-$ROOT/.venv/bin/python}"
mkdir -p "$STATE"
exec 8>"$STATE/ui.lock"
flock -n 8 || { echo '运行台已启动，请勿重复启动。' >&2; exit 1; }
"$PY" -c 'import streamlit' || { echo '先安装 requirements/monitoring.txt' >&2;exit 1; }
ui_pid='' worker_pid=''
cleanup() {
    trap - EXIT TERM INT
    [ -z "$ui_pid" ] || kill "$ui_pid" 2>/dev/null || true
    [ -z "$worker_pid" ] || kill "$worker_pid" 2>/dev/null || true
    rm -f "$STATE/ui.pid"
}
trap cleanup EXIT
trap 'exit 0' TERM INT
bash "$ROOT/monitoring/worker.sh" 8>&- > "$STATE/collector.log" 2>&1 &
worker_pid=$!
"$PY" -m streamlit run "$ROOT/monitoring/app.py" \
    --server.headless true --server.address 127.0.0.1 --server.port "${ETL_MONITOR_PORT:-8502}" \
    --browser.gatherUsageStats false &
ui_pid=$!
echo "$ui_pid" > "$STATE/ui.pid"
wait "$ui_pid"
