#!/usr/bin/env bash
# 只停止由本工具记录、且命令仍匹配的进程。
set -euo pipefail
STATE="${ETL_MONITOR_STATE:-$HOME/.local/state/ecom-etl-monitor}"
for component in ui collector; do
    file="$STATE/$component.pid"
    [ -f "$file" ] || continue
    pid=$(cat "$file")
    if [[ "$pid" =~ ^[0-9]+$ ]] && [ -r "/proc/$pid/cmdline" ]; then
        cmd=$(tr '\0' ' ' < "/proc/$pid/cmdline")
        case "$component:$cmd" in
            ui:*monitoring/app.py*|collector:*monitoring/worker.sh*) kill "$pid";echo "已停止 $component ($pid)";;
            *) echo "PID 已被其它进程使用，跳过 $component";;
        esac
    fi
    rm -f "$file"
done
