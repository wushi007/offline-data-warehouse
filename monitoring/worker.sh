#!/usr/bin/env bash
set -uo pipefail
ROOT=$(cd "$(dirname "$0")/.." && pwd -P)
STATE="${ETL_MONITOR_STATE:-$HOME/.local/state/ecom-etl-monitor}"
mkdir -p "$STATE"
exec 9>"$STATE/collector.lock"
flock -n 9 || exit 0
echo $$ > "$STATE/collector.pid"
child=''
cleanup() {
    [ -z "$child" ] || kill "$child" 2>/dev/null || true
    rm -f "$STATE/collector.pid"
}
trap cleanup EXIT
trap 'exit 0' TERM INT
while true; do
    bash "$ROOT/monitoring/collect_once.sh" &
    child=$!
    wait "$child" || true
    sleep 30 &
    child=$!
    wait "$child" || true
done
