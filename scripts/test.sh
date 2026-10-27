#!/usr/bin/env bash
set -euo pipefail
ROOT=$(cd "$(dirname "$0")/.." && pwd -P)
cd "$ROOT"
PY="${PYTHON:-$ROOT/.venv/bin/python}"
DW_ENV_FILE=/dev/null "$PY" -m unittest discover -s tests/unit -v
"$PY" -m unittest discover -s monitoring -p 'test_*.py' -v
for file in run.sh etl/run_locked.sh scripts/*.sh monitoring/*.sh scheduler/airflow_env.sh; do bash -n "$file"; done
