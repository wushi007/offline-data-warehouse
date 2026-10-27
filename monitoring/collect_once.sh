#!/usr/bin/env bash
set -euo pipefail
ROOT=$(cd "$(dirname "$0")/.." && pwd -P)
source "${ETL_AIRFLOW_ENV:-$HOME/my-spark/airflow/airflow-env.sh}"
exec "${ETL_AIRFLOW_PYTHON:-$HOME/my-spark/airflow-venv/bin/python}" "$ROOT/monitoring/collect.py"
