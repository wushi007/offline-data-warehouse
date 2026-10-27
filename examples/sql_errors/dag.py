"""三个任务、三个 Spark 进程；XCom 只传路径。默认转换失败，fix_sql=true 时成功。"""

import hashlib
import os
from pathlib import Path
import subprocess
from datetime import datetime, timedelta, timezone

from airflow.sdk import dag, task, get_current_context


def run_stage(stage, *args):
    root = Path(__file__).resolve().parents[2]
    cmd = [str(root / "run.sh"), "table", "examples/sql_errors/orders_etl.py", stage, *args]
    subprocess.run(
        cmd, cwd=root,
        env={**os.environ, "PYTHONUNBUFFERED": "1", "SPARK_LOCK_WAIT": "60"},
        check=True,
    )


def output_path(name):
    # 同一 run 重试使用同一路径，不同 run 隔离；不使用内置 hash（跨进程不稳定）。
    run_id = get_current_context()["run_id"]
    key = hashlib.sha256(run_id.encode()).hexdigest()
    path = Path("/tmp/airflow-orders-learning") / key / name
    path.parent.mkdir(parents=True, exist_ok=True)
    return path.as_uri()


@dag(
    dag_id="sql_error_learning_demo",
    schedule=None,
    start_date=datetime(2026, 1, 1, tzinfo=timezone.utc),
    catchup=False,
    max_active_runs=1,
    tags=["learning", "sql-error"],
    description="三任务订单 ETL：默认转换失败；conf 中 fix_sql=true 时全部成功",
)
def sql_error_learning_demo():
    @task(retries=1, retry_delay=timedelta(seconds=10),
          execution_timeout=timedelta(minutes=3))
    def extract_orders():
        path = output_path("raw")
        run_stage("extract", "--output", path)
        return path  # TaskFlow 自动写入 XCom：只传字符串，不传 DataFrame。

    @task(retries=0, execution_timeout=timedelta(minutes=3))
    def transform_orders(raw_path):
        fix = get_current_context()["dag_run"].conf.get("fix_sql", False)
        if not isinstance(fix, bool):
            raise ValueError("fix_sql 必须是 JSON 布尔值 true 或 false")
        path = output_path("aggregated")
        args = ["--input", raw_path, "--output", path]
        if fix:
            args.append("--fix-sql")
        run_stage("transform", *args)
        return path

    @task(retries=1, retry_delay=timedelta(seconds=10),
          execution_timeout=timedelta(minutes=2))
    def load_orders(result_path):
        run_stage("load", "--input", result_path)

    load_orders(transform_orders(extract_orders()))


sql_error_learning_demo()
