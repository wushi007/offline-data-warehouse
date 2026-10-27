"""独立观测数据库，不修改 Airflow 元数据库。"""
import os
from pathlib import Path
import sqlite3

DB_PATH = Path(os.environ.get('ETL_MONITOR_DB', '~/.local/state/ecom-etl-monitor/records.db')).expanduser()


def connect():
    DB_PATH.parent.mkdir(parents=True, exist_ok=True)
    db = sqlite3.connect(DB_PATH, timeout=30)
    db.row_factory = sqlite3.Row
    db.execute('PRAGMA journal_mode=WAL')
    db.execute('''CREATE TABLE IF NOT EXISTS attempts (
        id TEXT PRIMARY KEY, dag_id TEXT, run_id TEXT, task_id TEXT,
        map_index INTEGER, attempt INTEGER, state TEXT, start_date TEXT,
        end_date TEXT, duration REAL, log_path TEXT, log_stamp TEXT,
        raw_log TEXT, errors TEXT NOT NULL DEFAULT '[]', synced_at TEXT)''')
    db.execute('''CREATE TABLE IF NOT EXISTS sync_status (
        id INTEGER PRIMARY KEY CHECK(id=1), last_success TEXT, error TEXT)''')
    return db
