"""在 Airflow venv 中运行。只读元数据和本地日志，写独立观测数据库。"""
import hashlib
import json
import os
from pathlib import Path
import sys
from datetime import datetime, timezone

sys.path.insert(0, str(Path(__file__).resolve().parent))
from store import connect
from parser import parse_errors, PARSER_VERSION


def collect():
    from airflow.models.taskinstance import TaskInstance
    from airflow.models.taskinstancehistory import TaskInstanceHistory
    from airflow.utils.session import create_session
    from airflow.configuration import conf

    now = datetime.now(timezone.utc).isoformat()
    log_root = Path(conf.get('logging', 'base_log_folder')).expanduser().resolve()
    records = {}
    with create_session() as session:
        # History 是过去尝试，TaskInstance 是当前状态。仅使用 SELECT。
        for model in (TaskInstanceHistory, TaskInstance):
            cols = [getattr(model, k) for k in ('dag_id','run_id','task_id','map_index',
                    'try_number','state','start_date','end_date','duration')]
            for row in session.query(*cols).all():
                data = dict(row._mapping)
                attempt = int(data.pop('try_number') or 0)
                data['attempt'] = attempt
                for field in ('start_date', 'end_date'):
                    data[field] = data[field].isoformat() if data[field] else None
                key = (data['dag_id'], data['run_id'], data['task_id'], data['map_index'], attempt)
                records[key] = data
    # 从日志补齐仍保留的旧尝试；无历史状态时明确标记未知，不能猜测成功。
    paths = {}
    for path in log_root.glob('dag_id=*/run_id=*/task_id=*/**/attempt=*.log'):
        parts = path.relative_to(log_root).parts
        try:
            dag, run, task = [p.split('=',1)[1] for p in parts[:3]]
            mapped = next((int(p.split('=',1)[1]) for p in parts[3:-1] if p.startswith('map_index=')), -1)
            attempt = int(path.stem.split('=',1)[1])
        except (ValueError, IndexError):
            continue
        if not path.resolve().is_relative_to(log_root):
            continue
        key = (dag, run, task, mapped, attempt)
        paths[key] = path
        if key not in records:
            records[key] = dict(dag_id=dag,run_id=run,task_id=task,map_index=mapped,
                attempt=attempt,state='unknown',start_date=None,end_date=None,duration=None)
    with connect() as db:
        for key, data in records.items():
            ident = hashlib.sha256(json.dumps(key).encode()).hexdigest()
            old = db.execute('SELECT log_stamp,raw_log,errors FROM attempts WHERE id=?',(ident,)).fetchone()
            path = paths.get(key)
            try:
                stat = path.stat() if path else None
                stamp = f'{PARSER_VERSION}:{stat.st_mtime_ns}:{stat.st_size}' if stat else None
            except OSError:
                path, stamp = None, None
            raw, errors = (old['raw_log'], old['errors']) if old else ('', '[]')
            if path and (not old or old['log_stamp'] != stamp):
                try:
                    raw = path.read_text(encoding='utf-8',errors='replace')
                    errors = json.dumps(parse_errors(raw),ensure_ascii=False)
                except OSError as exc:
                    print(f'日志暂不可读 {path}: {exc}',file=sys.stderr)
                    stamp = None
            data.update(id=ident,log_path=str(path) if path else None, log_stamp=stamp,
                        raw_log=raw,errors=errors,synced_at=now)
            keys = list(data)
            db.execute(f"INSERT INTO attempts ({','.join(keys)}) VALUES ({','.join('?' for _ in keys)}) "
                       f"ON CONFLICT(id) DO UPDATE SET {','.join(k+'=excluded.'+k for k in keys if k!='id')}",
                       [data[k] for k in keys])
        db.execute('INSERT INTO sync_status VALUES (1,?,NULL) ON CONFLICT(id) DO UPDATE SET last_success=excluded.last_success,error=NULL',(now,))
    print(json.dumps({'records': len(records), 'synced_at': now}))


if __name__ == '__main__':
    try:
        collect()
    except Exception as exc:
        with connect() as db:
            db.execute('INSERT INTO sync_status(id,error) VALUES (1,?) ON CONFLICT(id) DO UPDATE SET error=excluded.error',(str(exc),))
        raise
