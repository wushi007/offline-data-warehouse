#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
backfill.py — 按天批量回补 ODS 层

对 [--start, --end] 内每天执行 ods_import → ods_dqc，任一步失败则当日中断并记录。
用法：python etl/backfill.py --start 2019-11-01 --end 2019-11-02
"""

import argparse
import datetime
import os
import subprocess
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
VENV_PY = f"{ROOT.parent}/.venv/bin/python"

STEPS = [
    ("ods_import", "etl/ods/ods_event_log.py", "ODS 导入"),
    ("ods_dqc", "etl/ods/ods_event_log_dqc.py", "ODS DQC"),
]

# 子进程环境（与 Airflow TASK_ENV 一致）
ENV = dict(os.environ)
ENV.update({
    "JAVA_HOME": "/usr/lib/jvm/java-17-openjdk-amd64",
    "PATH": "/usr/lib/jvm/java-17-openjdk-amd64/bin:" + ENV.get("PATH", ""),
})


def run_day(day: str) -> bool:
    print(f"\n{'=' * 70}\n📅 回补 {day}\n{'=' * 70}")
    ok = True
    for key, script, label in STEPS:
        cmd = [VENV_PY, str(ROOT / script), "--dt", day]
        r = subprocess.run(cmd, env=ENV, capture_output=True, text=True)
        tail = "\n".join((r.stdout or r.stderr).strip().splitlines()[-4:])
        if r.returncode == 0:
            print(f"  OK: {label:10s} [{key}]")
        else:
            print(f"  FAIL: {label:10s} [{key}] 失败 (exit={r.returncode})")
            print("     " + tail.replace("\n", "\n     "))
            ok = False
            break
    return ok


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--start", required=True, help="起始日期 YYYY-MM-DD")
    ap.add_argument("--end", required=True, help="结束日期 YYYY-MM-DD（含）")
    args = ap.parse_args()

    d = datetime.date.fromisoformat(args.start)
    end = datetime.date.fromisoformat(args.end)
    failed_days = []
    while d <= end:
        if not run_day(d.isoformat()):
            failed_days.append(d.isoformat())
        d += datetime.timedelta(days=1)

    print(f"\n{'=' * 70}")
    if failed_days:
        print(f"WARN: 回补完成，失败 {len(failed_days)} 天：{failed_days}")
        sys.exit(1)
    print(f"🎉 回补全部成功（{args.start} ~ {args.end}）")


if __name__ == "__main__":
    main()
