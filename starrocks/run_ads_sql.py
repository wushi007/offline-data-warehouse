#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
run_ads_sql.py — 在 StarRocks 里执行 starrocks/sql/ads/ 下的 ADS 建表 SQL
默认写 ads_v2：就地重建会覆盖 Hive 复刻的参照物，对账一致后再切换。
SQL 中的 ${DB} ${dt} ${start} ${end} ${d1} ${d3} ${d7} 由本脚本替换。
文件头 -- MODE: daily 为逐日执行，snapshot 为整个区间执行一次。
用法: python starrocks/run_ads_sql.py --dt 2019-10-01 | --start ... --end ... | --only T
"""

import argparse
import re
import subprocess
import sys
import time
from datetime import date, timedelta
from pathlib import Path

SQL_DIR = Path(__file__).resolve().parent / "sql" / "ads"


def mysql(sql, host, port, user, database=None):
    """把 SQL 经 stdin 喂给 mysql 客户端（一条管道可跑多语句）。"""
    cmd = ["mysql", "-h", host, "-P", str(port), "-u", user,
           "--default-character-set=utf8mb4"]
    if database:
        cmd.append(database)
    p = subprocess.run(cmd, input=sql, capture_output=True, text=True, timeout=7200)
    if p.returncode != 0:
        raise RuntimeError((p.stderr or p.stdout or "").strip()[:400])
    return p.stdout


def query(sql, host, port, user):
    cmd = ["mysql", "-h", host, "-P", str(port), "-u", user,
           "--default-character-set=utf8mb4", "-N", "-B"]
    p = subprocess.run(cmd, input=sql, capture_output=True, text=True, timeout=600)
    if p.returncode != 0:
        raise RuntimeError((p.stderr or "").strip()[:400])
    return (p.stdout or "").strip()


def ensure_tables(db, host, port, user, tables):
    """在目标库建表，schema 从 ads 库复制（保证与复刻版完全一致，便于对账）。"""
    query(f"CREATE DATABASE IF NOT EXISTS `{db}`", host, port, user)
    for t in tables:
        try:
            query(f"CREATE TABLE IF NOT EXISTS `{db}`.`{t}` LIKE `ads`.`{t}`",
                  host, port, user)
            continue
        except RuntimeError:
            pass
        try:
            # 回退 1：SHOW CREATE TABLE 拿到 DDL，把库名换掉再执行
            ddl = query(f"SHOW CREATE TABLE `ads`.`{t}`", host, port, user)
            ddl = re.sub(r"CREATE TABLE `?ads`?\.", f"CREATE TABLE `{db}`.", ddl)
            ddl = ddl.replace(f"CREATE TABLE `{t}`", f"CREATE TABLE `{db}`.`{t}`", 1)
            query(ddl, host, port, user)
            continue
        except RuntimeError:
            pass
        # 回退 2：ads 库无此表（如 ads_ec_daily_overview）。StarRocks 的
        # IF NOT EXISTS 不会短路，LIKE 引用不存在的表直接报 1064；
        # 目标库也没有时明确报错，不静默建一张缺列的表。
        n = query(f"SELECT COUNT(*) FROM information_schema.tables "
                  f"WHERE table_schema = '{db}' AND table_name = '{t}'",
                  host, port, user)
        if (n or "").strip() == "0":
            raise RuntimeError(
                f"`{db}`.`{t}` 不存在，且 `ads`.`{t}` 没有参照物 —— "
                f"请先执行 starrocks/sql/ddl_{t}.sql 手工建表")


def parse_mode(text):
    m = re.search(r"^--\s*MODE:\s*(\w+)", text, re.M)
    if not m:
        raise SystemExit(f"缺少 -- MODE: 声明：{SQL_DIR}")
    return m.group(1).lower()


def substitute(text, mapping):
    for k, v in mapping.items():
        text = text.replace("${" + k + "}", v)
    left = re.findall(r"\$\{(\w+)\}", text)
    if left:
        raise RuntimeError(f"还有未替换的变量: {sorted(set(left))}")
    return text


def plus(d, n):
    return (date.fromisoformat(d) + timedelta(days=n)).isoformat()


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--dt", help="单天")
    ap.add_argument("--start")
    ap.add_argument("--end")
    ap.add_argument("--db", default="ads_v2", help="目标库（默认 ads_v2）")
    ap.add_argument("--only", nargs="*", help="只跑指定的表")
    ap.add_argument("--host", default="127.0.0.1")
    ap.add_argument("--port", type=int, default=9030)
    ap.add_argument("--user", default="root")
    ap.add_argument("--dir", default=str(SQL_DIR))
    args = ap.parse_args()

    d = Path(args.dir)
    files = sorted(d.glob("*.sql"))
    if args.only:
        want = {f"{t}.sql" if not t.endswith(".sql") else t for t in args.only}
        files = [f for f in files if f.name in want]

    # 区间：显式给就用显式的；否则取 DWS 的实际覆盖范围
    if args.dt:
        days = [args.dt]
        start, end = args.dt, args.dt
    else:
        start = args.start or query(
            "SELECT MIN(event_date) FROM dws.dws_user_behavior_daily",
            args.host, args.port, args.user)
        end = args.end or query(
            "SELECT MAX(event_date) FROM dws.dws_user_behavior_daily",
            args.host, args.port, args.user)
        s, e = date.fromisoformat(start), date.fromisoformat(end)
        days = [(s + timedelta(days=i)).isoformat() for i in range((e - s).days + 1)]

    print(f"目标库 {args.db}｜区间 {start} ~ {end}（{len(days)} 天）｜{len(files)} 个文件")
    ensure_tables(args.db, args.host, args.port, args.user,
                  [f.stem for f in files])
    print()

    failures = []
    for f in files:
        text = f.read_text(encoding="utf-8")
        mode = parse_mode(text)
        tbl = f.stem
        t0 = time.time()

        if mode == "snapshot":
            sql = substitute(text, {"DB": args.db, "start": start, "end": end})
            try:
                mysql(sql, args.host, args.port, args.user)
                n = query(f"SELECT COUNT(*) FROM `{args.db}`.`{tbl}`",
                          args.host, args.port, args.user)
                print(f"  OK: {tbl:<30s} 快照 {n:>10s} 行  ({time.time()-t0:.0f}s)")
            except Exception as ex:
                print(f"  FAIL: {tbl:<30s} {type(ex).__name__}: {str(ex)[:150]}")
                failures.append(tbl)
            continue

        ok_days = 0
        for dt in days:
            sql = substitute(text, {
                "DB": args.db, "dt": dt, "start": start, "end": end,
                "d1": plus(dt, 1), "d3": plus(dt, 3), "d7": plus(dt, 7),
            })
            try:
                mysql(sql, args.host, args.port, args.user)
                ok_days += 1
            except Exception as ex:
                print(f"  FAIL: {tbl:<30s} {dt}: {type(ex).__name__}: {str(ex)[:150]}")
                failures.append(f"{tbl}@{dt}")
                break
        n = query(f"SELECT COUNT(*) FROM `{args.db}`.`{tbl}`",
                  args.host, args.port, args.user)
        print(f"  {'OK' if ok_days == len(days) else 'FAIL'} {tbl:<30s} "
              f"{ok_days}/{len(days)} 天  合计 {n:>10s} 行  ({time.time()-t0:.0f}s)")

    print()
    if failures:
        print(f"FAIL: 失败 {len(failures)} 项：{failures[:5]}")
        sys.exit(1)
    print(f"OK: 全部完成 → 库 `{args.db}`")


if __name__ == "__main__":
    main()
