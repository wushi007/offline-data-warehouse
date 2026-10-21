#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
injector.py — 在同一个 SparkContext 里按清单串行执行 ETL 脚本。

一层一个进程、一个 Session：层内表按顺序执行，脚本里的 spark.stop() 会被
拦截；运行期间临时替换 sys.argv / sys.path / get_spark，脚本本身不用改。

temp view 和 SQL conf 由整层共享，脚本需要用 createOrReplaceTempView 覆盖
自己用到的视图。缓存挂在 Context 上，每个任务结束后 clearCache()。

用法：
  python etl/injector.py --plan etl/plans/dwd.txt --dt 2019-11-01
  python etl/injector.py --plan etl/plans/dwd.txt --dt 2019-11-01 --dry-run
  python etl/injector.py 'etl/dwd/dwd_event_fact.py --dt 2019-11-01'
"""

from __future__ import annotations

import argparse
import os
import runpy
import shlex
import sys
import threading
import time
import traceback
from pathlib import Path

REPO = Path(__file__).resolve().parent.parent
if str(REPO) not in sys.path:
    sys.path.insert(0, str(REPO))

# base = 全局唯一的 SparkSession（getOrCreate 那个）；sess = 当前层复用的唯一 Session
_S = {"base": None, "sess": None, "real_get_spark": None, "real_stop": None}
_LOCK = threading.RLock()

# 每个任务开始前回填的 SQL conf。
# 复用一个 Context 时 builder 上的 .config() 会被 Spark 静默忽略
# （"Using an existing SparkSession..."），这些口径必须走 conf.set 才算数。
BASELINE_CONF = {
    # 源时间戳为 UTC；会话用 +8 会把每日 16:00 后的数据挪到次日分区
    "spark.sql.session.timeZone": os.environ.get("SPARK_SESSION_TZ", "UTC"),
    # INSERT OVERWRITE / df.write.mode("overwrite").partitionBy() 只覆盖目标 dt 分区。
    # 这一项丢了，ods_event_log 会把整个 ODS 目录清空 —— 不是只覆盖当天。
    "spark.sql.sources.partitionOverwriteMode": "dynamic",
    "spark.sql.adaptive.enabled": "true",
    "spark.sql.adaptive.coalescePartitions.enabled": "true",
    "spark.sql.shuffle.partitions": os.environ.get("SPARK_SHUFFLE_PARTITIONS", "100"),
}


def log(msg=""):
    print(msg, flush=True)


def ensure_conf(sess, extra=None):
    conf = dict(BASELINE_CONF)
    if extra:
        conf.update({k: str(v) for k, v in extra.items() if v not in (None, "")})
    for k, v in conf.items():
        try:
            if sess.conf.get(k, None) != v:
                sess.conf.set(k, v)
        except Exception:
            pass


# ------------------------------------------------------------------ 补丁
def _install():
    """保存原始实现并打上补丁（幂等）。"""
    if _S["real_get_spark"] is not None:
        return

    import etl.utils as u

    _S["real_get_spark"] = u.get_spark
    u.get_spark = _get_spark

    # etl/ddl.py:23、etl/check.py:8 写的是 `from utils import get_spark`，
    # 会以 "utils" 为名再加载一份 etl/utils.py。把别名指回同一模块，
    # 那两个脚本拿到的才是被接管的 get_spark。
    prev = sys.modules.get("utils")
    sys.modules["utils"] = u
    if prev is not None and prev is not u:
        log(f'  · sys.modules["utils"] 已指向 etl.utils（原 {getattr(prev, "__file__", "?")}）')

    from pyspark.sql import SparkSession

    _S["real_stop"] = SparkSession.stop

    def stop(self, *a, **kw):
        try:
            base = _S.get("base")
            # 子 Session 的 stop() 内部也是 self._sc.stop()，停的是共享 Context，
            # 所以按 Context 身份判断，而不是按 Session 身份。
            if base is not None and self._sc is base._sc:
                log("  · 拦截 spark.stop()：Context 由注入器持有")
                return
        except Exception:
            pass
        return _S["real_stop"](self, *a, **kw)

    SparkSession.stop = stop


def _get_spark(app_name="WarehouseTask", memory=None, shuffle_partitions=None):
    """替换 etl.utils.get_spark：返回当前层复用的 Session。

    memory 无法生效（Context 已建好，driver 堆改不了），shuffle_partitions 通过
    conf.set 真实生效。签名与 utils.get_spark 保持一致，脚本原样调用即可。
    """
    sess = _S.get("sess")
    if sess is None:                       # 兼容不经过 main() 直接调用 run_task 的场景
        sess = layer_session(app_name)
    if shuffle_partitions:
        try:
            sess.conf.set("spark.sql.shuffle.partitions", str(shuffle_partitions))
        except Exception:
            pass
    return sess


# ------------------------------------------------------------------ Session
def base_session(app_name="Injected"):
    """取全局唯一的 base Session；已停止则重建。"""
    _install()
    s = _S.get("base")
    if s is not None:
        try:
            if not s.sparkContext._jsc.sc().isStopped():
                return s
        except Exception:
            pass
        log("  共享 Context 已不可用，重建")
        _S["base"] = None
        _S["sess"] = None                  # 旧 Session 随旧 Context 失效

    s = _S["real_get_spark"](app_name)      # 必须用原函数，否则递归
    s.sparkContext.setLogLevel(os.environ.get("SPARK_LOG_LEVEL", "ERROR"))
    _S["base"] = s
    _S["sess"] = None                      # 新 Context 上还没有层 Session
    ensure_conf(s)
    return s


def layer_session(app_name="Injected"):
    """每层只建一个 Session，层内所有表串行复用；重复调用返回同一个。

    直接复用 base Session（不再 newSession），所以每层进程里只有一个
    SparkSession。并发约束由调用方保证：DAG 的 max_active_tasks=1 +
    run_locked.sh 的 flock，同一时刻只有一个 Session/一张表在跑。
    """
    with _LOCK:
        sess = _S.get("sess")
        if sess is None:
            sess = base_session(app_name)
            _S["sess"] = sess
            ensure_conf(sess)
            log("  · 本层唯一 Session 就绪，层内所有表串行复用（不逐表 newSession）")
        return sess


def stop_base():
    """真正停掉 Context（进程退出前调用；stop 补丁会绕过它，所以用原始实现）。"""
    s = _S.get("base")
    _S["base"] = None
    _S["sess"] = None
    if s is not None and _S.get("real_stop") is not None:
        try:
            _S["real_stop"](s)
        except Exception:
            pass


def _job_group(sc, gid, desc):
    """给当前任务的 job 打标签，Spark UI 上能按脚本名分组。"""
    try:
        if gid:
            sc.setJobGroup(gid, desc or "", False)
        else:
            sc.setLocalProperty("spark.jobGroup.id", None)
            sc.setLocalProperty("spark.job.description", None)
    except Exception:
        pass


def run_task(script, args):
    """等价于 `python <script> <args...>`，但跑在当前层的共享 Session 上。

    脚本抛出的 SystemExit 转成返回码，不掀翻整批；其它异常记 traceback 后
    同样只算本任务失败。层内不调用 spark.stop()，下一个脚本继续复用同一个
    Session / Context。
    """
    path = Path(script)
    if not path.is_absolute():
        path = (REPO / path).resolve()
    if not path.is_file():
        raise FileNotFoundError(f"找不到脚本：{path}")

    args = [str(a) for a in args]
    base = base_session()
    sess = layer_session()                   # 整层唯一 Session，表与表串行复用

    with _LOCK:
        ensure_conf(sess)
        _job_group(base.sparkContext, f"inject::{path.stem}", " ".join([path.name] + args))

        saved_argv = sys.argv
        saved_path = list(sys.path)
        sys.argv = [str(path)] + args
        sys.path.insert(0, str(path.parent))

        t0 = time.time()
        code, err = 0, None
        try:
            runpy.run_path(str(path), run_name="__main__")
        except SystemExit as e:
            c = e.code
            if c in (None, 0):
                pass                                  # 正常退出
            elif isinstance(c, int):
                code = c                              # sys.exit(1)
            else:
                code, err = 1, str(c)                 # raise SystemExit("FAIL: ...")
        except KeyboardInterrupt:
            code, err = 130, "KeyboardInterrupt"
        except BaseException:
            code, err = 1, traceback.format_exc(limit=10)
        finally:
            sec = time.time() - t0
            sys.argv = saved_argv
            sys.path[:] = saved_path
            _job_group(base.sparkContext, None, None)
            try:
                sess.catalog.clearCache()             # 缓存在 Context 级，隔离不了
            except Exception:
                pass
            # 不复位 _S["sess"]：整层复用，直到进程退出时 stop_base() 释放 Context

    return code, sec, err


# ------------------------------------------------------------------ 任务清单
def parse_spec(spec, dt=None, as_file=False):
    """解析一个来源。

    as_file=True  —— spec 是清单文件路径，每行 `脚本路径 参数...`，# 为注释
    as_file=False —— spec 本身就是一条任务命令
    """
    if as_file:
        p = Path(spec) if Path(spec).is_absolute() else REPO / spec
        if not p.is_file():
            raise SystemExit(f"清单文件不存在：{p}")
        lines = p.read_text(encoding="utf-8").splitlines()
    else:
        lines = [spec]

    tasks = []
    for raw in lines:
        line = raw.strip()
        if not line or line.startswith("#"):
            continue
        if "{dt}" in line:
            if not dt:
                raise SystemExit(f"清单里用了 {{dt}} 但没传 --dt：{line}")
            line = line.replace("{dt}", dt)
        parts = shlex.split(line)
        tasks.append((parts[0], parts[1:]))
    return tasks


# ------------------------------------------------------------------ CLI
EPILOG = """\
示例：
  ./run.sh inject --plan etl/plans/dwd.txt --dt 2019-11-01
  ./run.sh inject --plan etl/plans/dws.txt --dt 2019-11-01 --fail-fast
  ./run.sh inject --plan etl/plans/ads.txt --dt 2019-11-01 --only ads_trade_daily
  ./run.sh inject --plan etl/plans/dwd.txt --dt 2019-11-01 --dry-run
  ./run.sh inject 'etl/dws/dws_product_daily.py --dt 2019-11-01'
  ./run.sh inject --plan etl/plans/dwd.txt --dt 2019-11-01 --master 'local[2]'
"""


def main(argv=None):
    ap = argparse.ArgumentParser(
        prog="etl/injector.py",
        description="把多个 ETL 脚本注入同一个 SparkContext + 同一个 Session 串行执行（脚本零改动）",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=EPILOG,
    )
    ap.add_argument("tasks", nargs="*", help="一条或多条任务：'脚本路径 参数...'")
    ap.add_argument("--plan", action="append", default=[], metavar="FILE",
                    help="任务清单文件（每行一条，# 注释，支持 {dt}），可重复")
    ap.add_argument("--dt", help="替换清单里的 {dt} 占位符")
    ap.add_argument("--only", default="", help="只跑这些脚本（按文件名，逗号分隔）")
    ap.add_argument("--skip", default="", help="跳过这些脚本（按文件名，逗号分隔）")
    ap.add_argument("--app-name", default="Injected", help="Context 的 appName（UI 上可见）")
    ap.add_argument("--master", help="覆盖 master，如 'local[2]' / 'spark://localhost:7077'")
    ap.add_argument("--fail-fast", action="store_true", help="第一个失败就停（DAG 层内用这个）")
    ap.add_argument("--dry-run", action="store_true", help="只解析任务，不连集群")
    args = ap.parse_args(argv)

    if not args.plan and not args.tasks:
        ap.error("没有任务：用 --plan 指定清单，或直接列任务")

    tasks = []
    for spec in args.plan:
        tasks.extend(parse_spec(spec, args.dt, as_file=True))
    for spec in args.tasks:
        tasks.extend(parse_spec(spec, args.dt, as_file=False))
    if not tasks:
        ap.error("清单里没有有效任务行")

    only = {x.strip() for x in args.only.split(",") if x.strip()}
    skip = {x.strip() for x in args.skip.split(",") if x.strip()}
    if only:
        tasks = [t for t in tasks if Path(t[0]).stem in only]
    if skip:
        tasks = [t for t in tasks if Path(t[0]).stem not in skip]
    if not tasks:
        ap.error("筛选后没有任务，检查 --only / --skip")

    if args.dry_run:
        log(f"共 {len(tasks)} 个任务（dry-run，不连集群）：")
        for i, (s, a) in enumerate(tasks, 1):
            miss = "" if (Path(s).is_absolute() and Path(s).is_file()
                          or (REPO / s).is_file()) else "   文件不存在"
            log(f"  {i:2d}. {s} {' '.join(a)}{miss}")
        return 0

    if args.master:
        os.environ["SPARK_MASTER"] = args.master

    t_sess = time.time()
    base_session(args.app_name)
    layer_session(args.app_name)             # 层内唯一 Session，先建好再跑任务
    sess_sec = time.time() - t_sess
    log(f"共享 Context + 单一 Session 就绪（{sess_sec:.1f}s，仅此一次），"
        f"开始注入 {len(tasks)} 个任务（串行，同一时刻只处理一张表）")

    failed = done = 0
    t_all = time.time()
    for i, (s, a) in enumerate(tasks, 1):
        log(f"\n=== [{i}/{len(tasks)}] {s} {' '.join(a)} ===")
        code, sec, err = run_task(s, a)
        done += 1
        log(f"--- {'OK' if code == 0 else 'FAIL'} {Path(s).stem} {sec:.1f}s")
        if code:
            failed += 1
            if err:
                log(err)
            if args.fail_fast:
                log(f"FAIL-FAST：跳过剩余 {len(tasks) - i} 个任务")
                break

    log(f"\n批次结束：{done}/{len(tasks)} 个任务，失败 {failed}，"
        f"总耗时 {(time.time() - t_all) / 60:.1f} 分钟"
        f"（Context 冷启动 {sess_sec:.1f}s）")
    # 层内所有表跑完后才释放 Context；进程随即退出，不会再有表需要复用
    stop_base()
    return 1 if failed else 0


if __name__ == "__main__":
    sys.exit(main())
