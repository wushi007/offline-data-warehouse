# incremental_warehouse_dag — 电商离线数仓增量 T+1 调度（TaskFlow API）
# 链路：ODS → DWD → DWS → DIM → ADS，一层一 task，层内共享一个 SparkContext + 一个 Session。
# 内存约束：同一时刻只处理一张表、只允许一个 SparkSession
#           （max_active_tasks=1 + run_locked.sh 的 flock）。
# 调度：@daily，end_date 取数据末日，数据为历史批次；HDFS 与 Airflow 需先启动。
"""增量数仓：ODS → DWD → DWS → DIM → ADS（一层一 task，由 backfill / 手动触发）。

- 写法：TaskFlow API（``@dag`` / ``@task``），每组计算走 etl/injector.py 一个子进程
- 会话：每层只建一个 SparkSession，层内逐表串行复用；不逐表 newSession / spark.stop
- 配置：Airflow Variable ``warehouse_env``（由 ``scheduler/airflow_env.sh`` 生成）
- 守卫：``scheduler/check_env.sh``；ODS / DWD DQC 不过则下游全部不跑
"""

import collections
import json
import os
import subprocess
import sys
from datetime import datetime, timedelta
from pathlib import Path

from airflow.sdk import dag, task

# 本文件经 airflow/dags/ 软链接加载，须按 resolve() 后的真实路径加入 sys.path，否则找不到 alerts。
sys.path.insert(0, str(Path(__file__).resolve().parent))
import alerts  # noqa: E402

# 配置来源：os.environ > Airflow Variable `warehouse_env` > 本文件默认值。
# `airflow variables import` 把 JSON 顶层每个 key 当作独立变量名，故须包一层
# {"warehouse_env": {...}}（见 scheduler/airflow_env.sh）；非 Airflow 环境下 import 会失败。
try:
    from airflow.exceptions import AirflowException
    from airflow.models import Variable as _Variable

    def _variable_env():
        """读 Variable `warehouse_env`；不存在/不是 JSON/不是对象 → 返回空 dict（走默认值）。"""
        try:
            raw = _Variable.get("warehouse_env")
        except AirflowException:          # 变量不存在
            return {}
        except Exception:                 # 连不上元数据库等
            return {}
        try:
            data = raw if isinstance(raw, dict) else json.loads(raw)
        except (TypeError, ValueError):
            return {}
        return data if isinstance(data, dict) else {}

except Exception:                          # pragma: no cover - 非 Airflow 环境
    def _variable_env():
        return {}


_REPO_ROOT = str(Path(__file__).resolve().parent.parent)

# 默认值从 airflow.cfg（DAG 进程自身读取的配置）取，不复制解析结果。
_AIRFLOW_HOME = None
try:
    from airflow.configuration import conf as _conf

    _CFG_DAGS_FOLDER = _conf.get("core", "dags_folder", fallback="")
    _AIRFLOW_HOME = Path(_CFG_DAGS_FOLDER).parent if _CFG_DAGS_FOLDER else None
except Exception:                              # pragma: no cover
    _CFG_DAGS_FOLDER = ""

_CFG = _variable_env()


def _cfg(key, default=None):
    """按优先级链取配置：os.environ > Variable(warehouse_env) > 默认值。"""
    for src in (os.environ.get(key.upper()), _CFG.get(key)):
        if src:
            return str(src)
    return default


def _pick_dir(patterns, fallback):
    """探测约定俗成的安装位置；都不存在时返回 fallback（由 check_env 任务负责报错）。"""
    import glob
    for pat in patterns:
        found = sorted(p for p in glob.glob(os.path.expanduser(pat)) if os.path.isdir(p))
        if found:
            return found[-1]
    return fallback


PROJECT_ROOT = _cfg("project_root", _REPO_ROOT)
VENV_PYTHON = _cfg("venv_python", f"{PROJECT_ROOT}/.venv/bin/python")
JAVA_HOME = _cfg("java_home", _pick_dir(
    ["/usr/lib/jvm/java-17-openjdk-*", "/usr/lib/jvm/java-17-*", "/usr/lib/jvm/default-java"],
    "/usr/lib/jvm/java-17-openjdk-amd64"))
SPARK_HOME = _cfg("spark_home", _pick_dir(
    ["~/apps/spark-*", "/opt/spark*", "/usr/local/spark*"], "/opt/spark"))
HADOOP_HOME = _cfg("hadoop_home", _pick_dir(
    ["~/apps/hadoop-[0-9]*", "/opt/hadoop*", "/usr/local/hadoop*"], "/opt/hadoop"))
HADOOP_CONF_DIR = _cfg("hadoop_conf_dir", f"{HADOOP_HOME}/etc/hadoop")
DATA_START = _cfg("data_start", "2019-10-01")   # 数仓数据起点（RFM/留存这类区间任务要用）
DATA_END = _cfg("data_end", "2019-11-30")       # DAG 时间窗右端（数据末日）

# ---------------- 子进程环境 ----------------
# Airflow 任务进程通常没有 .bashrc 里的 SPARK_HOME / HADOOP_CONF_DIR；缺失时 PySpark
# 回退到 venv 内的 pyspark 包目录，会话不挂 hive metastore，报 TABLE_OR_VIEW_NOT_FOUND。
# subprocess 以 {**os.environ, **TASK_ENV} 传入，无 BashOperator 的 append_env 问题。
TASK_ENV = {
    "JAVA_HOME": JAVA_HOME,
    "SPARK_HOME": SPARK_HOME,
    "HADOOP_HOME": HADOOP_HOME,
    "HADOOP_CONF_DIR": HADOOP_CONF_DIR,
    "SPARK_MASTER": _cfg("spark_master", "spark://localhost:7077"),
    "SPARK_DRIVER_MEMORY": _cfg("spark_driver_memory", "1g"),
    "SPARK_EXECUTOR_MEMORY": _cfg("spark_executor_memory", "1g"),
    "SPARK_EXECUTOR_CORES": _cfg("spark_executor_cores", "2"),
    "SPARK_EXECUTOR_INSTANCES": _cfg("spark_executor_instances", "4"),
    "PATH": (f"{JAVA_HOME}/bin:{SPARK_HOME}/bin:{HADOOP_HOME}/bin:{HADOOP_HOME}/sbin:"
             "/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin"),
}

# ---------------- 超时与告警分级（可被 Variable `warehouse_env` 覆盖）----------------
_DEFAULT_TIMEOUT_MIN = 20
# 按组分级：task_id 形如 <layer>_layer
_QUALITY_TASKS = {"ods_layer", "dwd_layer"}
_QUALITY_WARN_TASKS = {"ads_daily_layer"}

DEFAULT_TIMEOUT = timedelta(minutes=int(_cfg("default_timeout_min", _DEFAULT_TIMEOUT_MIN)))
QUALITY_TASKS = set(_CFG.get("quality_tasks") or _QUALITY_TASKS)
QUALITY_WARN_TASKS = set(_CFG.get("quality_warn_tasks") or _QUALITY_WARN_TASKS)


def failure_event(task_id):
    if task_id in QUALITY_TASKS:
        return "quality_block"
    if task_id in QUALITY_WARN_TASKS:
        return "quality_warn"
    return "failure"


def _render_check_env(tpl_path, mapping):
    tpl = Path(tpl_path).read_text(encoding="utf-8")
    for k, v in mapping.items():
        tpl = tpl.replace(f"__{k}__", str(v))
    return tpl


_CHECK_ENV_CMD = _render_check_env(
    Path(__file__).resolve().parent / "check_env.sh",
    {"VENV": VENV_PYTHON, "JAVA": JAVA_HOME, "SPARK": SPARK_HOME,
     "HADOOP": HADOOP_HOME, "CONF": HADOOP_CONF_DIR, "DAGS": _CFG_DAGS_FOLDER or "",
     "AIRFLOW_HOME": str(_AIRFLOW_HOME or ""), "OWNER_ROOT": PROJECT_ROOT,
     "SPARK_MASTER": _cfg("spark_master", "spark://localhost:7077")},
)

STEP_SCRIPTS = {
    "ods_import":            "etl/ods/ods_event_log.py",
    "ods_dqc":               "etl/ods/ods_event_log_dqc.py",
    "dwd_event_fact":        "etl/dwd/dwd_event_fact.py",
    "dwd_event_dirty":       "etl/dwd/dwd_event_dirty.py",
    "dwd_event_fact_dqc":    "etl/dwd/dwd_event_fact_dqc.py",
    "dws_user_session":      "etl/dws/dws_user_session_daily.py",
    "dws_product":           "etl/dws/dws_product_daily.py",
    "dws_user_behavior":     "etl/dws/dws_user_behavior_daily.py",
    "dws_traffic_hour":      "etl/dws/dws_traffic_hour_daily.py",
    "dim_product_scd2":      "etl/dim/dim_product_scd2.py",
    "dim_user":              "etl/dim/dim_user.py",
    "dim_session":           "etl/dim/dim_session.py",
    "dim_category":          "etl/dim/dim_category.py",
    "ads_trade":             "etl/ads/ads_trade_daily.py",
    "ads_conversion_funnel": "etl/ads/ads_conversion_funnel_daily.py",
    "ads_product_hot_rank":  "etl/ads/ads_product_hot_rank_daily.py",
    "ads_session_behavior":  "etl/ads/ads_session_behavior_daily.py",
    "ads_traffic_hour":      "etl/ads/ads_traffic_hour_daily.py",
    "ads_user_retention":    "etl/ads/ads_user_retention_daily.py",
    "ads_user_rfm":          "etl/ads/ads_user_rfm_snapshot.py",
}

# 区间/快照型：不走 --dt，按 [DATA_START, ds] 全区间算。
RANGE_TASKS = {"ads_user_retention", "ads_user_rfm"}

# ---------------- 分层（一层一个子进程）----------------
# 同一层的脚本跑在同一个子进程、同一个 SparkContext、同一个 SparkSession 里。
# 层内逐表串行（同一时刻只处理一张表），不再 per-table newSession()，也不 spark.stop()，
# 省掉逐表进程化带来的重复初始化（每个独立进程的 Context 冷启动单次 15~40s）。
# 每层 2~5 张：4~5 张是 1g driver 的实测安全上限（dws 4 张、dim 4 张含 2300 万行的
# dim_session 都跑过，无 OOM）。
LAYERS = {
    "ods":       ["ods_import", "ods_dqc"],
    "dwd":       ["dwd_event_fact", "dwd_event_dirty", "dwd_event_fact_dqc"],
    "dws":       ["dws_user_session", "dws_product", "dws_user_behavior", "dws_traffic_hour"],
    "dim":       ["dim_product_scd2", "dim_user", "dim_session", "dim_category"],
    "ads_daily": ["ads_trade", "ads_conversion_funnel", "ads_product_hot_rank",
                  "ads_session_behavior", "ads_traffic_hour"],
    "ads_range": ["ads_user_retention", "ads_user_rfm"],
}

# 组超时：按层给一个上限，层内多表串行共享同一 Session
LAYER_TIMEOUT_MIN = {"ods": 90, "dwd": 75, "dws": 110,
                     "dim": 120, "ads_daily": 130, "ads_range": 120}

# 非 tty 的子进程默认块缓冲；不设这两项，整组跑完日志才一次性刷出来
RUN_ENV = {**os.environ, **TASK_ENV,
           "PYTHONUNBUFFERED": "1",
           "PYTHONIOENCODING": "utf-8"}


def _plan_text(layer, ds):
    """把一组任务渲染成 injector 能读的清单（层内顺序即清单顺序）。"""
    lines = []
    for tid in LAYERS[layer]:
        args = ["--start", DATA_START, "--end", ds] if tid in RANGE_TASKS else ["--dt", ds]
        lines.append(" ".join([STEP_SCRIPTS[tid], *args]))
    return "\n".join(lines) + "\n"


def _run_layer(layer, ds):
    """一组脚本跑在一个子进程里：一个 SparkContext + 一个 Session，层内逐表串行。

    输出逐行转发（不是 capture_output），否则整组跑二十分钟 UI 上一直空白。
    """
    plan = Path(f"/tmp/dw-plan-{layer}-{ds}.txt")
    plan.write_text(_plan_text(layer, ds), encoding="utf-8")

    cmd = [f"{PROJECT_ROOT}/etl/run_locked.sh", VENV_PYTHON,
           f"{PROJECT_ROOT}/etl/injector.py",
           "--plan", str(plan), "--fail-fast", "--app-name", f"DW_{layer}"]

    print(f"[{layer}] 计划：\n{_plan_text(layer, ds)}", flush=True)
    print(f"[{layer}] $ {' '.join(cmd)}", flush=True)

    p = subprocess.Popen(cmd, env=RUN_ENV, stdout=subprocess.PIPE,
                         stderr=subprocess.STDOUT, text=True, bufsize=1)
    tail = collections.deque(maxlen=80)
    for line in p.stdout:
        print(line, end="", flush=True)
        tail.append(line)
    rc = p.wait()

    if rc != 0:
        raise RuntimeError(
            f"{layer} 组失败（退出码 {rc}）\n"
            f"计划:\n{_plan_text(layer, ds)}\n"
            f"末尾输出:\n{''.join(tail)[-1500:]}")
    return {"layer": layer, "ds": ds, "tasks": LAYERS[layer],
            "rc": rc, "mode": "inject"}


def _layer_options(layer):
    """@task 的公共选项（超时 / 重试 / 分级告警），按组取。"""
    return dict(
        task_id=f"{layer}_layer",
        # 整组重跑代价大，只给质量闸门那两组自动重试
        retries=1 if layer in ("ods", "dwd") else 0,
        retry_delay=timedelta(minutes=5),
        execution_timeout=timedelta(
            minutes=int(_cfg(f"{layer}_timeout_min", LAYER_TIMEOUT_MIN[layer]))),
        on_failure_callback=alerts.make_callback(failure_event(f"{layer}_layer")),
        on_retry_callback=alerts.make_callback("retry"),
    )


@dag(
    dag_id="incremental_warehouse_dag",
    description="增量数仓：ODS→DWD→DWS→DIM→ADS（一层一 task，层内单 Session 串行）",
    schedule="@daily",
    start_date=datetime.strptime(DATA_START, "%Y-%m-%d"),
    end_date=datetime.strptime(DATA_END, "%Y-%m-%d").replace(hour=23, minute=59, second=59),
    catchup=False,
    max_active_runs=1,
    max_active_tasks=1,          # 同一时刻只跑一个 Spark 任务（内存受限）
    tags=["spark", "warehouse", "incremental", "dqc"],
    default_args={
        "owner": _cfg("owner", "wushi007"),
        "depends_on_past": False,
        "retries": 0,
        "retry_delay": timedelta(minutes=2),
        "on_failure_callback": alerts.make_callback("failure"),
        "on_retry_callback": alerts.make_callback("retry"),
        "execution_timeout": DEFAULT_TIMEOUT,
    },
    doc_md=__doc__,
)
def incremental_warehouse():

    # ---- 0. 环境守卫（提前失败并给出可操作提示）----
    @task(task_id="check_env", retries=0)
    def check_env():
        p = subprocess.run(["bash", "-c", _CHECK_ENV_CMD],
                           env={**os.environ, **TASK_ENV},
                           capture_output=True, text=True)
        print(p.stdout or "", flush=True)
        if p.returncode != 0:
            print(p.stderr or "", file=sys.stderr, flush=True)
            raise RuntimeError(f"环境守卫失败（退出码 {p.returncode}）")
        return "env ok"

    # ---- 各组（每组一个子进程、一个 SparkContext）----
    @task(**_layer_options("ods"))
    def ods_layer(ds=None, upstream=None):
        """ODS 导入 + 质量闸门（闸门失败即断链）。"""
        return _run_layer("ods", ds)

    @task(**_layer_options("dwd"))
    def dwd_layer(ds=None, upstream=None):
        """事实表 + 脏数据隔离 + DWD 质量闸门。"""
        return _run_layer("dwd", ds)

    @task(**_layer_options("dws"))
    def dws_layer(ds=None, upstream=None):
        """四张 DWS 日报。"""
        return _run_layer("dws", ds)

    @task(**_layer_options("dim"))
    def dim_layer(ds=None, upstream=None):
        """四张维表（SCD2 全量重算 + 三张增量）。"""
        return _run_layer("dim", ds)

    @task(**_layer_options("ads_daily"))
    def ads_daily_layer(ds=None, upstream=None):
        """五张 ADS 单日日报；ads_trade 在前，带 GMV 波动阻断。"""
        return _run_layer("ads_daily", ds)

    @task(**_layer_options("ads_range"))
    def ads_range_layer(ds=None, upstream=None):
        """两张全区间任务（留存 / RFM 快照），最重，放最后。"""
        return _run_layer("ads_range", ds)

    # ================= 依赖（数据流） =================
    ok = check_env()

    l_ods = ods_layer(upstream=ok)
    l_dwd = dwd_layer(upstream=l_ods)
    l_dws = dws_layer(upstream=l_dwd)
    # dim 实际只读 dwd_event_fact（etl/dim/staging.py 的 FACT），串在 dws 后面
    # 只是为了让顺序确定 —— max_active_tasks=1，本来就是串行
    l_dim = dim_layer(upstream=l_dws)
    l_ads_daily = ads_daily_layer(upstream=l_dim)
    # retention 读 dws_user_behavior、rfm 读 dwd_event_fact，都已在上面各组里
    ads_range_layer(upstream=l_ads_daily)


incremental_warehouse()
