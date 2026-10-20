# incremental_warehouse_dag — 电商离线数仓增量 T+1 调度（TaskFlow API）
# 链路：ODS → DWD → DWS → DIM → ADS，一表一 task，各表独立子进程、可单独重试。
# 内存约束：同一时刻只允许一个 SparkSession（max_active_tasks=1 + run_locked.sh 的 flock）。
# 调度：@daily，end_date 取数据末日，数据为历史批次；HDFS 与 Airflow 需先启动。
"""增量数仓：ODS → DWD → DWS → DIM → ADS（一表一 task，由 backfill / 手动触发）。

- 写法：TaskFlow API（``@dag`` / ``@task``），计算走 etl/run_locked.sh 子进程
- 配置：Airflow Variable ``warehouse_env``（由 ``scheduler/airflow_env.sh`` 生成）
- 守卫：``scheduler/check_env.sh``；ODS / DWD DQC 不过则下游全部不跑
"""

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
    "PATH": (f"{JAVA_HOME}/bin:{SPARK_HOME}/bin:{HADOOP_HOME}/bin:{HADOOP_HOME}/sbin:"
             "/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin"),
}

# ---------------- 超时与告警分级（可被 Variable `warehouse_env` 覆盖）----------------
_DEFAULT_TIMEOUT_MIN = 20
_TIMEOUT_OVERRIDES = {"ods_import": 45, "dim_session": 30, "ads_user_rfm": 45}
_QUALITY_TASKS = {"ods_dqc", "dwd_event_fact_dqc"}
_QUALITY_WARN_TASKS = {"ads_trade"}

DEFAULT_TIMEOUT = timedelta(minutes=int(_cfg("default_timeout_min", _DEFAULT_TIMEOUT_MIN)))
TIMEOUTS = {
    k: timedelta(minutes=v)
    for k, v in dict((_CFG.get("timeout_overrides_min") or _TIMEOUT_OVERRIDES)).items()
}
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
     "AIRFLOW_HOME": str(_AIRFLOW_HOME or ""), "OWNER_ROOT": PROJECT_ROOT},
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


def _run_step(task_id, ds):
    """在独立子进程里跑一个 ETL 脚本，返回写入 XCom 的 dict。

    经 etl/run_locked.sh 的 flock 闸门执行，全机同一时刻只允许一个 SparkSession。
    """
    script = STEP_SCRIPTS[task_id]
    args = ["--start", DATA_START, "--end", ds] if task_id in RANGE_TASKS else ["--dt", ds]
    cmd = [f"{PROJECT_ROOT}/etl/run_locked.sh", VENV_PYTHON,
           f"{PROJECT_ROOT}/{script}", *args]
    print(f"[{task_id}] $ {' '.join(cmd)}", flush=True)

    p = subprocess.run(cmd, env={**os.environ, **TASK_ENV},
                       capture_output=True, text=True)
    if p.stdout:
        print(p.stdout, flush=True)
    if p.stderr:
        print(p.stderr, file=sys.stderr, flush=True)

    if p.returncode != 0:
        raise RuntimeError(
            f"{task_id} 失败（退出码 {p.returncode}）：{script}\n"
            f"命令: {' '.join(cmd)}\n"
            f"stderr 末尾:\n{(p.stderr or '')[-1500:]}"
        )
    return {"task_id": task_id, "script": script, "rc": p.returncode, "ds": ds}


def _step_options(task_id):
    """@task 的公共选项（超时 / 重试 / 分级告警），按 task_id 取。"""
    return dict(
        task_id=task_id,
        retries=1,
        retry_delay=timedelta(minutes=2),
        execution_timeout=TIMEOUTS.get(task_id, DEFAULT_TIMEOUT),
        on_failure_callback=alerts.make_callback(failure_event(task_id)),
        on_retry_callback=alerts.make_callback("retry"),
    )


@dag(
    dag_id="incremental_warehouse_dag",
    description="增量数仓：ODS→DWD→DWS→DIM→ADS（一表一 task，由 backfill/手动触发）",
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

    # ---- 1. ODS ----
    # ds 是 TaskFlow 模板上下文变量：在签名中声明即由 Airflow 注入当前 run 的逻辑日期。
    @task(**_step_options("ods_import"))
    def ods_import(ds=None, upstream=None):
        """ODS 导入：把当日原始日志灌进 HDFS（只读路径，不碰 metastore）。

        upstream 接收 check_env 的返回值，使本任务依赖 check_env 成功。
        """
        return _run_step("ods_import", ds)

    @task(**_step_options("ods_dqc"))
    def ods_dqc(ds=None, upstream=None):
        return _run_step("ods_dqc", ds)

    # ---- 2. DWD ----
    # TaskFlow 依赖由函数参数表达：upstream 接收上游返回值（经 XCom）。
    @task(**_step_options("dwd_event_fact"))
    def dwd_event_fact(ds=None, upstream=None):
        return _run_step("dwd_event_fact", ds)

    @task(**_step_options("dwd_event_dirty"))
    def dwd_event_dirty(ds=None, upstream=None):
        return _run_step("dwd_event_dirty", ds)

    @task(**_step_options("dwd_event_fact_dqc"))
    def dwd_event_fact_dqc(ds=None, fact=None, dirty=None):
        """DWD 质量闸门：账对平 / 去重键唯一 / 空值率 / 枚举 / 价格非负 / 时区一致。

        需等事实表与脏数据表都写完才校验（账对平需两边行数），故有 fact / dirty 两个上游参数。
        """
        return _run_step("dwd_event_fact_dqc", ds)

    # ---- 3. DWS（四张，各依赖 DWD 闸门） ----
    @task(**_step_options("dws_user_session"))
    def dws_user_session(ds=None, upstream=None):
        return _run_step("dws_user_session", ds)

    @task(**_step_options("dws_product"))
    def dws_product(ds=None, upstream=None):
        return _run_step("dws_product", ds)

    @task(**_step_options("dws_user_behavior"))
    def dws_user_behavior(ds=None, upstream=None):
        return _run_step("dws_user_behavior", ds)

    @task(**_step_options("dws_traffic_hour"))
    def dws_traffic_hour(ds=None, upstream=None):
        return _run_step("dws_traffic_hour", ds)

    # ---- 4. DIM（四张，增量只算当天） ----
    @task(**_step_options("dim_product_scd2"))
    def dim_product_scd2(ds=None, upstream=None):
        return _run_step("dim_product_scd2", ds)

    @task(**_step_options("dim_user"))
    def dim_user(ds=None, upstream=None):
        return _run_step("dim_user", ds)

    @task(**_step_options("dim_session"))
    def dim_session(ds=None, upstream=None):
        return _run_step("dim_session", ds)

    @task(**_step_options("dim_category"))
    def dim_category(ds=None, upstream=None):
        return _run_step("dim_category", ds)

    # ---- 5. ADS（日报五张） ----
    @task(**_step_options("ads_trade"))
    def ads_trade(ds=None, upstream=None):
        return _run_step("ads_trade", ds)

    @task(**_step_options("ads_conversion_funnel"))
    def ads_conversion_funnel(ds=None, upstream=None):
        return _run_step("ads_conversion_funnel", ds)

    @task(**_step_options("ads_product_hot_rank"))
    def ads_product_hot_rank(ds=None, upstream=None):
        return _run_step("ads_product_hot_rank", ds)

    @task(**_step_options("ads_session_behavior"))
    def ads_session_behavior(ds=None, upstream=None):
        return _run_step("ads_session_behavior", ds)

    @task(**_step_options("ads_traffic_hour"))
    def ads_traffic_hour(ds=None, upstream=None):
        return _run_step("ads_traffic_hour", ds)

    # ---- 6. ADS（区间/快照型） ----
    @task(**_step_options("ads_user_retention"))
    def ads_user_retention(ds=None, upstream=None):
        """留存：按 [DATA_START, ds] 全区间算（不是单日）。"""
        return _run_step("ads_user_retention", ds)

    @task(**_step_options("ads_user_rfm"))
    def ads_user_rfm(ds=None, upstream=None):
        """RFM 快照：按 [DATA_START, ds] 全区间算。"""
        return _run_step("ads_user_rfm", ds)

    # ================= 依赖（数据流） =================
    ok = check_env()

    ods_imp = ods_import(upstream=ok)
    ods_chk = ods_dqc(upstream=ods_imp)

    # DWD：事实表与脏数据表同源同规则，均只依赖 ods_dqc（可并行）
    fact = dwd_event_fact(upstream=ods_chk)
    dirty = dwd_event_dirty(upstream=ods_chk)
    dwd_gate = dwd_event_fact_dqc(fact=fact, dirty=dirty)

    # DWS 四张 + DIM 四张：都等 DWD 闸门
    sess = dws_user_session(upstream=dwd_gate)
    prod = dws_product(upstream=dwd_gate)
    beh = dws_user_behavior(upstream=dwd_gate)
    hour = dws_traffic_hour(upstream=dwd_gate)

    dim_product_scd2(upstream=dwd_gate)
    dim_user(upstream=dwd_gate)
    dim_session(upstream=dwd_gate)
    dim_category(upstream=dwd_gate)

    # ADS：各自依赖其来源层
    ads_trade(upstream=beh)
    ads_user_retention(upstream=beh)
    ads_conversion_funnel(upstream=dwd_gate)
    ads_user_rfm(upstream=dwd_gate)
    ads_product_hot_rank(upstream=prod)
    ads_session_behavior(upstream=sess)
    ads_traffic_hour(upstream=hour)


incremental_warehouse()
