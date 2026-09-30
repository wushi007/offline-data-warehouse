# =====================================================================
# incremental_warehouse_dag — 电商离线数仓 增量 T+1 调度
#
# 链路（一表一个 task，每表独立进程、可单独重试）：
#   ODS 导入 → ODS DQC
#     → dwd_event_fact → dwd_event_dirty → dwd DQC
#     → dws 四张（user_session / product / user_behavior / traffic_hour）
#     → dim 四张（product_scd2 / user / session / category）—— 增量，只算当天
#     → ads 五张日报 → ads retention + rfm（区间/快照型）
#
# 质量闸门（任一失败 → 下游自动阻断）：
#   · ODS DQC：源数据完整性/格式校验
#   · DWD DQC：账对平 / 去重键唯一性 / 空值率 / 枚举 / 价格非负 / 时区一致性
#
# 增量语义：按 event_date 分区静态 INSERT OVERWRITE，每日仅算当日；
#           重跑指定 ds 自动覆盖旧分区，DQC 同步重校验。
#
# 调度方式：schedule="@daily" + 固定 end_date（关在数据区间内），由 backfill / 手动触发驱动。
#           为什么不用 schedule=None：
#             Airflow 3 的 backfill **拒绝** NullTimetable（schedule=None）的 DAG
#             （DagNonPeriodicScheduleException），写 None 会把回填这条路堵死。
#           为什么 end_date 必须早于"真实的今天"：
#             数据是历史批次（2019-10/11），@daily 会让调度器为"今天"建 run，
#             而 ODS 里没有那天的数据 → 必失败。end_date 一过即不再生成新 run，
#             回填 2019-11 仍在窗口内，正常可用。
#           回填示例：
#             airflow backfill create --dag-id incremental_warehouse_dag \
#                 --from-date 2019-11-01 --to-date 2019-11-30 --max-active-runs 1
#
# ⚠️ 内存约束：同一时刻只允许一个 SparkSession。原因：
#     宿主 16G 内存，WSL2 默认只分到约一半（实测可见 7.6G），
#     而 HDFS / MySQL / Airflow 常驻约 2.5G —— 多会话并存会被内核 OOM killer 杀掉。
#   两道保障：
#      1) max_active_tasks=1 + max_active_runs=1 —— 本 DAG 内串行；
#      2) 所有 Spark 任务经 etl/run_locked.sh 的 flock 闸门 —— 跨 DAG、跨手动执行也串行
#         （机器上若有别的 DAG 会起 Spark，DAG 级配置管不住它）。
#
# 运行前提：HDFS 与 Airflow 已启动（check_env 任务会守卫）。
# =====================================================================

import glob
import os
import sys
from datetime import datetime, timedelta
from pathlib import Path

from airflow import DAG
from airflow.providers.standard.operators.bash import BashOperator

# 告警模块在 scheduler/ 下（与本文件同级）。
# 注意：Airflow 是通过 airflow/dags/ 里的**软链接**加载本文件的，所以这里显式按
# resolve() 后的真实路径把 scheduler/ 加进 sys.path —— 不然 `import alerts` 会找不到。
sys.path.insert(0, str(Path(__file__).resolve().parent))
import alerts  # noqa: E402

# ---------------- 路径常量 ----------------
# 全部支持环境变量覆盖，并给出合理默认值 —— 默认值优先从本文件位置推导，
# 所以把仓库 clone 到任何目录都能直接跑，无需改代码。
# 需要覆盖时（比如 Spark/Hadoop 装在别处），在 Airflow 的环境里导出：
#   export DW_PROJECT_ROOT=/path/to/ecom-warehouse
#   export DW_VENV_PYTHON=/path/to/venv/bin/python
#   export JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
#   export SPARK_HOME=/opt/spark
#   export HADOOP_HOME=/opt/hadoop
#
# 本文件在 <repo>/scheduler/ 下，故 repo 根 = 上两级目录
_REPO_ROOT = str(Path(__file__).resolve().parent.parent)


def _first_existing(candidates, fallback):
    """返回第一个存在的路径；都不存在时返回 fallback（由 check_env 任务负责报错）。"""
    for c in candidates:
        if c and os.path.exists(c):
            return c
    return fallback


def _pick_dir(env_key, patterns, fallback):
    """按优先级取一个目录：环境变量 > 探测常见安装位置 > fallback。

    为什么要探测：Spark / Hadoop 的安装位置因人而异，写死会让人 clone 后跑不起来，
    而完全不探测又要求每次都导出环境变量。折中做法是探测几个约定俗成的路径。
    """
    v = os.environ.get(env_key)
    if v:
        return v
    for pat in patterns:
        # 只取目录（排除 .tgz 之类的安装包），版本号排序取最新
        found = sorted(p for p in glob.glob(os.path.expanduser(pat)) if os.path.isdir(p))
        if found:
            return found[-1]
    return fallback


PROJECT_ROOT = os.environ.get("DW_PROJECT_ROOT", _REPO_ROOT)

# venv 位置按候选顺序探测（与 run.sh 的策略一致）：
# 环境变量 → 仓库内 .venv → 同级 .venv → 家目录 .venv
# 若 venv 在别处，建议在仓库根建软链接：ln -s /path/to/venv .venv
VENV_PYTHON = _first_existing(
    [
        os.environ.get("DW_VENV_PYTHON"),
        f"{PROJECT_ROOT}/.venv/bin/python",
        f"{PROJECT_ROOT}/../.venv/bin/python",
        os.path.expanduser("~/.venv/bin/python"),
    ],
    f"{PROJECT_ROOT}/.venv/bin/python",
)

JAVA_HOME = _pick_dir(
    "JAVA_HOME",
    ["/usr/lib/jvm/java-17-openjdk-*", "/usr/lib/jvm/java-17-*", "/usr/lib/jvm/default-java"],
    "/usr/lib/jvm/java-17-openjdk-amd64",
)
SPARK_HOME = _pick_dir(
    "SPARK_HOME",
    ["~/apps/spark-*", "/opt/spark*", "/usr/local/spark*"],
    "/opt/spark",
)
HADOOP_HOME = _pick_dir(
    "HADOOP_HOME",
    ["~/apps/hadoop-[0-9]*", "/opt/hadoop*", "/usr/local/hadoop*"],
    "/opt/hadoop",
)
HADOOP_CONF_DIR = os.environ.get("HADOOP_CONF_DIR", f"{HADOOP_HOME}/etc/hadoop")
DATA_START = os.environ.get("DW_DATA_START", "2019-10-01")   # 数仓数据起点（RFM/留存这类区间任务要用）

# ★ 为什么 SPARK_HOME / HADOOP_CONF_DIR 必须显式写在这里：
#   Airflow 的 BashOperator.get_env() 逻辑是——
#       env = self.env
#       if env is None: env = os.environ.copy()
#       elif self.append_env: system_env.update(env); env = system_env
#   **默认 append_env=False**，所以一旦传了 env，任务进程就只拿到这个字典，
#   `.bashrc` 里的 SPARK_HOME/HADOOP_CONF_DIR 全都丢掉 → PySpark 的 _find_spark_home()
#   回退到 venv 里的 pyspark 包目录（那里没有 conf/spark-defaults.conf）
#   → 会话不挂 hive metastore → 报 `TABLE_OR_VIEW_NOT_FOUND: dwd.dwd_event_fact`。
#   （ODS 两步只读 HDFS 路径、不碰 metastore，所以它们不报错——这就是当时"只有 DWD 挂"的原因。）
#   已在 build_task 里同时打开 append_env=True 兜底；这两个变量仍显式列出，
#   保证"不管 Airflow 是怎么启动的"都能跑。
TASK_ENV = {
    "JAVA_HOME": JAVA_HOME,
    "SPARK_HOME": SPARK_HOME,
    "HADOOP_HOME": HADOOP_HOME,
    "HADOOP_CONF_DIR": HADOOP_CONF_DIR,
    "PATH": (f"{JAVA_HOME}/bin:{SPARK_HOME}/bin:{HADOOP_HOME}/bin:{HADOOP_HOME}/sbin:"
             "/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin"),
}

# ---------------- 超时与告警分级 ----------------
# 超时：单表单天通常 30~60 秒，给足余量但要能兜住"卡死"——因为 max_active_tasks=1，
#       一个卡住的任务会霸占唯一的并发位，把整天堵死。
DEFAULT_TIMEOUT = timedelta(minutes=20)
TIMEOUTS = {
    "ods_import":   timedelta(minutes=45),   # 最慢：要扫整份 9GB CSV 抽当天
    "dim_session":  timedelta(minutes=30),   # 955 万行维表
    "ads_user_rfm": timedelta(minutes=45),   # 全量快照，随数据增长而变慢
}

# DQC 任务的失败语义是"数据质量不达标，已阻断下游"，与"脚本崩了"要分开报警，
# 否则收到 `🔴 任务失败` 根本不知道是代码问题还是数据问题。
QUALITY_TASKS = {"ods_dqc", "dwd_event_fact_dqc"}

# 指标异常（闸门之外的检查）：ads_trade_daily 会校验 GMV 相对近 7 日均值的波动，
# 超 30%（config.DQC_GMV_VOLATILITY）就非 0 退出。它是叶子节点、没有下游可拦，
# 所以用 quality_warn（"值超阈值，未阻断下游"）而不是 quality_block —— 报警措辞要对得上事实，
# 否则收到"已拦住下游"却发现什么都没被拦，会让人对告警失去信任。
QUALITY_WARN_TASKS = {"ads_trade"}


def failure_event(task_id):
    if task_id in QUALITY_TASKS:
        return "quality_block"
    if task_id in QUALITY_WARN_TASKS:
        return "quality_warn"
    return "failure"

# ---------------- 一表一脚本 ----------------
STEP_SCRIPTS = {
    # ODS
    "ods_import":            "etl/ods/ods_event_log.py",
    "ods_dqc":               "etl/ods/ods_event_log_dqc.py",
    # DWD
    "dwd_event_fact":        "etl/dwd/dwd_event_fact.py",
    "dwd_event_dirty":       "etl/dwd/dwd_event_dirty.py",
    "dwd_event_fact_dqc":    "etl/dwd/dwd_event_fact_dqc.py",
    # DWS
    "dws_user_session":      "etl/dws/dws_user_session_daily.py",
    "dws_product":           "etl/dws/dws_product_daily.py",
    "dws_user_behavior":     "etl/dws/dws_user_behavior_daily.py",
    "dws_traffic_hour":      "etl/dws/dws_traffic_hour_daily.py",
    # DIM（增量：只算当天）
    "dim_product_scd2":      "etl/dim/dim_product_scd2.py",
    "dim_user":              "etl/dim/dim_user.py",
    "dim_session":           "etl/dim/dim_session.py",
    "dim_category":          "etl/dim/dim_category.py",
    # ADS（每日报表）
    "ads_trade":             "etl/ads/ads_trade_daily.py",
    "ads_conversion_funnel": "etl/ads/ads_conversion_funnel_daily.py",
    "ads_product_hot_rank":  "etl/ads/ads_product_hot_rank_daily.py",
    "ads_session_behavior":  "etl/ads/ads_session_behavior_daily.py",
    "ads_traffic_hour":      "etl/ads/ads_traffic_hour_daily.py",
    # ADS（区间/快照型，非按天）
    "ads_user_retention":    "etl/ads/ads_user_retention_daily.py",
    "ads_user_rfm":          "etl/ads/ads_user_rfm_snapshot.py",
}


def build_task(task_id, args=None):
    """args 为 None 时默认传 --dt {{ ds }}（按天增量）；区间型任务显式传 args。

    所有 Spark 任务都经 etl/run_locked.sh 包一层 flock 串行闸门：
    保证**同一时刻只有一个 SparkSession**（不依赖 Airflow 并发配置，
    也拦得住手动跑单表的情况）。详见 etl/run_locked.sh 头部说明。
    """
    script = STEP_SCRIPTS[task_id]
    a = args if args is not None else "--dt {{ ds }}"
    return BashOperator(
        task_id=task_id,
        bash_command=f"{PROJECT_ROOT}/etl/run_locked.sh {VENV_PYTHON} {PROJECT_ROOT}/{script} {a}",
        env=TASK_ENV,
        # ★ 让任务环境 = os.environ + TASK_ENV（TASK_ENV 优先），否则只拿到 TASK_ENV、
        #   丢掉 HOME/SPARK_HOME 等 → 详见 TASK_ENV 上方注释
        append_env=True,
        retries=1,
        retry_delay=timedelta(minutes=2),
        execution_timeout=TIMEOUTS.get(task_id, DEFAULT_TIMEOUT),
        on_failure_callback=alerts.make_callback(failure_event(task_id)),
        on_retry_callback=alerts.make_callback("retry"),
    )


default_args = {
    "owner": "wushi007",
    "depends_on_past": False,
    "start_date": datetime(2019, 10, 1),   # 要能 backfill 2019 年的日期，起点必须早于数据区间
    "retries": 0,
    "retry_delay": timedelta(minutes=2),
    # 兜底：任何没显式设回调的任务（如 check_env）也会告警
    "on_failure_callback": alerts.make_callback("failure"),
    "on_retry_callback": alerts.make_callback("retry"),
    "execution_timeout": DEFAULT_TIMEOUT,
}

with DAG(
    dag_id="incremental_warehouse_dag",
    description="增量数仓：ODS→DWD→DWS→DIM→ADS（一表一 task，由 backfill/手动触发）",
    default_args=default_args,
    # schedule=@daily + end_date=数据末日 —— 为什么不写 schedule=None：
    #   Airflow 3 的 backfill **拒绝** NullTimetable（schedule=None）的 DAG
    #   （`DagNonPeriodicScheduleException`，源码模型里明确列了 NullTimetable），
    #   改成 None 会把回填这条路堵死。
    #   而数据是**历史批次**（2019-10/11），@daily 又会让调度器为"真实的今天"建 run
    #   （catchup=False 也建最近一个周期 = 今天-1 = 2026-xx-xx），ODS 里没有那天数据 → 必失败。
    #   正解：保留周期调度（backfill 兼容），但把 DAG 的时间窗关在数据区间内——
    #   end_date 一过，调度器就不再生成"今天"的 run；回填 2019-11 仍在窗口内，正常可用。
    #   数据换月份时，同步改 DATA_START / end_date。
    schedule="@daily",
    start_date=datetime(2019, 10, 1),
    end_date=datetime(2019, 11, 30),
    catchup=False,
    max_active_runs=1,
    max_active_tasks=1,          # 同一时刻只跑一个 Spark 任务（内存受限）
    tags=["spark", "warehouse", "incremental", "dqc"],
    doc_md=__doc__,
) as dag:

    # 0. 环境守卫：先校验路径配置，再确认 HDFS 就绪
    #    提前失败 + 给出可操作的提示，避免后面报出难懂的 Spark 异常
    check_env = BashOperator(
        task_id="check_env",
        bash_command="""
fail() { echo "ERROR: $1"; echo "提示: $2"; exit 1; }

# 1) 解释器（要有 pyspark）
if [ ! -x "__VENV__" ]; then
    fail "解释器不存在: __VENV__" \
         "设置 DW_VENV_PYTHON 指向你的 venv python，或先创建 venv 并 pip install -r requirements.txt"
fi

# 2) JDK / Spark / Hadoop 目录
for pair in "JAVA_HOME=__JAVA__" "SPARK_HOME=__SPARK__" "HADOOP_HOME=__HADOOP__"; do
    name="${pair%%=*}"; path="${pair#*=}"
    [ -d "$path" ] || fail "$name 目录不存在: $path" "导出 $name 指向实际安装路径（见 docs/GETTING_STARTED.md）"
done

# 3) Hadoop 配置目录
[ -d "__CONF__" ] || fail "HADOOP_CONF_DIR 不存在: __CONF__" "导出 HADOOP_CONF_DIR 指向 Hadoop 的 etc/hadoop 目录"

# 4) HDFS NameNode
if command -v ss >/dev/null 2>&1 && ss -tln 2>/dev/null | grep -q ':8020 '; then
    echo "OK: 路径配置正常，HDFS NameNode 正在监听 8020"
else
    fail "HDFS 未就绪（8020 无监听）" "先启动 HDFS（hdfs namenode -format 后 start-dfs.sh）"
fi
""".replace("__VENV__", VENV_PYTHON)
   .replace("__JAVA__", JAVA_HOME)
   .replace("__SPARK__", SPARK_HOME)
   .replace("__HADOOP__", HADOOP_HOME)
   .replace("__CONF__", HADOOP_CONF_DIR),
        env=TASK_ENV,
        append_env=True,
        retries=0,
    )

    # 区间/快照型任务：不走 --dt，按 [DATA_START, ds] 全区间算。
    # 必须先排除，否则会和下面的重建撞成 DuplicateTaskIdFound（同名 task 加两次 → DAG 加载直接报错）。
    RANGE_TASKS = {"ads_user_retention", "ads_user_rfm"}

    t = {name: build_task(name) for name in STEP_SCRIPTS if name not in RANGE_TASKS}
    t["ads_user_retention"] = build_task(
        "ads_user_retention", args=f"--start {DATA_START} --end {{{{ ds }}}}")
    t["ads_user_rfm"] = build_task(
        "ads_user_rfm", args=f"--start {DATA_START} --end {{{{ ds }}}}")

    # ---------------- 依赖 ----------------
    check_env >> t["ods_import"] >> t["ods_dqc"]

    # DWD：事实表与脏数据表同源同规则，可并行；DQC 必须等两张都写完（要账对平）
    t["ods_dqc"] >> [t["dwd_event_fact"], t["dwd_event_dirty"]]
    [t["dwd_event_fact"], t["dwd_event_dirty"]] >> t["dwd_event_fact_dqc"]

    # DWS 四张（依赖当日 DWD 通过质量校验）
    t["dwd_event_fact_dqc"] >> [
        t["dws_user_session"], t["dws_product"],
        t["dws_user_behavior"], t["dws_traffic_hour"],
    ]

    # DIM 四张（增量，只读当天分区 + 维表快照）
    t["dwd_event_fact_dqc"] >> [
        t["dim_product_scd2"], t["dim_user"], t["dim_session"], t["dim_category"],
    ]

    # ADS 日报五张（各自依赖其来源层）
    t["dws_user_behavior"] >> [t["ads_trade"], t["ads_user_retention"]]
    t["dwd_event_fact_dqc"] >> [t["ads_conversion_funnel"], t["ads_user_rfm"]]
    t["dws_product"] >> t["ads_product_hot_rank"]
    t["dws_user_session"] >> t["ads_session_behavior"]
    t["dws_traffic_hour"] >> t["ads_traffic_hour"]
