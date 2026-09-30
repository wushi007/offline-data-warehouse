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
# 调度方式：schedule=None（不设定时），由 backfill / 手动触发驱动 ——
#           因为数据是历史批次（2019-10/11），@daily 会去跑"真实的今天"而必然失败。
#           回填示例：
#             airflow backfill create --dag-id incremental_warehouse_dag \
#                 --from-date 2019-11-01 --to-date 2019-11-30 --max-active-runs 1
#
# ⚠️ 内存约束：同一时刻只允许一个 SparkSession（本机 7.6G，StarRocks/HDFS/Kafka
#    常驻约 2.5G，多会话并存被内核 OOM killer 杀过多次）。两道保障：
#      1) max_active_tasks=1 + max_active_runs=1 —— 本 DAG 内串行；
#      2) 所有 Spark 任务经 etl/run_locked.sh 的 flock 闸门 —— 跨 DAG、跨手动执行也串行
#         （机器上还有另一个项目的 DAG 会起 Spark，DAG 级配置管不住它）。
#
# 运行前提：./start-env.sh 拉起 HDFS 与 Airflow（check_env 守卫）。
# =====================================================================

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

# ---------------- 路径常量（按你的环境改这三行） ----------------
PROJECT_ROOT = "/home/lst/my-spark/my-second-project-add"
VENV_PYTHON = f"{PROJECT_ROOT}/.venv/bin/python"   # 含 pyspark
JAVA_HOME = "/usr/lib/jvm/java-17-openjdk-amd64"
SPARK_HOME = "/home/lst/apps/spark-3.5.9-bin-hadoop3"
HADOOP_HOME = "/home/lst/apps/hadoop-3.4.3"
HADOOP_CONF_DIR = f"{HADOOP_HOME}/etc/hadoop"
DATA_START = "2019-10-01"        # 数仓数据起点（RFM/留存这类区间任务要用）

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

    # 0. 环境守卫
    check_env = BashOperator(
        task_id="check_env",
        bash_command="""
if command -v ss >/dev/null 2>&1 && ss -tln 2>/dev/null | grep -q ':8020 '; then
    echo "OK: HDFS NameNode 正在监听 8020"
else
    echo "ERROR: HDFS 未就绪，请先运行 ./start-env.sh"
    exit 1
fi
""",
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
