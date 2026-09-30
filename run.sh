#!/usr/bin/env bash
# ============================================================
# 电商离线数仓 · 便捷启动脚本
#
# 代码组织：etl/<层>/<表>.py —— 每一层一个文件夹、每一张表一个文件，
#           每张表都能单独跑（一个进程一个 SparkSession，一张表一张表跑）。
# ============================================================
set -euo pipefail
cd "$(dirname "$0")"

# JDK 17：环境变量优先；未设置时用 Debian/Ubuntu 的默认位置
if [ -z "${JAVA_HOME:-}" ]; then
    for _jh in /usr/lib/jvm/java-17-openjdk-amd64 /usr/lib/jvm/java-17-openjdk /usr/lib/jvm/default-java; do
        [ -d "$_jh" ] && { JAVA_HOME="$_jh"; break; }
    done
fi
if [ -n "${JAVA_HOME:-}" ]; then
    export JAVA_HOME
    export PATH="$JAVA_HOME/bin:$PATH"
fi

# Spark / Hadoop 目录：决定 Hive metastore 与 catalogImplementation 能否生效
# ★ 为什么必须解析（不是可有可无）：
#   pyspark 找不到 SPARK_HOME 时会回退到 venv 里的 pyspark 包目录，那里没有
#   conf/spark-defaults.conf —— 而 spark.sql.catalogImplementation=hive 与
#   metastore 的 JDBC 连接都写在那，后果是建表直接报
#     NOT_SUPPORTED_COMMAND_WITHOUT_HIVE_SUPPORT
#   交互式终端里 .bashrc 已经导出这些变量，所以手敲看不出来；
#   但非交互场景（脚本 / cron / CI / 未显式传 env 的调度器）会静默丢 Hive 支持
#   —— .bashrc 开头那句 "If not running interactively, don't do anything" 就是原因。
#   探测顺序与 scheduler/incremental_warehouse_dag.py 保持一致。
resolve_dir() {
    # $1 = 当前值（环境变量，可能为空或失效）；其余参数 = glob 模式，按顺序探测
    local cur="$1"; shift
    if [ -n "$cur" ] && [ -d "$cur" ]; then printf '%s' "$cur"; return 0; fi
    local pat matches d best
    for pat in "$@"; do
        matches="$(compgen -G "$pat" 2>/dev/null || true)"
        best=""
        for d in $matches; do
            [ -d "$d" ] || continue      # 排除 spark-*.tgz 之类的安装包
            if [ -z "$best" ] || [[ "$d" > "$best" ]]; then best="$d"; fi
        done
        if [ -n "$best" ]; then printf '%s' "$best"; return 0; fi
    done
    return 1
}

SPARK_HOME="$(resolve_dir "${SPARK_HOME:-}" "${HOME}/apps/spark-*" "/opt/spark*" "/usr/local/spark*" || true)"
HADOOP_HOME="$(resolve_dir "${HADOOP_HOME:-}" "${HOME}/apps/hadoop-[0-9]*" "/opt/hadoop*" "/usr/local/hadoop*" || true)"
[ -n "$SPARK_HOME" ]  && { export SPARK_HOME;  PATH="$SPARK_HOME/bin:$PATH"; }
[ -n "$HADOOP_HOME" ] && { export HADOOP_HOME; PATH="$HADOOP_HOME/bin:$HADOOP_HOME/sbin:$PATH"; }
HADOOP_CONF_DIR="${HADOOP_CONF_DIR:-${HADOOP_HOME:+$HADOOP_HOME/etc/hadoop}}"
[ -n "$HADOOP_CONF_DIR" ] && export HADOOP_CONF_DIR
export PATH

if [ -z "${SPARK_HOME:-}" ]; then
    cat >&2 <<'WARN'
⚠️  未找到 Spark 安装目录（SPARK_HOME 未设置，且探测 ~/apps、/opt、/usr/local 均失败）
    后果：pyspark 会回退到 venv 内的包目录，读不到 conf/spark-defaults.conf，
          Hive 支持与 metastore 连接会失效 → 建表报
          NOT_SUPPORTED_COMMAND_WITHOUT_HIVE_SUPPORT
    解决：export SPARK_HOME=/你的/spark/安装路径
WARN
fi

# 解释器解析顺序：PYTHON 环境变量 > 仓库内 .venv > 同级 .venv > 家目录 .venv > 系统 python3
# 可用 PYTHON=/path/to/python ./run.sh ... 显式指定
# 若 venv 在仓库外，推荐在仓库根建软链接：ln -s /path/to/venv .venv
resolve_python() {
    if [ -n "${PYTHON:-}" ]; then echo "$PYTHON"; return; fi
    local c
    for c in "$PWD/.venv/bin/python" \
             "$PWD/../.venv/bin/python" \
             "$HOME/.venv/bin/python"; do
        [ -x "$c" ] && { echo "$c"; return; }
    done
    echo "python3"
}
PY="$(resolve_python)"

# 所有 Spark 调用都过串行闸门：同一时刻只有一个 SparkSession（抢不到锁会排队等待）
LOCKED=./etl/run_locked.sh

run() { $LOCKED "$PY" "$@"; }

# ---------------------------------------------------------------
# 纯 SQL 通道：./run.sh sql <文件> [--dt D]
#
# 为什么需要这一层：spark-defaults.conf 里**只有** master / driver.memory /
# Hive metastore，而下面这几项会话配置原本只写在 etl/utils.py 的 get_spark() 里
# —— 那是 Python 通道的东西，spark-sql 一无所知。缺了它们，同一条 SQL 在两条
# 通道上会算出不同结果（详见 sql/README.md 的「配置缺口」）：
#   · session.timeZone=UTC            不设 → 退回系统时区(+8)，
#                                     TO_DATE(event_time) 会把每天 16:00-23:59
#                                     UTC 的事件算到次日（实测 2019-10-01 分区
#                                     多出 337,080 行「错位」）
#   · partitionOverwriteMode=dynamic  不设 = STATIC，增量 INSERT OVERWRITE
#                                     （不带 PARTITION 子句那种）会覆盖整张表
#   · fs.defaultFS                    不设 → parquet 路径按本地文件系统解析
# ★ 改动 etl/utils.py 的 get_spark() 时必须同步这里，两个通道要保持一致。
# ---------------------------------------------------------------
HDFS_ROOT="$(sed -n 's/^HDFS_ROOT *= *"\(.*\)"/\1/p' config/config.py 2>/dev/null | head -1)"
[ -n "$HDFS_ROOT" ] || HDFS_ROOT="hdfs://localhost:8020"

SPARK_SQL_BIN=""    # 实际路径在 sql) 分支里解析，见那里对「裸 spark-sql」的警告

SQL_CONF=(
    --conf "spark.sql.session.timeZone=UTC"
    --conf "spark.sql.sources.partitionOverwriteMode=dynamic"
    --conf "spark.hadoop.fs.defaultFS=$HDFS_ROOT"
    --conf "spark.master=local[6]"
    --conf "spark.driver.memory=4g"
    --conf "spark.sql.shuffle.partitions=100"
)

# SQL 同样过串行闸门 —— spark-sql 起的也是完整 SparkSession，一样吃内存
run_sql() { $LOCKED "$SPARK_SQL_BIN" "${SQL_CONF[@]}" "$@"; }

usage() {
    cat <<'EOF'
用法: ./run.sh <命令> [参数]

建表 / 调度
  ddl                         建全链路库表（5 库 19 张）+ MSCK 注册 ODS 分区
  ddl --drop-tables dim.dim_user   只重建指定表（改 schema 时用，自动 MSCK）

ODS 接入
  ods-import --dt D           ODS 单日导入（幂等：分区已存在则跳过）
  ods-bulk --csv <hdfs路径>    ODS 批量导入（单次扫描整份 CSV，切出全部日期分区）
  day <YYYY-MM-DD>            跑某天的 ODS 链路: 导入 → DQC
  backfill --start D --end D  批量回补 ODS（逐日）

整条链路（一个进程、一个 SparkSession）
  build --start D --end D     全链路：DWD→DWS→DIM→ADS + 跨层对账
  build --dt D                单日增量（DIM 走增量）
  build --only dwd,dws        只跑指定层
  check --start D --end D     只做跨层对账，不重新构建

单张表（一张表一个进程，便于单表重试/定位问题）
  ods    --dt D               etl/ods/ods_event_log.py
  dwd    --dt D               etl/dwd/dwd_event_fact.py（dwd-dirty / dwd-dqc 同理）
  dws    --dt D               etl/dws/dws_user_behavior_daily.py
  dim    --dt D               etl/dim/dim_user.py（增量）| --start/--end（全量重建）
  ads    --dt D               etl/ads/ads_trade_daily.py
  table <相对路径> [参数]      跑任意单表，如: ./run.sh table etl/dim/dim_category.py --dt 2019-11-01

纯 SQL（直接跑 .sql 文件，不经过 Python）
  sql <sql文件> --dt D        跑 sql/ 下的 SQL 文件，如:
                                ./run.sh sql sql/dqc/dwd_event_fact_dqc.sql --dt 2019-10-01
                              自动把 --dt 转成 --hivevar dt=...，并带上必需的会话配置
EOF
}

case "${1:-}" in
    ddl)        run etl/ddl.py "${@:2}" ;;
    backfill)   run etl/backfill.py "${@:2}" ;;
    ods-import) run etl/ods/ods_event_log.py "${@:2}" ;;
    ods-bulk)   run etl/ods/ods_event_log_bulk.py "${@:2}" ;;
    day)
        D="${2:?用法: ./run.sh day YYYY-MM-DD}"
        run etl/ods/ods_event_log.py --dt "$D"
        run etl/ods/ods_event_log_dqc.py --dt "$D"
        ;;
    build)      run etl/pipeline.py "${@:2}" ;;
    check)      run etl/pipeline.py "${@:2}" --only "" --check-only ;;

    # ---- 单张表（默认给常用那张；其余用 `table` 通道） ----
    ods)   run etl/ods/ods_event_log.py "${@:2}" ;;
    dwd)   run etl/dwd/dwd_event_fact.py "${@:2}" ;;
    dwd-dirty) run etl/dwd/dwd_event_dirty.py "${@:2}" ;;
    dwd-dqc)   run etl/dwd/dwd_event_fact_dqc.py "${@:2}" ;;
    dws)   run etl/dws/dws_user_behavior_daily.py "${@:2}" ;;
    dim)   run etl/dim/dim_user.py "${@:2}" ;;
    dim-scd2) run etl/dim/dim_product_scd2.py "${@:2}" ;;
    ads)   run etl/ads/ads_trade_daily.py "${@:2}" ;;
    ads-retention) run etl/ads/ads_user_retention_daily.py "${@:2}" ;;
    ads-rfm)       run etl/ads/ads_user_rfm_snapshot.py "${@:2}" ;;

    table)
        shift
        S="${1:?用法: ./run.sh table etl/<层>/<表>.py [参数]}"
        shift || true
        run "$S" "$@"
        ;;

    # ---- 纯 SQL 通道 ----
    # --dt D 会被转成 --hivevar dt=D（SQL 里用 ${dt} 引用）；
    # ods_path 由这里从 config/config.py 读出来注入，避免同一个路径
    # 在 config.py 和 .sql 文件里各写一份；
    # 其余参数原样透传给 spark-sql（如额外的 --hivevar、--conf）。
    sql)
        shift
        S="${1:?用法: ./run.sh sql <sql文件> [--dt YYYY-MM-DD] [其它 spark-sql 参数]}"
        shift || true
        _DT=""
        _EXTRA=()
        while [ "$#" -gt 0 ]; do
            case "$1" in
                --dt) _DT="${2:?--dt 后面要跟日期，如 --dt 2019-10-01}"; shift 2 ;;
                *)    _EXTRA+=("$1"); shift ;;
            esac
        done

        # ODS 分区根路径：从 config.py 读，保证与 Python 通道同源。
        # 放在这里而不是文件开头，是为了不拖慢其它子命令。
        _ODS="$("$PY" -c 'import sys; sys.path.insert(0, "."); from config.config import ODS_PATH; print(ODS_PATH)' 2>/dev/null || true)"
        if [ -z "$_ODS" ]; then
            echo "⚠️  读不到 config/config.py 里的 ODS_PATH（$PY 不可用或缺 pyspark）" >&2
            echo "     用 PYTHON=<项目 venv 下的 python> 指定解释器后重试" >&2
        fi

        # spark-sql 必须显式用 $SPARK_HOME 下的那一份。
        # ★ 不要退回裸 `spark-sql`：非交互 shell 里 SPARK_HOME 会被 .bashrc 的
        #   "not running interactively" 守卫拦掉，裸命令会顺着 PATH 落到
        #   ~/.local/bin/spark-sql（pip 装 pyspark 时带进来的 3.5.8），
        #   那份没有 conf/spark-defaults.conf → 没有 Hive metastore →
        #   报 TABLE_OR_VIEW_NOT_FOUND，看着像「表没建」，其实是找错了 Spark。
        SPARK_SQL_BIN="${SPARK_HOME:+$SPARK_HOME/bin/spark-sql}"
        if [ ! -x "${SPARK_SQL_BIN:-}" ]; then
            {
                echo "❌ 找不到 spark-sql：SPARK_HOME=${SPARK_HOME:-（未设置）}，其下没有 bin/spark-sql。"
                echo "   刻意不退回裸 spark-sql —— 那会落到 /usr/local/bin 的另一份 Spark，"
                echo "   它没有 Hive metastore 配置，查询会报 TABLE_OR_VIEW_NOT_FOUND。"
                echo "   解决办法：export SPARK_HOME=/你的/spark 安装路径"
            } >&2
            exit 1
        fi

        _HV=(--hivevar "ods_path=$_ODS")
        if [ -n "$_DT" ]; then _HV+=(--hivevar "dt=$_DT"); fi
        run_sql "${_HV[@]}" "${_EXTRA[@]}" -f "$S"
        ;;
    *) usage ;;
esac
