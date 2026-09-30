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
    *) usage ;;
esac
