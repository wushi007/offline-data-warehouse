#!/usr/bin/env bash
# 电商离线数仓启动脚本。
# 代码组织：etl/<层>/<表>.py，每张表均可单独运行。
set -euo pipefail
cd "$(dirname "$0")"

# 敏感配置统一来自项目根目录 .env
source ./scripts/env.sh

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

# Spark/Hadoop 目录决定 Hive 支持：未设置 SPARK_HOME 时 pyspark 会回退到 venv
# 包目录，读不到 conf/spark-defaults.conf，建表报 NOT_SUPPORTED_COMMAND_WITHOUT_HIVE_SUPPORT。
# 非交互场景（脚本/cron/调度器）不会加载 .bashrc，故需在此探测。
resolve_dir() {
    # $1 为当前值，其余参数为 glob 模式，按顺序探测
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
WARN: 未找到 Spark 安装目录（SPARK_HOME 未设置，且探测 ~/apps、/opt、/usr/local 均失败）
    后果：pyspark 会回退到 venv 内的包目录，读不到 conf/spark-defaults.conf，
          Hive 支持与 metastore 连接会失效 → 建表报
          NOT_SUPPORTED_COMMAND_WITHOUT_HIVE_SUPPORT
    解决：export SPARK_HOME=/你的/spark/安装路径
WARN
fi

# 解释器顺序：PYTHON 环境变量 > 仓库内 .venv > 同级 .venv > ~/.venv > python3
# venv 在仓库外时，可在仓库根建软链接：ln -s /path/to/venv .venv
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

# 所有 Spark 调用经串行闸门，同一时刻只有一个 SparkSession
LOCKED=./etl/run_locked.sh

run() { $LOCKED "$PY" "$@"; }

usage() {
    cat <<'EOF'
用法: ./run.sh <命令> [参数]

本地验证
  demo [--verify-rerun]        隔离样本跑真实五层 ETL，不需要 HDFS/Airflow
  doctor                       检查核心版本和 Java

建表 / 调度
  ddl                         建全链路库表（5 库 19 张）+ MSCK 注册全部分区表（RFM 快照除外）
  ddl --drop-tables dim.dim_user   只重建指定表（改 schema 时用，自动 MSCK）

ODS 接入
  ods-import --dt D           ODS 单日导入（幂等：分区已存在则跳过）
  ods-bulk --csv <hdfs路径>    ODS 批量导入（单次扫描整份 CSV，切出全部日期分区）
  day <YYYY-MM-DD>            跑某天的 ODS 链路: 导入 → DQC
  backfill --start D --end D  批量回补 ODS（逐日）

整条链路（一个进程、一个 SparkSession）
  build --start D --end D     区间全链路：DWD→DWS→DIM(全量重算)→ADS(含留存/RFM) + 跨层对账
  build --dt D                单日增量（DIM 走增量；留存/RFM 按 DATA_START..D 重建）
  build --only dwd,dws        只跑指定层
  check --start D --end D     只做跨层对账，不重新构建

按层注入（一个进程、一个 Context、一个 Session；层内逐表串行）
  inject --plan etl/plans/dwd.txt --dt D   注入一整层（清单见 etl/plans/）
  inject --plan ... --dt D --fail-fast     任一张失败即停（DAG 用这个）
  inject 'etl/dws/dws_product_daily.py --dt D'   注入单条
  inject --plan ... --dt D --dry-run       只解析清单，不连集群

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
    demo)       exec bash ./scripts/demo.sh "${@:2}" ;;
    doctor)     exec "$PY" scripts/doctor.py ;;
    ddl)        run etl/ddl.py "${@:2}" ;;
    backfill)   run etl/backfill.py "${@:2}" ;;
    ods-import) run etl/ods/ods_event_log.py "${@:2}" ;;
    ods-bulk)   run etl/ods/ods_event_log_bulk.py "${@:2}" ;;
    day)
        D="${2:?用法: ./run.sh day YYYY-MM-DD}"
        run etl/ods/ods_event_log.py --dt "$D"
        run etl/ods/ods_event_log_dqc.py --dt "$D"
        ;;
    inject)     run etl/injector.py "${@:2}" ;;
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
    *) usage >&2; exit 2 ;;
esac
