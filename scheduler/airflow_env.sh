#!/usr/bin/env bash
# =====================================================================
# airflow_env.sh —— Airflow 环境配置入口
# 生成 Variable warehouse_env 与 AIRFLOW_HOME/airflow-env.sh；--show 查看当前配置。
# 配置经 AIRFLOW__<SECTION>__<KEY> 环境变量注入，不改 airflow.cfg（升级会被重写）。
# 用法：bash scheduler/airflow_env.sh [--show|--sync]
# =====================================================================
set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
# 敏感配置统一来自项目根目录 .env（不入库）
if [ -f "$REPO_ROOT/.env" ]; then
    set -a
    . "$REPO_ROOT/.env"
    set +a
fi
AIRFLOW_HOME="${AIRFLOW_HOME:-$HOME/my-spark/airflow}"
AIRFLOW_VENV="${AIRFLOW_VENV:-$HOME/my-spark/airflow-venv}"

# ---------------- 可覆盖配置（同名环境变量优先）----------------
JAVA_HOME="${JAVA_HOME:-$(ls -d /usr/lib/jvm/java-17-openjdk-* 2>/dev/null | tail -1)}"
# 用 -d 过滤，排除 ~/apps 下的安装包，避免 .tgz / .tar.gz 被选为 HADOOP_HOME。
_pick() { for p in "$@"; do [ -d "$p" ] && echo "${p%/}"; done | tail -1; }
SPARK_HOME="${SPARK_HOME:-$(_pick "$HOME"/apps/spark-*/)}"
HADOOP_HOME="${HADOOP_HOME:-$(_pick "$HOME"/apps/hadoop-[0-9]*/)}"
HADOOP_CONF_DIR="${HADOOP_CONF_DIR:-$HADOOP_HOME/etc/hadoop}"
VENV_PYTHON="${VENV_PYTHON:-$REPO_ROOT/.venv/bin/python}"
DATA_START="${DATA_START:-2019-10-01}"
DATA_END="${DATA_END:-2019-11-30}"
# Spark standalone（本机 master:7077）；切本地模式设 SPARK_MASTER=local[6]
SPARK_MASTER="${SPARK_MASTER:-spark://localhost:7077}"
SPARK_DRIVER_MEMORY="${SPARK_DRIVER_MEMORY:-1g}"
SPARK_EXECUTOR_MEMORY="${SPARK_EXECUTOR_MEMORY:-1g}"
SPARK_EXECUTOR_CORES="${SPARK_EXECUTOR_CORES:-2}"
SPARK_EXECUTOR_INSTANCES="${SPARK_EXECUTOR_INSTANCES:-4}"

AIRFLOW_DB_USER="${AIRFLOW_DB_USER:-airflow}"
AIRFLOW_DB_PASS="${AIRFLOW_DB_PASS:-}"
AIRFLOW_DB_HOST="${AIRFLOW_DB_HOST:-127.0.0.1}"
AIRFLOW_DB_PORT="${AIRFLOW_DB_PORT:-3307}"
AIRFLOW_DB_NAME="${AIRFLOW_DB_NAME:-airflow}"
AIRFLOW_PORT="${AIRFLOW_PORT:-8090}"
AIRFLOW_PARALLELISM="${AIRFLOW_PARALLELISM:-2}"
# ---- 飞书告警机器人（留空 = 不启用；地址只填 DW_ALERT_FEISHU_WEBHOOK）----
# 建议通过环境变量注入，不写入仓库；修改后需重启 Airflow。
DW_ALERT_FEISHU_WEBHOOK="${DW_ALERT_FEISHU_WEBHOOK:-}"
DW_ALERT_FEISHU_SECRET="${DW_ALERT_FEISHU_SECRET:-}"
DW_ALERT_FEISHU_STYLE="${DW_ALERT_FEISHU_STYLE:-card}"

ENV_FILE="$AIRFLOW_HOME/airflow-env.sh"
VARIABLE_JSON="$AIRFLOW_HOME/warehouse_env.json"

usage() {
    sed -n '2,7p' "${BASH_SOURCE[0]}" | sed 's/^# \{0,1\}//'
    exit "${1:-0}"
}

# ---------------- ① DAG 读的 Variable ----------------
# `airflow variables import` 把 JSON 顶层每个 key 当作独立变量名，故须包一层 {"warehouse_env": {...}}。
gen_variable() {
    cat > "$VARIABLE_JSON" <<JSON
{
  "warehouse_env": {
    "project_root":    "$REPO_ROOT",
    "venv_python":     "$VENV_PYTHON",
    "java_home":       "$JAVA_HOME",
    "spark_home":      "$SPARK_HOME",
    "hadoop_home":     "$HADOOP_HOME",
    "hadoop_conf_dir": "$HADOOP_CONF_DIR",
    "data_start":      "$DATA_START",
    "data_end":        "$DATA_END",
    "spark_master":    "$SPARK_MASTER",
    "spark_driver_memory": "$SPARK_DRIVER_MEMORY",
    "spark_executor_memory": "$SPARK_EXECUTOR_MEMORY",
    "spark_executor_cores": "$SPARK_EXECUTOR_CORES",
    "spark_executor_instances": "$SPARK_EXECUTOR_INSTANCES"
  }
}
JSON
    echo "OK: 已生成 Variable JSON: $VARIABLE_JSON"
    if [ -x "$AIRFLOW_VENV/bin/airflow" ]; then
        # DAG 加载时读 Variable，须导入元数据库才生效；-a overwrite 覆盖同名 key。
        if ( set +u; source "$ENV_FILE" 2>/dev/null || true
             "$AIRFLOW_VENV/bin/airflow" variables import "$VARIABLE_JSON" -a overwrite >/dev/null
             "$AIRFLOW_VENV/bin/airflow" variables get warehouse_env >/dev/null 2>&1 ); then
            echo "OK: 已导入 Airflow Variable \"warehouse_env\" 并回读校验通过"
        else
            echo "WARN: Variable \"warehouse_env\" 导入或回读校验失败 —— 检查元数据库连接"
        fi
    else
        echo "WARN: 未找到 $AIRFLOW_VENV/bin/airflow，跳过导入。JSON 已生成，装好 Airflow 后重跑本脚本。"
    fi
}

# ---------------- ② Airflow 三进程读的环境变量 ----------------
gen_env_file() {
    cat > "$ENV_FILE" <<ENV
# 由 scheduler/airflow_env.sh 生成，勿手工编辑；环境变量优先于 airflow.cfg（升级会重写）。

# 敏感配置统一来自项目根目录 .env
if [ -f "$REPO_ROOT/.env" ]; then
    set -a
    . "$REPO_ROOT/.env"
    set +a
fi

export AIRFLOW_HOME=$AIRFLOW_HOME

# ---- 元数据库：MySQL（三进程并发连库，SQLite 文件锁会报 WAL locking protocol）----
export AIRFLOW__DATABASE__SQL_ALCHEMY_CONN="mysql+pymysql://\${AIRFLOW_DB_USER}:\${AIRFLOW_DB_PASS}@\${AIRFLOW_DB_HOST}:\${AIRFLOW_DB_PORT}/\${AIRFLOW_DB_NAME}?charset=utf8mb4"

# ---- Executor：Airflow 3 已移除 SequentialExecutor，用默认 LocalExecutor ----
export AIRFLOW__CORE__EXECUTOR=LocalExecutor
export AIRFLOW__CORE__LOAD_EXAMPLES=False
export AIRFLOW__SCHEDULER__CATCHUP_BY_DEFAULT=False

# ---- WebUI / API 端口（8080 被 Spark Master 占用，Airflow 用 8090）----
export AIRFLOW__API__PORT=$AIRFLOW_PORT
# 只绑定本机，避免暴露到局域网，并规避 SimpleAuthManager 的生产环境告警
export AIRFLOW__API__HOST=127.0.0.1
# LocalExecutor worker 用它拼 execution API 地址；默认 localhost:8080 是 Spark Master 端口，
# 须指向 Airflow 端口，否则 worker 状态上报会打到 Spark 并返回 501。
export AIRFLOW__API__BASE_URL=http://localhost:$AIRFLOW_PORT

# ---- 并发：本机内存 7.6G，默认按核数 fork 32 个 worker 过于浪费 ----
export AIRFLOW__CORE__PARALLELISM=$AIRFLOW_PARALLELISM

# ---- 告警：飞书自定义机器人（alerts.py 的 _feishu() 读取，webhook 为空则跳过）----
# 具体值由顶部 .env 导出，本文件不重复保存。

# .bashrc 的 all_proxy（clash socks5）会被 httpx 使用，venv 无 socksio 时任务直接失败；
# no_proxy 拦不住 all_proxy，只能 unset。
unset all_proxy ALL_PROXY http_proxy https_proxy HTTP_PROXY HTTPS_PROXY
ENV
    echo "OK: 已生成 Airflow 环境文件: $ENV_FILE"
}

show() {
    echo "── 仓库 / 路径 ─────────────────────────────"
    printf '  %-18s %s\n' PROJECT_ROOT "$REPO_ROOT"
    printf '  %-18s %s\n' VENV_PYTHON "$VENV_PYTHON"
    printf '  %-18s %s\n' JAVA_HOME "$JAVA_HOME"
    printf '  %-18s %s\n' SPARK_HOME "$SPARK_HOME"
    printf '  %-18s %s\n' HADOOP_HOME "$HADOOP_HOME"
    printf '  %-18s %s\n' HADOOP_CONF_DIR "$HADOOP_CONF_DIR"
    printf '  %-18s %s\n' DATA_RANGE "$DATA_START ~ $DATA_END"
    echo "── Airflow ─────────────────────────────────"
    printf '  %-18s %s\n' AIRFLOW_HOME "$AIRFLOW_HOME"
    printf '  %-18s %s\n' AIRFLOW_VENV "$AIRFLOW_VENV"
    printf '  %-18s %s\n' WebUI "http://localhost:$AIRFLOW_PORT"
    printf '  %-18s %s\n' DB "$AIRFLOW_DB_HOST:$AIRFLOW_DB_PORT/$AIRFLOW_DB_NAME"
    printf '  %-18s %s\n' PARALLELISM "$AIRFLOW_PARALLELISM"
    if [ -n "$DW_ALERT_FEISHU_WEBHOOK" ]; then
        _sign="否"
        if [ -n "$DW_ALERT_FEISHU_SECRET" ]; then _sign="是"; fi
        printf '  %-18s %s\n' FEISHU "已配置（style=$DW_ALERT_FEISHU_STYLE，签名=$_sign）"
    else
        printf '  %-18s %s\n' FEISHU "未配置（飞书通道自动跳过）"
    fi
    echo "── 存在性检查 ──────────────────────────────"
    for p in "$VENV_PYTHON" "$JAVA_HOME" "$SPARK_HOME" "$HADOOP_HOME" "$HADOOP_CONF_DIR"; do
        [ -e "$p" ] && echo "  $p" || echo "  $p  （不存在）"
    done
    echo "── 已生成的配置文件 ────────────────────────"
    for f in "$ENV_FILE" "$VARIABLE_JSON"; do
        [ -f "$f" ] && echo "  $f" || echo "  - $f  （还没生成）"
    done
    if [ -x "$AIRFLOW_VENV/bin/airflow" ]; then
        echo "── Airflow 记录的 Variable ─────────────────"
        ( set +u; source "$ENV_FILE" 2>/dev/null || true
          if "$AIRFLOW_VENV/bin/airflow" variables get warehouse_env >/dev/null 2>&1; then
              echo "  warehouse_env 已写入元数据库（DAG 加载时会读它）"
          else
              echo "  warehouse_env 未写入 —— 跑一次不带 --show 的本脚本"
          fi )
    fi
}

case "${1:---apply}" in
    --show|-s)   show ;;
    --sync)      gen_env_file; gen_variable ;;
    --apply|-a)  gen_env_file; gen_variable; echo; show ;;
    -h|--help)   usage 0 ;;
    *)           usage 1 ;;
esac
