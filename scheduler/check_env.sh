#!/usr/bin/env bash
# =====================================================================
# check_env.sh —— DAG check_env 任务的环境守卫。
# DAG 加载时将 __VENV__ / __JAVA__ / __SPARK__ / __HADOOP__ / __CONF__ /
# __DAGS__ / __AIRFLOW_HOME__ / __OWNER_ROOT__ 替换为实际配置后执行。
# 失败即提前退出并给出修复提示；典型根因是 SPARK_HOME 缺失导致 TABLE_OR_VIEW_NOT_FOUND。
# =====================================================================
set -uo pipefail

fail() { echo "ERROR: $1"; echo "提示: $2"; exit 1; }

echo "本次解析到的配置："
echo "  PROJECT_ROOT   = __OWNER_ROOT__"
echo "  VENV_PYTHON    = __VENV__"
echo "  JAVA_HOME      = __JAVA__"
echo "  SPARK_HOME     = __SPARK__"
echo "  HADOOP_HOME    = __HADOOP__"
echo "  HADOOP_CONF_DIR= __CONF__"
echo "  AIRFLOW_HOME   = __AIRFLOW_HOME__"
echo "  dags_folder    = __DAGS__"
echo

if [ ! -x "__VENV__" ]; then
    fail "解释器不存在: __VENV__" \
         "在 Airflow Variable warehouse_env 里设置 venv_python，或先创建 venv 并 pip install -r requirements.txt"
fi

for pair in "JAVA_HOME=__JAVA__" "SPARK_HOME=__SPARK__" "HADOOP_HOME=__HADOOP__"; do
    name="${pair%%=*}"; path="${pair#*=}"
    [ -d "$path" ] || fail "$name 目录不存在: $path" \
        "在 Airflow Variable warehouse_env 里设置 ${name,,}（见 docs/GETTING_STARTED.md §九）"
done

[ -d "__CONF__" ] || fail "HADOOP_CONF_DIR 不存在: __CONF__" \
    "在 Airflow Variable warehouse_env 里设置 hadoop_conf_dir"

if command -v ss >/dev/null 2>&1 && ss -tln 2>/dev/null | grep -q ':8020 '; then
    echo "OK: 路径配置正常，HDFS NameNode 正在监听 8020"
else
    fail "HDFS 未就绪（8020 无监听）" "先启动 HDFS（hdfs namenode -format 后 start-dfs.sh）"
fi
