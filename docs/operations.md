# 运行手册

## 核心演示

```bash
.venv/bin/python scripts/doctor.py
bash scripts/test.sh
bash scripts/demo.sh --verify-rerun
```

小样本输出保存在命令末尾显示的临时目录。先查看 verification.json；不再需要时仅删除这次明确的目录。不要对正式仓库路径执行清空。

## 正式数据环境

使用已有 HDFS、Spark 和 Hive 配置。将 `.env.example` 复制为 `.env` 后设置自己的路径，不需要修改 Python：

```text
DW_FS_URI=hdfs://localhost:8020
DW_WAREHOUSE_URI=hdfs://localhost:8020/warehouse/ecommerce
DW_SOURCE_CSV=hdfs://localhost:8020/data/ecommerce/2019-Nov.csv
```

读取优先级：进程环境变量 > .env > 代码默认值。shell 与 Python 使用同一解析器；空字符串也是显式值。dotenv 支持简单 KEY=VALUE、引号和注释，不执行命令或展开 $变量。通过 DW_ENV_FILE 可选择其它文件，演示固定使用 /dev/null。

保留 run.sh 作为正式入口：

```bash
./run.sh ddl
./run.sh ods-bulk --csv hdfs://localhost:8020/data/ecommerce/2019-Oct.csv
./run.sh build --start 2019-10-01 --end 2019-10-31
./run.sh check --start 2019-10-01 --end 2019-10-31
./run.sh build --dt 2019-11-01
```

单日 build 不包含 ODS 导入，需先运行 ods-import/day。DIM 历史回补用完整区间重建，窗口外版本会被覆盖；RFM 只保留当前快照。

已有本机的 HDFS 路径已保留在本机 .env，不随 Git 分发。仓库外启停脚本属于个人环境，不是新用户的安装前提。系统组件可按自己的安装方式启动：

```bash
# 已有 Hadoop/Spark 安装且已配置目录、SSH 和 NameNode 时
"$HADOOP_HOME/sbin/start-dfs.sh"
"$SPARK_HOME/sbin/start-master.sh"
"$SPARK_HOME/sbin/start-worker.sh" spark://localhost:7077
```

不要重复 format 现有 NameNode。Spark 的 Hive catalog、metastore 连接与 HDFS 配置需在外部 Spark 安装中配置，模板见 [conf](../conf/README.md)。完整安装仍需要你自己的 Java/Hadoop/MySQL 环境；快速验证请使用上面的隔离演示。

## Airflow（可选）

依赖见 [安装说明](../requirements/README.md)。建议显式设置：

```bash
export AIRFLOW_HOME="$HOME/.local/share/ecom-airflow"
export AIRFLOW_VENV="$PWD/.airflow-venv"
# 学习环境先使用 standalone；正式 MySQL 配置另见 scheduler/airflow_env.sh
"$AIRFLOW_VENV/bin/airflow" db migrate
bash scripts/install_dags.sh
"$AIRFLOW_VENV/bin/airflow" standalone
```

使用本机已配置的 MySQL/8090 三进程环境时，先 source 对应 airflow-env.sh，再运行相同安装 DAG 命令。`scheduler/airflow_env.sh` 可生成配置，但会写入 Airflow Variable；先用 --show 检查，确认路径与数据区间再同步。不要拿初始化命令去覆盖现有数据库。

安装故障示例需额外执行 `bash scripts/install_dags.sh --with-examples`。例子默认制造失败，不混入正式业务验收。

## 运行台（可选）

```bash
.venv/bin/python -m pip install -r requirements/monitoring.txt
# 采集器使用的环境文件和解释器可以显式设置
export ETL_AIRFLOW_ENV=/你的/airflow-env.sh
export ETL_AIRFLOW_PYTHON=/你的/airflow-venv/bin/python
bash monitoring/start.sh
# 另一个终端停止
bash monitoring/stop.sh
```

页面默认 8502，只绑定本机。stop 只停止启动脚本记录且命令仍匹配的进程。首次迁移前已经运行、没有 PID 记录的旧进程需由原终端停止。

完整日志可能包含 SQL 常量和业务字段，不应直接暴露到公网；独立 SQLite 没有自动历史清理，需定期查看磁盘占用。
