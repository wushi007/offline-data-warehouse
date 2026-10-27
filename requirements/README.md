# 依赖与环境

当前版本：Python 3.10、JDK 17、pip PySpark 3.5.8；Airflow 3.3.0 独立运行。
已有全量环境使用外部 Spark 3.5.9 + Python PySpark 3.5.8，保留现有配置。

```bash
python3.10 -m venv .venv
.venv/bin/python -m pip install -r requirements.txt
# 运行台（可选）
.venv/bin/python -m pip install -r requirements/monitoring.txt
# StarRocks 导入与查询（可选）
.venv/bin/python -m pip install -r requirements/starrocks.txt
```

Airflow 安装到单独环境，不能把它的整套依赖混入核心环境：

```bash
python3.10 -m venv .airflow-venv
.airflow-venv/bin/python -m pip install -r requirements/airflow.txt \
  --constraint https://raw.githubusercontent.com/apache/airflow/constraints-3.3.0/constraints-3.10.txt
```

Airflow 的 PyMySQL 使用官方 constraints 指定版本（当前为 1.1.3），不与项目可选 StarRocks 环境的版本混用。不要使用 --no-deps 绕过冲突。
核心和可选组件固定直接依赖版本，传递依赖由包/官方 constraints 解析；这不是全平台锁文件。
不要将共享开发环境的 pip freeze 覆盖到项目清单。
Java/HDFS/MySQL 属于系统组件，不放入 pip requirements。
