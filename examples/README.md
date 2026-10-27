# 两个运行样例

## 1. 小数据跑五层 ETL

```bash
.venv/bin/python -m pip install -r requirements.txt
bash examples/warehouse/run.sh
# 需要检查重复运行时再加此参数
bash examples/warehouse/run.sh --verify-rerun
```

需要 Python 3.10、JDK17。输入是 [warehouse/events.csv](warehouse/events.csv)，8 天共 37 条合成事件，复用正式 DDL 和 ETL。
不需要外部 HDFS、MySQL 或 Airflow；每次使用新的本地目录和 Hive metastore。
结束时显示输出目录与结果，已有运行结果见 [warehouse/result.json](warehouse/result.json)。

结果应为 32 条事实、3 条脏数据、2 条去重记录，购买 12 次、GMV 120。
商品 1001 在 10-05 从 alpha 变为 beta；留存窗口不足时为 NULL。
目录不会自动删除，查看结果后可删除本次明确的临时目录，不要清理正式数仓目录。

## 2. 在 Airflow 中看 SQL 失败

先配置独立 Airflow 环境，见 [依赖说明](../requirements/README.md)。在仓库根目录执行：

```bash
# AIRFLOW_HOME 指向你已经配置好的 Airflow 目录
ln -s "$PWD/examples/sql_errors/dag.py" "$AIRFLOW_HOME/dags/sql_error_learning_dag.py"
```

如果链接已经存在，先确认它指向当前文件，不要覆盖不相关的 DAG。
打开 Airflow，启用 `sql_error_learning_demo` 并手动触发。

- 默认 `{}`：Extract 成功，Transform 因 GROUP BY 后缺少字段失败，Load 不执行。
- 配置 `{"fix_sql": true}`：三个任务成功，Load 日志显示 alice=150、bob=80。

任务没有定时调度，不操作业务表。每次运行的中间数据位于独立的 `/tmp/airflow-orders-learning/` 子目录，XCom 只传路径。
失败日志在 Transform 的 Logs；在运行台中搜索相同运行批次可以查看提取后的 SQL 错误。

这是为了说明任务状态和错误定位；正式数仓仍是一层一个 Airflow task，不能把这个三任务例子当作正式链路的调度粒度。
