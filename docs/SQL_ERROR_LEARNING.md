# Airflow 三任务 ETL 学习示例

打开 http://localhost:8090 ，进入 `sql_error_learning_demo`。

流程：`extract_orders → transform_orders → load_orders`。
三个任务分别启动本地 Spark 进程，通过 XCom 传递 Parquet 的 `file:///tmp/...` 路径。
Extract 写入三条模拟订单，Transform 按客户汇总，Load 展示结果，不写业务表。

| 任务 | retries（额外重试次数） | execution_timeout |
|---|---:|---:|
| extract_orders | 1 | 3 分钟 |
| transform_orders | 0 | 3 分钟 |
| load_orders | 1 | 2 分钟 |

Extract/Load 重试等待 10 秒。语法错误不会通过重试恢复，所以 Transform 不重试。
每个任务继承仓库串行锁，最多等锁 60 秒，该时间也计入任务超时。

## 查看已执行的例子

- `learn_three_tasks_fail_20261027`：Extract 成功，Transform 失败，Load 为 upstream_failed（未执行）。
- `learn_three_tasks_success_20261027`：三个任务全部成功。

点击运行记录，再点具体任务的 Logs。
失败运行的 Transform 日志中搜索 `PARSE_SYNTAX_ERROR`，可看到 SQL 第 3 行、pos 8 和箭头定位。
末尾的 CalledProcessError 只是子进程失败的包装，向上查找真正的 SQL 异常。
成功运行的 Load 日志中可看到 alice=150、bob=80。

在 Extract 或 Transform 的 XCom 页面查看 `return_value`，它只是路径字符串。
每个 run_id 对应独立目录，同一次运行重试覆盖自己的输出。

## 自己触发

默认配置 `{}` 会制造错误。
触发时配置 `{"fix_sql": true}` 则执行正确 SQL。
DAG 没有定时调度，retries 不会修正 SQL；如需修改模式，创建新运行。

## 文件与限制

- DAG：`examples/sql_errors/dag.py`
- Spark 阶段脚本：`examples/sql_errors/orders_etl.py`

此例适用于当前单机 LocalExecutor。多机器执行应改用所有 worker 可访问的 HDFS 或对象存储。
中间数据保留在 `/tmp/airflow-orders-learning/` 供学习查看；清理时须确保相关运行已结束，清理后重跑下游任务需先重新执行上游。
