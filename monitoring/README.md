# ETL 运行台

本机页面：http://localhost:8502 。Airflow 仍在 http://localhost:8090 。

## 启动

从仓库根目录执行：

```bash
bash monitoring/start.sh
# 停止由脚本记录的进程
bash monitoring/stop.sh
```

启动 Streamlit 页面和单例后台采集器。每 30 秒同步一次，页面也每 30 秒刷新，可手动点击“立即同步”。
先安装 `requirements/monitoring.txt`；采集器使用独立 Airflow venv。环境位置可显式配置，见下方变量。
独立数据库默认在 `~/.local/state/ecom-etl-monitor/records.db`，采集日志在同目录 `collector.log`。
采集器只读取 Airflow 元数据和任务日志，不修改调度状态，不触发任务。

## 怎么看

1. 在 DAG 筛选中选 `sql_error_learning_demo`。
2. 搜索 `learn_three_tasks_fail_20261027`。
3. 点击 `transform_orders` 记录的“查看详情”，在右侧查看 SQL 错误详情、脚本位置、SQL 行列和完整异常。
4. 搜索 `learn_three_tasks_success_20261027`，对比三个成功任务。
5. 在“错误范围”中选“仅语法错误”或“包含 SQL 错误”。

每行是一条任务尝试，以 DAG、run_id、task_id、map_index、attempt 唯一标识。
重试单独保留；尚未执行的任务可能显示 attempt=0。
无法从 Airflow 历史表找到状态、但日志仍存在的旧尝试标记为“历史状态未知”，不猜测成功或失败。
任务未执行、日志缺失、非 SQL 原因失败，不会被误标记成 SQL 成功。
时间统一显示为北京时间。

## 提取与数据范围

读取 TaskInstance 和 TaskInstanceHistory，以及本机日志目录。
支持 Airflow JSONL 的文本消息和 error_detail 结构化异常，也支持普通 traceback。
识别 Spark 解析/分析异常及常见 SQL 执行错误码（包括 MERGE_CARDINALITY_VIOLATION），保存完整原始日志以便回查。
按日志大小、修改时间和解析器版本增量解析；相同尝试覆盖更新，不重复插入。
SQL 错误提取是规则识别，并非所有引擎错误码的完整词典；没有匹配不代表没有错误。

正式数仓 DAG 是按层调度，所以每行代表层任务，不会将层内每张表虚构为独立 Airflow 任务。
能从 traceback 取得的具体 ETL 文件会显示在详情中；日志里保留 injector 的脚本开始/结束标记。
历史日志无法恢复已经删除的 SQL，远端日志后端和跨机器 worker 尚不支持。
独立数据库保留已采集的历史，不随源日志清理自动删除。初版没有自动保留期限，需自行管理磁盘。
完整日志可能包含 SQL 常量或业务数据，本服务默认只绑定 127.0.0.1，没有额外用户认证，不应直接暴露到公网。

## 测试

```bash
.venv/bin/python -m unittest discover -s monitoring -p 'test_*.py'
```

可选环境变量：`ETL_MONITOR_DB`（数据库路径）、`ETL_MONITOR_STATE`（采集日志/锁目录）、
`ETL_MONITOR_PORT`（页面端口）、`ETL_AIRFLOW_ENV`（现有 airflow-env.sh）、
`ETL_AIRFLOW_PYTHON`（Airflow Python 路径）。启动采集器前设置，修改后重启进程。
