# 示例

正式数仓 DAG 位于 scheduler/。此目录只放可选演示，不会自动安装到 Airflow。

## SQL 失败定位

`sql_errors/dag.py` 包含 extract → transform → load 三个任务；默认在转换时产生语法错误。
通过配置 `{"fix_sql": true}` 触发可得到成功运行。数据是三行内存订单，不读取业务表。

```bash
# 先设置 AIRFLOW_HOME，并安装独立 Airflow 环境
bash scripts/install_dags.sh --with-examples
```

文件：`sql_errors/orders_etl.py`。本地直接运行指定阶段也需要传入输入/输出路径；
详细步骤见 [学习说明](../docs/SQL_ERROR_LEARNING.md)。
