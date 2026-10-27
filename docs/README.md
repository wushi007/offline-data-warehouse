# 文档入口

当前使用路径：

1. [从小样本开始](GETTING_STARTED.md)：安装、测试、真实链路演示。
2. [架构与边界](architecture.md)：手动/调度运行方式、为什么按层串行。
3. [数据模型](data-model.md)：表粒度、指标口径、拉链与留存限制。
4. [运行手册](operations.md)：正式 HDFS 环境、Airflow、任务诊断。
5. [验证记录](validation/README.md)：本轮样本验收与历史全量结果的区别。
6. [开发与发布](WORKFLOW.md)：分支、检查与 GitHub 发布。

补充材料：[详细代码解析](CODE_GUIDE.md)、[告警](incremental/ALERTS.md)、[SQL 故障示例](SQL_ERROR_LEARNING.md)。

历史建设过程保留在 incremental/；早期表说明与性能记录保留在 legacy/。其中的数据范围和命令可能反映当时状态，不作为当前启动入口。最新表结构以 etl/ddl.py 为准。
