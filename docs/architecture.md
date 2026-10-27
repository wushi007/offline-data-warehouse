# 架构与执行边界

核心链路是 CSV → ODS → DWD → DIM/DWS → ADS。表结构统一在 `etl/ddl.py`，业务参数在 `config/config.py`，Spark 工厂在 `etl/utils.py`。

## 两种运行方式

- 手动：`run.sh` → `etl/run_locked.sh` → 单表脚本或 `etl/pipeline.py`。
- 调度：Airflow → 层任务 → 串行锁 → injector → 层内多个表脚本。

两者共用业务函数。正式 DAG 以层为重试边界；失败会保留已完成表的输出，重跑依赖覆盖写的幂等性，不具备整层事务回滚。

injector 为兼容现有 CLI，运行时接管 get_spark/stop，避免重复建立会话。这是有限资源下的实现折衷，不是通用执行框架。暂不重写；改动它前先扩展真实会话回归。

## 数据落地

正式模式使用 HDFS + Hive 外部表。日期分区覆盖只影响目标分区；DIM 部分增量通过临时 Parquet 快照绕过同表自读写限制。RFM 明确只保留一个当前快照。

小样本模式使用独立本地文件和嵌入式 Hive metastore。目录由 mktemp 创建，数据库前缀为 demo_，不会复用正式表。它复用同一份 DDL 和转换代码，不依赖本机外部 Spark 配置。

## 调度和诊断

正式安装只链接 scheduler 的主 DAG；示例需显式安装。日志采集器读取 Airflow 元数据与本机日志，写独立 SQLite，Streamlit 页面只做查看，不改变调度状态。

监控对 Airflow 3.3 内部模型有版本耦合；当前本机方案优先简单可维护。以后确有远端 worker 或跨版本需求，再引入 REST API/远端日志适配。

## 运行约束

现有笔记本环境使用单层并发和整机锁，减少 Spark JVM 并存与临时目录竞争。不要只提高 DAG 并发而忽略共享 staging 路径。

小样本独立目录允许隔离验证；正式脚本不自动删除现有生产目录。所有全量重建和 RFM 覆盖均需先确认数据范围。
