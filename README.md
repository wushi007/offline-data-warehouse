# 电商离线数仓

用公开电商行为数据实现一条离线数仓链路：ODS 接入，DWD 清洗去重，DIM 维度维护，DWS 日汇总，ADS 交易、留存和 RFM 指标。Spark 负责计算，Hive 外部表管理数据，Airflow 负责调度。

项目围绕两个问题展开：数据重复、缺失或商品属性变化时，指标能否保持一致；任务失败时，能否定位到具体脚本和 SQL。

## 先运行小样本

需要 Linux / WSL、Python 3.10、JDK 17。不需要下载完整 Kaggle 数据，也不需要启动 HDFS、MySQL 或 Airflow。

```bash
python3.10 -m venv .venv
.venv/bin/python -m pip install -r requirements.txt
bash scripts/test.sh
bash scripts/demo.sh --verify-rerun
```

演示使用 pip PySpark 3.5.8，自建隔离的本地 Hive metastore 和 19 张表，复用正式 DDL、ETL 和对账代码。每次输出位于新的 `/tmp/ecom-warehouse-demo.*` 目录，不访问已有数仓。

固定样本包含 8 天、37 条事件：

| 检查 | 预期结果 |
|---|---|
| 清洗与分流 | 32 条事实、3 条脏数据、2 条去重记录 |
| 购买和 GMV | 12 次购买、GMV 120，各层一致 |
| 商品属性变更 | alpha → beta，两个历史版本、一个当前版本 |
| 留存右删失 | 7 个基准日的 D+7 为 NULL |
| RFM | 两个购买用户，只保留当前快照 |
| 完整重跑 | 19 张表的逐值摘要一致 |
| 增量与回补 | 同日重跑不多开版本，历史增量被拒绝 |

结束时打印检查结果，并生成 `verification.json`。样本验证业务边界，不用于性能测量。

## 实现与取舍

```text
CSV → ODS → DWD ─┬→ DIM 商品拉链、用户、会话、品类、日期
                └→ DWS 用户、商品、会话、小时汇总
                       ↓
                   ADS 指标
```

- **确定性去重**：同一去重键追加稳定排序，重跑不随机选择属性。
- **脏数据留痕**：清洗和隔离表共用判定规则，保留原因并核对数量。
- **商品拉链**：源数据没有商品主数据，用每日主导属性估计版本；保留这种推断的业务限制。
- **质量阻断**：DWD 校验失败停止下游；购买、GMV、流量和留存通过跨层不变量检查。
- **有限资源运行**：正式 Airflow 按层启动共享 SparkSession，层内串行；手动与调度统一经过 flock。恢复粒度是层，而不是每张表独立重试。

具体表粒度、恢复方式见 [数据模型](docs/data-model.md) 和 [架构](docs/architecture.md)。

## Airflow 与任务诊断（可选）

Airflow 使用独立 Python 环境。正式 DAG 是 `incremental_warehouse_dag`，安装方法见 [运行手册](docs/operations.md)。

运行台同步任务状态、各次重试和本地日志，提取 SQL 错误码、脚本位置、SQL 行列，并保留完整日志。它是单机排查工具，规则识别不承诺所有引擎错误全覆盖。

```bash
.venv/bin/python -m pip install -r requirements/monitoring.txt
bash monitoring/start.sh
# 浏览器打开 http://localhost:8502
```

![任务运行台中的真实 SQL 失败记录](docs/assets/task-sql-error.png)

可选的 `sql_error_learning_demo` 展示提取成功、转换失败、加载未执行的状态；通过 `fix_sql=true` 可运行成功。见 [示例](examples/README.md) 与 [运行台说明](monitoring/README.md)。

## 完整数据与实验

数据来自 [Kaggle eCommerce behavior](https://www.kaggle.com/datasets/mkechinov/ecommerce-behavior-data-from-multi-category-store)，2019 年 10–11 月。数据不随仓库分发。

现有环境曾构建 61 个日分区、109,806,507 条 DWD 事实记录。历史结果与当前样本验证分开记录，见 [验证记录](docs/validation/README.md)；没有把本轮小样本运行当作亿级链路重验。

- [StarRocks 查询对照](docs/legacy/PERFORMANCE_BENCHMARK.md)：可选实验，有独立部署要求和测量局限。
- [倾斜与加盐实验](skew_lab/README.md)：记录加盐引入额外开销的结果，不宣称所有场景均有效。

## 目录与安装

| 目录 | 内容 |
|---|---|
| `etl/` | 五层转换、建表、质量校验、手动编排 |
| `scheduler/` | 正式 Airflow DAG 与可选告警 |
| `monitoring/` | 任务记录和 SQL 错误查看页 |
| `examples/` | 与正式调度分离的故障示例 |
| `tests/` | 固定事件样本与回归测试 |
| `requirements/` | 核心、监控、Airflow、StarRocks 分组依赖 |
| `starrocks/`、`skew_lab/` | 可选实验 |

[依赖与版本](requirements/README.md) · [运行手册](docs/operations.md) · [文档索引](docs/README.md)

当前边界：单机部署、批处理、非事务性文件覆盖；无多租户权限或高可用承诺。生产环境的写入原子性、凭据管理和跨机器日志需另外设计。
