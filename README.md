# 电商离线数仓（Spark + Hive + Airflow）

基于 [Kaggle eCommerce behavior](https://www.kaggle.com/datasets/mkechinov/ecommerce-behavior-data-from-multi-category-store)
数据集（2019-Oct / 2019-Nov，61 天共约 1.1 亿行行为日志）构建的离线数仓，覆盖 ODS → DWD → DIM → DWS → ADS 五层。
2019-10 的 31 天完成全链路跑通与跨层对账（DWD 4,241 万行；ODS 4,700 万行含 11-01~03 三个分区）；2019-11 数据用于增量与性能基准，全量 DWD 为 109,806,507 行 / 61 分区。

> 运行环境：16 GB 内存 / 1 TB 硬盘的 Windows 笔记本，WSL2 + Ubuntu 22.04，
> 单机完成 1.1 亿行源数据的导入与 DWD 构建，其中 31 天跑通 ODS→ADS 全链路。
>
> 本项目是 Spark SQL 数仓 Demo，环境配置不是通用方案，仅供思路参考。
> 所有内存与并发参数都按「这台机器只有 16 GB 内存」这一前提调整，处于单机 Spark 演示的范围内。
>
> WSL2 默认只向虚拟机分配约一半物理内存（16 GB 宿主机上实测可见 7.6 GB），
> 扣掉常驻的 HDFS / MySQL / Airflow（约 2.5 GB）后，留给 Spark 的实际余量只有 4~5 GB。
> 由此确定三条硬性设计：standalone 模式下 driver 1g / executor 4×1g、单会话 flock 闸门、调度并发 = 1，
> 详见 [内存与并发约束](#内存与并发约束)。

> 第一次接触本项目时，先读 [从零跑通](docs/GETTING_STARTED.md)；
> 了解代码结构读 [代码解析](docs/CODE_GUIDE.md)。

## 项目亮点

- 一表一文件：24 张表层脚本 + 6 个公共工具按层组织（`etl/<层>/<表>.py`），每张表独立运行、独立重试，定位问题不需要读整条链路
- SCD2 拉链：从事实表反推商品属性，日主导组合去噪 → 变更打点 → 折叠区间 → 开链/闭链，全量落地 222,761 个版本（206,876 商品）
- 全链路幂等：分区级 `INSERT OVERWRITE` + 确定性去重排序，同一输入重跑结果完全一致
- 跨层对账：购买次数、GMV、漏斗不变量、分时守恒等 6 项月度指标全部通过
- DQC 质量闸门：ODS/DWD 两层校验，失败即阻断下游调度
- Airflow 调度与告警：一层一 task，层内一个 SparkContext + 一个 Session 逐表串行，flock 串行闸门保证同一时刻只处理一张表；6 通道任务告警
- 性能基准：Spark 与 StarRocks 双引擎对比（见 `docs/legacy/PERFORMANCE_BENCHMARK.md`）

## 架构

```
                 ┌──────────────────── 2019-Oct / Nov CSV (HDFS) ────────────────────┐
                 ▼
        ┌─────────────────┐
        │  ODS 层 (1 表)   │  ods_event_log            物理分区，61 个 event_date 分区（2019-10-01 ~ 11-30）
        └────────┬────────┘
                 ▼  清洗 / 去重 / 派生列
        ┌─────────────────┐
        │  DWD 层 (2 表)   │  dwd_event_fact  109,806,507 行 · 13 列（2019-10/11 全量）
        │                 │  dwd_event_dirty 脏数据隔离
        └────────┬────────┘
                 ▼
    ┌────────────┴────────────┐
    ▼                         ▼
┌──────────────┐      ┌──────────────┐
│ DIM 层 (5 表) │      │ DWS 层 (4 表) │  日×会话 / 日×商品 / 日×用户 / 日×小时
│ 拉链+全量维度  │      │              │  聚合不 JOIN 维度（星型模型收益）
└──────┬───────┘      └──────┬───────┘
       └──────────┬──────────┘
                  ▼
          ┌──────────────┐
          │ ADS 层 (7 表) │  交易 / 漏斗 / 热榜 / 会话 / 留存 / 分时 / RFM
          └──────────────┘
```

关键设计取舍：`event_type / behavior_type / is_purchase / event_hour / user_session` 作为退化维度直接进入事实表，
DWS 聚合因此不需要 JOIN 维度表；ADS 漏斗需要跨商品精确去重 UV，必须回到 DWD 计算。

## 内存与并发约束

本节参数不是调优结果，而是 16 GB / WSL2 环境下的必要约束，代码中的串行和单会话限制都由此而来。

可用内存：宿主机 16 GB，WSL2 默认只分配约一半（`/etc/wsl.conf` 未调优时
约为物理内存的 50%，实测可见 7.6 GB）。扣掉常驻的 HDFS + MySQL + Airflow（约 2.5 GB），
留给 Spark 的实际余量只有 4~5 GB。

由此确定以下硬性设计：

| 约束 | 取值 | 说明 |
|---|---|---|
| driver 内存 | `1g`（`SPARK_DRIVER_MEMORY`） | standalone 的 driver 在客户端进程内，按可用内存给，不按任务规模 |
| executor | `4 × 1g × 2 核`（`SPARK_EXECUTOR_*`） | Worker 8 核 / 4g，正好 4 个 executor，并行度 8 |
| 同一时刻一个 SparkSession | `etl/run_locked.sh` 的 `flock` 闸门 | 两个作业并存会 OOM，闸门对 DAG 和手动执行都生效 |
| 调度并发 = 1 | DAG 的 `max_active_tasks=1` + `max_active_runs=1` | 与闸门构成双保险：DAG 级管住本 DAG，闸门管住整机 |

Spark 侧内存上限：Master 512m + Worker 512m + Driver 1g + 4×1g executor ≈ 6g。
executor 的 1g 是堆上限而非预占，轻量作业实际占用更低；同时只有一个作业，靠 `flock` 保证。

分区粒度与数据到达粒度一致：数据按天批量落盘，不是小时流，DWD 重跑一天约 45 秒。
31 天对应 31 个日分区，重跑只覆盖当天。项目没有采用小时级分区，否则 31 个目录会变成 744 个碎分区。

### Spark 运行模式

默认连 standalone 集群（`spark://localhost:7077`）：

```bash
$SPARK_HOME/sbin/start-master.sh
$SPARK_HOME/sbin/start-worker.sh spark://localhost:7077
```

集群参数模板见 `conf/spark-env.sh.example`，复制到 `$SPARK_HOME/conf/spark-env.sh` 后按本机路径调整。
切换本地模式只改一个变量：`SPARK_MASTER=local[6]`。

常用环境变量：`SPARK_MASTER`、`SPARK_DRIVER_MEMORY`、`SPARK_EXECUTOR_MEMORY`、
`SPARK_EXECUTOR_CORES`、`SPARK_EXECUTOR_INSTANCES`，默认值在 `.env` 里。

换到更大内存的机器时，可以提高 executor 数、`max_active_tasks` 和 driver 内存，
但不建议去掉 flock 闸门，它也是重跑安全的一部分，不只是内存保护。

## 快速开始

### 环境要求

| 组件 | 版本 | 说明 |
|---|---|---|
| 操作系统 | WSL2 + Ubuntu 22.04 | 其他 Linux 发行版同理 |
| 内存 | 16 GB（宿主） | WSL2 实际可见约 7.6 GB，见 [内存与并发约束](#内存与并发约束) |
| 硬盘 | 1 TB | 源 CSV 约 9 GB，数仓中间结果约 15 GB |
| JDK | 17 | Spark 3.5 要求 |
| Spark | 3.5.9 | PySpark 3.5.8 |
| Hadoop (HDFS) | 3.4.3 | 只用 HDFS，不需要 MR |
| Hive Metastore | Spark 内嵌 | 用于外部表注册 |
| Python | 3.10 | 3.9+ 均可 |

最低可行配置：JDK 17 + Spark + HDFS + Python 环境。
MySQL / Airflow / StarRocks 均为可选组件，不影响主链路跑通。

```bash
# 1. 安装依赖
python -m venv .venv
.venv/bin/pip install -r requirements.txt

# 2. 配置环境变量（连接信息见 .env.example）
cp .env.example .env && vim .env

# 3. 准备源数据（CSV 上传到 HDFS）
hdfs dfs -put 2019-Oct.csv /home/lst/hadoop-data/eCommerce_behavior/

# 4. ODS 接入（10 月用 ods-bulk 指定源文件；ods-import 默认读 2019-Nov.csv）
./run.sh ods-bulk --csv hdfs://localhost:8020/home/lst/hadoop-data/eCommerce_behavior/2019-Oct.csv

# 5. 建库建表：5 库 19 张表 + MSCK 注册全部分区表（RFM 快照除外）
./run.sh ddl

# 6. 全链路跑通 10 月整月（DWD → DWS → DIM → ADS + 留存/RFM + 跨层对账）
./run.sh build --start 2019-10-01 --end 2019-10-31

# 7. 只做对账（不重新构建）
./run.sh check --start 2019-10-01 --end 2019-10-31
```

需要按环境调整的只有一处：`config/config.py` 里的 HDFS 数据路径。
Java / Spark / Hadoop / venv 的位置由 `run.sh` 自动探测，探测不到时用
`SPARK_HOME=... PYTHON=... ./run.sh ...` 显式指定。
详细步骤与常见报错见 [从零跑通](docs/GETTING_STARTED.md)。

### 常用命令

```bash
./run.sh day 2019-11-01          # 单日 ODS 接入 + 校验
./run.sh build --dt 2019-11-01   # 单日增量（DIM 走增量）
./run.sh build --only dwd,dws    # 只跑指定层
./run.sh backfill --start 2019-11-01 --end 2019-11-30   # 批量回补

# 单张表（一张表一个进程，都经 flock 串行闸门排队）
./run.sh dwd --dt 2019-11-01
./run.sh dim-scd2 --dt 2019-11-01
./run.sh table etl/ads/ads_trade_daily.py --dt 2019-11-01
```

## 数据成果（2019-10 单月，历史）

| 层 | 表数 | 代表表规模 |
|---|---|---|
| ODS | 1 | `ods_event_log` 47,017,436 行 |
| DWD | 2 | `dwd_event_fact` 42,413,557 行 |
| DIM | 5 | `dim_product_scd2` 172,794 版本 / `dim_user` 3,022,290 |
| DWS | 4 | `dws_user_session_daily` 9,280,709 行 |
| ADS | 7 | `ads_user_rfm_snapshot` 347,118 用户 |

对账结果（全部通过）：

```
OK 购买次数守恒 DWD=DWS用户=DWS商品=DWS会话=ADS: 742,773
OK GMV 守恒     DWD=DWS用户=DWS商品=ADS: 229,933,212.63
OK 漏斗不变量（每日 view_uv 为上游最大值）: 0 天违规
OK SCD2 每商品单一当前版本: 0 个违规
OK 分时守恒 DWS=ADS=DWD明细: PV 42,413,557 / 购买 742,773
OK 留存不变量（D+n 人数 ≤ D0）: 0 天违规
```

### 当前 61 天全量（2019-10-01 ~ 11-30）

| 层 | 代表规模 |
|---|---|
| DWD | `dwd_event_fact` 109,806,507 行 |
| DIM | `dim_product_scd2` 222,761 版本 / 206,876 商品；`dim_user` 5,316,649；`dim_session` 23,004,969；`dim_category` 691 |
| ADS | `ads_user_rfm_snapshot` 707,466 用户（as_of_dt=2019-11-30）；留存 61 天（7 天右删失） |

当前全量对账（2019-10-01 ~ 11-30）：

```
OK 购买次数守恒 DWD=DWS用户=DWS商品=DWS会话=ADS: 1,680,272
OK GMV 守恒     DWD=DWS用户=DWS商品=ADS: 511,810,289.17
OK 漏斗不变量（每日 view_uv 为上游最大值）: 0 天违规
OK SCD2 每商品单一当前版本: 0 个违规
OK 分时守恒 DWS=ADS=DWD明细: PV 109,806,507 / 购买 1,680,272
OK 留存覆盖与不变量: 61/61 天，0 天违规；其中 7 天右删失（NULL）
```

2019-10 业务洞察：

- RFM 分布符合二八法则：「重要价值客户」占购买用户的 12.2%，贡献 55.6% 的 GMV
- 留存（基准日 10-16，D0 = 230,199）：D+1 17.68% / D+3 13.20% / D+7 10.74%，单调递减；月末右删失记 NULL 而非 0
- 分时流量峰谷比 13.76×：上午(7-12) 转化最高（贡献 41.04% 订单），晚间(19-23) 客单价最高（354 元）

## 源数据质量观察（2019-10-01 ~ 11-30，61 天）

将窗口扩展到 61 天检查源数据，发现三类现象。这些现象均属采集侧或源数据性质，不是业务波动，也不是计算错误。

### 1. 采集漏数：purchase 事件成片缺小时

11-14 ~ 11-17 四天里 `view` / `cart` 都覆盖满 24 小时，`purchase` 只覆盖 19 / 0 / 8 / 21 小时（11-15 原始 purchase 全天 0 条）。这不是业务上无人购买，而是采集缺失。

> 判别方法：按日期 × 事件类型 × 小时统计覆盖小时数。缺小时的情况不会像总量那样被高流量日掩盖。

### 2. 采集换挡：11-14 ~ 11-17 的 UV 突增（11-18 又换回）

同一套采集系统在 11-14 ~ 11-17 更换了采集档位，有四条证据：

| 证据 | 观测 |
|---|---|
| 切换边界 | 切换发生在自然日边界，前后相邻日完全正常（业务高峰通常缓升缓降、跨午夜延续） |
| 放大倍数 | view 3.13× / cart 5.96× / purchase 8.10×，三者不一致（真实业务变化应同比例） |
| 分布形状 | 每用户事件数整条分布平移、形状不变（p50 3→6、p90 16→32、p99 52→106），不是尾部抬高 |
| 用户池 | 与前后日的重叠率接近随机期望，排除换用户群或换数据源 |

> 从数据文件本身无法确定上游更换档位的原因，只能确认这是采集侧变化，不是业务波动。

### 3. 语义 / 一致性：源数据自带，不是算错

- 漏斗倒挂在源数据中已经存在：61 天里有 22 天 `purchase_uv > cart_uv`（转化率最大 1.763；会话级 21 天 > 1，最大 1.324）。一部分与上面的 purchase 缺失、cart 换挡相关，另一部分是加购与下单可跨天的窗口语义，属于源数据特征。
- 存在 `price = 0` 的购买事件：有销量而零金额的情况在源数据内部自洽，不是丢单。
- 存在当日无浏览的购买：购买 UV 可以超过曝光 UV（曝光只统计 `view`），因为用户当天不浏览也能下单。这是 61 天观察窗口截断和跨天行为的结果。

## 目录结构

```
.
├── run.sh                  # 统一入口：所有命令都从这里进
├── requirements.txt
├── config/
│   ├── config.py           # HDFS 路径 / 源 Schema / 业务口径常量 / 连接配置（读环境变量）
│   └── requirements.txt
├── etl/
│   ├── ddl.py              # 建表 + 全分区 MSCK
│   ├── pipeline.py         # 编排 + 跨层对账
│   ├── injector.py         # 层内单 Session 注入器（DAG 用）
│   ├── plans/              # 分层任务清单（inject --plan）
│   ├── run_locked.sh       # 串行闸门（flock）
│   ├── utils.py            # get_spark / drop_path / date_range
│   ├── ods/                # 接入 + 校验
│   ├── dwd/                # 清洗 + 派生列 + 脏数据隔离 + DQC
│   ├── dim/                # SCD2 拉链 + 4 张全量维表
│   ├── dws/                # 4 张汇总表
│   └── ads/                # 7 张应用表
├── scheduler/
│   ├── incremental_warehouse_dag.py   # Airflow DAG，一层一 task（层内单 Session 串行）
│   ├── alerts.py                      # 任务告警（6 通道）
│   └── alert_records/                 # 告警台账（运行时产物，不入库）
├── starrocks/               # StarRocks 性能基准与 ADS 导出
├── docs/
│   ├── GETTING_STARTED.md  # 从零跑通：配置 → 建表 → 跑数 → 验证
│   ├── CODE_GUIDE.md       # 代码解析：架构 / 各层实现 / 设计取舍
│   ├── WORKFLOW.md         # 日常操作手册
│   ├── incremental/        # 本次建设：构建记录 / 告警机制
│   └── legacy/             # 旧实现：数据字典 / 性能基准
```

## 文档

| 文档 | 内容 |
|---|---|
| [docs/GETTING_STARTED.md](docs/GETTING_STARTED.md) | 从零跑通：环境要求、依赖安装、启 HDFS、改配置、建表、跑数据、验证结果、11 个常见报错 |
| [docs/CODE_GUIDE.md](docs/CODE_GUIDE.md) | 代码解析：五层架构、每层实现细节、SCD2 算法、幂等机制、6 个关键设计决策 |
| [docs/WORKFLOW.md](docs/WORKFLOW.md) | 日常操作手册 |
| [docs/incremental/WAREHOUSE_BUILD.md](docs/incremental/WAREHOUSE_BUILD.md) | 构建记录：19 张表清单、分层依赖、SCD2 设计、对账结果、交叉验证 |
| [docs/incremental/ALERTS.md](docs/incremental/ALERTS.md) | 告警机制：6 通道、事件分级、台账字段 |
| [docs/legacy/DWS_ADS_TABLES.md](docs/legacy/DWS_ADS_TABLES.md) | DWS/ADS 数据字典（34 天实测数字） |
| [docs/legacy/PERFORMANCE_BENCHMARK.md](docs/legacy/PERFORMANCE_BENCHMARK.md) | StarRocks vs Spark 查询性能基准 |

## 已知限制

- SCD2 时间精度到天：源数据只有「观测时刻」没有「变更时刻」，`dw_start_date` 无法更细
- 单日 UV 与旧实现有 0.0x% 偏移：本次固定了确定性去重排序，旧实现保留哪条不确定；月度守恒指标完全对得平
- `dwd_event_dirty` 为 0 行是正常现象：源数据本身无脏数据，隔离表与规则保留待用
- 源数据自带漏斗倒挂 / 零金额 / 无浏览购买：与上表「漏斗不变量 0 天违规」（只要求 `view_uv` 为上游最大值）不矛盾，
  属源侧特征，详见 [源数据质量观察](#源数据质量观察2019-10-01--11-3061-天)

## 许可

[MIT](LICENSE)
