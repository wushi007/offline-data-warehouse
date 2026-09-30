# 电商离线数仓（Spark + Hive + Airflow）

基于 [Kaggle eCommerce behavior](https://www.kaggle.com/datasets/mkechinov/ecommerce-behavior-data-from-multi-category-store)
数据集（2019-Oct / 2019-Nov，约 4,700 万行行为日志）构建的**离线数仓**，覆盖 **ODS → DWD → DIM → DWS → ADS** 完整五层，
31 天数据全链路跑通并通过跨层对账。

> **运行环境**：一台 **16 GB 内存 / 1 TB 硬盘**的 Windows 笔记本，环境为 **WSL2 + Ubuntu 22.04**，单机跑完 4,700 万行。
>
> ⚠️ **内存是这套设计的硬约束**：WSL2 默认只向虚拟机分配约一半物理内存
> （16 GB 宿主机上实测可见 **7.6 GB**），而 HDFS / MySQL / Airflow 常驻约 2.5 GB。
> 因此本项目的并发度、driver 内存、串行闸门都是围绕「**单会话、低并发**」来设计的 ——
> 详见 [内存与并发约束](#内存与并发约束)。换到更大内存的机器上可以放宽，但没必要。

> **第一次看这个项目？** 想跑起来 → [从零跑通](docs/GETTING_STARTED.md)；
> 想懂代码 → [代码解析](docs/CODE_GUIDE.md)。

## 项目亮点

- **一表一文件**：29 个 ETL 脚本按层组织（`etl/<层>/<表>.py`），每张表都能独立运行、独立重试，定位问题不用读整条链路
- **真实 SCD2 拉链**：从事实表反推商品属性，日主导组合去噪 → 变更打点 → 折叠区间 → 开链/闭链，落地 172,794 个版本
- **全链路幂等**：分区级 `INSERT OVERWRITE` + 确定性去重排序，同一输入重跑结果完全一致
- **跨层对账**：购买次数、GMV、漏斗不变量、分时守恒等 6 项月度指标全绿
- **DQC 质量闸门**：ODS/DWD 两层校验，失败即阻断下游调度
- **Airflow 调度 + 告警**：一表一 task、串行闸门（flock）保证同一时刻只有一个 SparkSession；4+1 通道任务告警
- **性能基准**：Spark vs StarRocks 双引擎对比（见 `docs/legacy/PERFORMANCE_BENCHMARK.md`）

## 架构

```
                 ┌──────────────────── 2019-Oct / Nov CSV (HDFS) ────────────────────┐
                 ▼
        ┌─────────────────┐
        │  ODS 层 (1 表)   │  ods_event_log            物理分区，34 个 event_date 分区
        └────────┬────────┘
                 ▼  清洗 / 去重 / 派生列
        ┌─────────────────┐
        │  DWD 层 (2 表)   │  dwd_event_fact  42,413,557 行 · 13 列
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

**关键设计取舍**：`event_type / behavior_type / is_purchase / event_hour / user_session` 作为**退化维度**直接进事实表，
DWS 聚合因此不需要 JOIN 维度表；而 ADS 漏斗跨商品精确去重 UV，必须回 DWD 计算。

## 内存与并发约束

这一节的参数不是「调优结果」，而是**在 16 GB / WSL2 环境下的生存底线** ——
理解了它，才知道为什么代码里有那么多「串行」和「单会话」的限制。

**可用内存有多少**：宿主 16 GB，但 WSL2 默认只分配约一半（`/etc/wsl.conf` 未调优时
约为物理内存的 50%，实测可见 **7.6 GB**）。再扣掉常驻的 HDFS + MySQL + Airflow（约 2.5 GB），
**留给 Spark 的实际余量只有 4~5 GB**。

**由此决定的三条硬性设计**：

| 约束 | 取值 | 为什么 |
|---|---|---|
| **driver 内存** | `3g`（`get_spark(memory=...)`） | 按机器可用内存给，不是按「任务看起来大」给。给到 `6g` 时 JVM 会被内核 OOM killer 杀掉 |
| **同一时刻一个 SparkSession** | `etl/run_locked.sh` 的 `flock` 闸门 | 两个会话并存会 OOM。闸门跨 DAG、跨手动执行都生效 |
| **调度并发 = 1** | DAG 的 `max_active_tasks=1` + `max_active_runs=1` | 与上面闸门是双保险：DAG 级管住本 DAG，闸门管住整机 |

**判据是「分区粒度匹配到达粒度」**：数据是**按天批量**落的（不是小时流），
DWD 重跑一天只要约 45 秒。所以 31 天就是 31 个日分区，重跑只覆盖当天 ——
没有为了「看起来更快」去做小时级分区，那只会把 31 个目录变成 744 个碎分区。

**换到更大内存的机器**：可以放开 `driver.memory`、提高 `max_active_tasks`，
但**不建议去掉 flock 闸门** —— 它是「重跑安全」的一部分，不只是内存保护。

## 快速开始

### 环境要求

| 组件 | 版本 | 说明 |
|---|---|---|
| 操作系统 | **WSL2 + Ubuntu 22.04** | 其他 Linux 发行版同理 |
| **内存** | **16 GB**（宿主） | WSL2 实际可见约 7.6 GB，见 [内存与并发约束](#内存与并发约束) |
| **硬盘** | **1 TB** | 源 CSV 约 9 GB，数仓中间结果约 15 GB |
| JDK | 17 | Spark 3.5 要求 |
| Spark | 3.5.9 | PySpark 3.5.8 |
| Hadoop (HDFS) | 3.4.3 | 只用 HDFS，不需要 MR |
| Hive Metastore | Spark 内嵌 | 用于外部表注册 |
| Python | 3.10 | 3.9+ 均可 |

**最低可行配置**：JDK 17 + Spark + HDFS + Python 环境。
MySQL / Airflow / StarRocks 都是可选组件，不影响主链路跑通。

```bash
# 1. 安装依赖
python -m venv .venv
.venv/bin/pip install -r requirements.txt

# 2. 配置环境变量（连接信息见 .env.example）
cp .env.example .env && vim .env

# 3. 准备源数据（CSV 上传到 HDFS）
hdfs dfs -put 2019-Oct.csv /home/lst/hadoop-data/eCommerce_behavior/

# 4. 建库建表：5 库 19 张表 + MSCK 注册 ODS 分区
./run.sh ddl

# 5. 全链路跑通 10 月整月（DWD → DWS → DIM → ADS + 跨层对账）
./run.sh build --start 2019-10-01 --end 2019-10-31

# 6. 只做对账（不重新构建）
./run.sh check --start 2019-10-01 --end 2019-10-31
```

> 需要按你的环境调整的只有两处：`config/config.py` 里的 HDFS 根路径，
> 以及 venv 位置（`run.sh` 会自动探测，探测不到时用 `PYTHON=/path/to/python ./run.sh ...`）。
> 详细步骤与常见报错见 [从零跑通](docs/GETTING_STARTED.md)。

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

## 数据成果（2019-10-01 ~ 10-31）

| 层 | 表数 | 代表表规模 |
|---|---|---|
| ODS | 1 | `ods_event_log` 47,017,436 行 |
| DWD | 2 | `dwd_event_fact` **42,413,557 行** |
| DIM | 5 | `dim_product_scd2` 172,794 版本 / `dim_user` 3,022,290 |
| DWS | 4 | `dws_user_session_daily` 9,280,709 行 |
| ADS | 7 | `ads_user_rfm_snapshot` 347,118 用户 |

**对账结果（全绿）**

```
✅ 购买次数守恒 DWD=DWS用户=DWS商品=DWS会话=ADS: 742,773
✅ GMV 守恒     DWD=DWS用户=DWS商品=ADS: 229,933,212.63
✅ 漏斗不变量（每日 view_uv 为上游最大值）: 0 天违规
✅ SCD2 每商品单一当前版本: 0 个违规
✅ 分时守恒 DWS=ADS=DWD明细: PV 42,413,557 / 购买 742,773
✅ 留存不变量（D+n 人数 ≤ D0）: 0 天违规
```

**业务洞察**

- **RFM 二八法则显著**：「重要价值客户」占购买用户的 **12.2%**，贡献 **55.6%** 的 GMV
- **留存**（基准日 10-16，D0 = 230,199）：D+1 **17.68%** / D+3 **13.20%** / D+7 **10.74%**，单调递减；月末右删失记 NULL 而非 0
- **分时流量峰谷比 13.76×**：上午(7-12) 转化最高（贡献 41.04% 订单），晚间(19-23) 客单价最高（354 元）

## 目录结构

```
.
├── run.sh                  # 统一入口：所有命令都从这里进
├── requirements.txt
├── config/
│   ├── config.py           # HDFS 路径 / 源 Schema / 业务口径常量 / 连接配置（读环境变量）
│   └── requirements.txt
├── etl/
│   ├── ddl.py              # 建表 + MSCK
│   ├── pipeline.py         # 编排 + 跨层对账
│   ├── run_locked.sh       # 串行闸门（flock）
│   ├── utils.py            # get_spark / drop_path / date_range
│   ├── ods/                # 接入 + 校验
│   ├── dwd/                # 清洗 + 派生列 + 脏数据隔离 + DQC
│   ├── dim/                # SCD2 拉链 + 4 张全量维表
│   ├── dws/                # 4 张汇总表
│   └── ads/                # 7 张应用表
├── scheduler/
│   ├── incremental_warehouse_dag.py   # Airflow DAG，一表一 task
│   ├── alerts.py                      # 任务告警（4+1 通道）
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
| **[docs/GETTING_STARTED.md](docs/GETTING_STARTED.md)** | **从零跑通**：环境要求、依赖安装、启 HDFS、改配置、建表、跑数据、验证结果、11 个常见报错 |
| **[docs/CODE_GUIDE.md](docs/CODE_GUIDE.md)** | **代码解析**：五层架构、每层实现细节、SCD2 算法、幂等机制、6 个关键设计决策 |
| [docs/WORKFLOW.md](docs/WORKFLOW.md) | 日常操作手册 |
| [docs/incremental/WAREHOUSE_BUILD.md](docs/incremental/WAREHOUSE_BUILD.md) | **构建记录**：19 张表清单、分层依赖、SCD2 设计、对账结果、交叉验证 |
| [docs/incremental/ALERTS.md](docs/incremental/ALERTS.md) | 告警机制：4+1 通道、事件分级、台账字段 |
| [docs/legacy/DWS_ADS_TABLES.md](docs/legacy/DWS_ADS_TABLES.md) | DWS/ADS 数据字典（34 天实测数字） |
| [docs/legacy/PERFORMANCE_BENCHMARK.md](docs/legacy/PERFORMANCE_BENCHMARK.md) | StarRocks vs Spark 查询性能基准 |

## 已知限制

- **SCD2 时间精度到天**：源数据只有「观测时刻」没有「变更时刻」，`dw_start_date` 无法更细
- **单日 UV 与旧实现有 0.0x% 偏移**：本次固定了确定性去重排序，旧实现保留哪条不确定；月度守恒指标完全对得平
- **`dwd_event_dirty` 为 0 行**是正常的：源数据本身无脏数据，隔离表与规则保留待用

## 许可

[MIT](LICENSE)
