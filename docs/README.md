# 文档索引

本目录按**两代实现**分成两个文件夹 —— 读任何一篇前，先确认它讲的是哪一代。

```
docs/
├── README.md          ← 本文件：索引 + 两代的区别
├── GETTING_STARTED.md ← 从零跑通：环境配置 → 建表 → 跑数 → 验证
├── CODE_GUIDE.md      ← 代码解析：架构、各层实现、关键设计决策
├── WORKFLOW.md        ← 日常操作手册（改代码 / 跑任务 / 推送）
├── incremental/       本次建设（增量数仓：Spark + Airflow，2019-10/11）
└── legacy/            旧实现（批量数仓：Spark + StarRocks，2019-10/11）
```

## 先读这两篇

| 文档 | 什么时候看 |
|---|---|
| [GETTING_STARTED.md](GETTING_STARTED.md) | **第一次拿到代码**：环境准备、依赖安装、改配置、建表、跑通、查结果、常见报错 |
| [CODE_GUIDE.md](CODE_GUIDE.md) | **想搞懂代码**：五层架构、每层在做什么、SCD2 算法、幂等机制、关键设计取舍 |

## 其余文档

| 文档 | 内容 |
|---|---|
| [WORKFLOW.md](WORKFLOW.md) | **日常怎么干活**：三个路径的分工、提交推送流程、跑任务的命令、环境启停 |

## 怎么区分两代实现

| | `incremental/`（本次，2026-09-29 起） | `legacy/`（旧，2026-08 及更早） |
|---|---|---|
| 代码 | `etl/` 下**一表一文件**、每层一个文件夹 | 多张表挤在一个脚本里（如 `build_dims.py`、`dws_aggregate.py`）——**这些文件已被删除或取代** |
| 调度 | Airflow（`scheduler/incremental_warehouse_dag.py`，一表一 task） | 手动 / `run.sh` |
| 引擎 | Spark（HDFS + Hive metastore） | Spark + **StarRocks** 双引擎 |
| 存储 | Hive 外部表，19 张（5 层） | 部分表随 2026-09-29 数仓清空被 DROP |
| 数据区间 | 2019-10-01 ~ 2019-11-04（已处理） | 2019-10-01 ~ 2019-11-03（34 天） |

⚠️ `legacy/` 里的文档**仍然有价值**：它记录了被删实现的**历史数字**，本次重建就是拿它做交叉验证
（11-01/02/03 的 GMV 逐项对得上）。只是里面的**代码路径引用大多已失效**，别照着去找文件。

## `incremental/` —— 本次建设

| 文档 | 内容 |
|---|---|
| [WAREHOUSE_BUILD.md](incremental/WAREHOUSE_BUILD.md) | 建设过程与结果：19 张表清单、分层依赖、SCD2 设计、幂等机制、跨层对账、与旧文档的交叉验证、常用命令 |
| [ALERTS.md](incremental/ALERTS.md) | **任务告警机制**：4+1 通道、事件分级、触发点、台账字段与真实样例、怎么启手机推送、已知局限 |

## `legacy/` —— 旧实现

| 文档 | 内容 | 注意 |
|---|---|---|
| [DWS_ADS_TABLES.md](legacy/DWS_ADS_TABLES.md) | DWS/ADS 层数据字典（34 天实测数字） | 表结构口径**沿用至今**；代码路径已失效 |
| [PERFORMANCE_BENCHMARK.md](legacy/PERFORMANCE_BENCHMARK.md) | StarRocks vs Spark 查询性能基准 | 依赖已停的 StarRocks |

## 不在本目录的相关材料

| 位置 | 说明 |
|---|---|
| `/home/lst/my-spark/docs/` | **上一级仓库**的文档，不属于本项目，未纳入本次整理 |
