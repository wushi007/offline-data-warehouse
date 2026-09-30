# 数仓建设落地记录（2026-09-29）

> 背景：2026-09-29 数仓分层被整体清空（只留 ODS 物理文件），本文件记录**重新建表 + 跑通一个月**的完整过程、结果、以及踩过的每一个坑与修法。
> 口径**沿用仓库既有约定**（不是新设计）：分区列 `event_date`、四类事件枚举、既有表名。
> 数据区间：**2019-10-01 ~ 2019-10-31（31 天整月）**，源为 `2019-Oct.csv`（ODS 里本就已按天落好，无需重导）。

---

## 一、产出总览：5 库 19 张表

建表脚本 `etl/ddl.py`，全部是 **Hive 外部表 + LOCATION 指向 HDFS**（DROP 表不删文件，便于重建）。

| 层 | 表 | 行数 | 分区 |
|---|---|---|---|
| ODS | `ods.ods_event_log` | 47,017,436 | 34（含 11-01~11-03） |
| DWD | `dwd.dwd_event_fact` | **42,413,557** | 31 |
| DWD | `dwd.dwd_event_dirty` | 0 | 0（源数据无脏数据，见 §六） |
| DIM | `dim.dim_product_scd2` | 172,794 版本 / 166,794 商品 | 全量 |
| DIM | `dim.dim_date` | 365（2019 全年） | 全量 |
| DIM | `dim.dim_user` | 3,022,290 | 全量 |
| DIM | `dim.dim_category` | 624 | 全量 |
| DIM | `dim.dim_session` | 9,240,063 | 全量 |
| DWS | `dws.dws_user_session_daily` | 9,280,709 | 31 |
| DWS | `dws.dws_product_daily` | 2,299,245 | 31 |
| DWS | `dws.dws_user_behavior_daily` | 6,473,723 | 31 |
| DWS | `dws.dws_traffic_hour_daily` | 744（31×24） | 31 |
| ADS | `ads.ads_trade_daily` | 31 | 31 |
| ADS | `ads.ads_conversion_funnel_daily` | 31 | 31 |
| ADS | `ads.ads_product_hot_rank_daily` | 3,100（31×100） | 31 |
| ADS | `ads.ads_session_behavior_daily` | 31 | 31 |
| ADS | `ads.ads_traffic_hour_daily` | 744 | 31 |
| ADS | `ads.ads_user_retention_daily` | 31 | 31 |
| ADS | `ads.ads_user_rfm_snapshot` | 347,118 | 快照 `as_of_dt` |

**DWD 事实表 13 列口径**（与旧表 `dwd_ecom_behavior` 对齐，可拼接）：
9 个源字段 + `behavior_type`(中文) + `is_purchase`(0/1) + `event_hour` + `event_date` 分区列。
其中 `event_type / behavior_type / is_purchase / event_hour / user_session` 是**退化维度**，直接进事实表不做维表关联。

---

## 二、分层与依赖

```
ODS(物理分区) → DWD 清洗 → DWS 汇总(4表) → ADS 应用(7表)
                        ↘ DIM 维度(5表)    ↗
```

- **DWS 聚合不 JOIN 维度**：类目/品牌已在事实表退化成列，直接 GROUP BY，省掉 JOIN（星型模型的核心收益）。
- **ADS 漏斗回 DWD**：跨商品精确去重 UV，不能拿商品级 UV 累加，所以漏斗不走 DWS。
- **DIM 拉链要窗口**：SCD2 需要多天比对才能收敛版本区间，故传 31 天回看窗口重建；而 `dim_user` 等全量维度扫全部 DWD 分区、不随窗口变化。

---

## 三、SCD2 拉链（真实拉链，不是初始快照）

源里没有商品主数据表，商品属性只能从事实表反推。算法（`etl/dim/dim_product_scd2.py::SCD2_SQL`）：

1. **日主导组合**：每天每商品取当日出现次数最多的 `(category_id, category_code, brand)`
   （同票数按 `category_id` 兜底排序 → 结果确定），消除单日噪声（实测日粒度上一个商品一天最多出现 3 种属性组合）；
2. **变更打点**：按 `product_id` 按天排，`LAG` 比对组合是否变化，变化即版本号 +1；
3. **折叠区间**：同版本连续日期折叠成 `[dw_start_date, last_seen]`；
4. **开链/闭链**：
   - 每商品**最后一个版本**永远开放：`dw_end_date = 9999-12-31`、`dw_is_current = 1`；
   - 其余版本闭链：`dw_end_date = last_seen + 1`（= 下一版本起始日），区间无重叠、无间隙；
5. **代理键** `dim_product_sk` 按 `(product_id, dw_start_date)` 全局编号，与业务键解耦。

结果：**166,794 个商品共 172,794 个版本**，其中 **6,000 个商品在 10 月发生过属性变更**。
校验：当前版本数 ≠1 的商品 **0** 个、区间重叠 **0** 对。

**用途**：查某天归属用 `event_date BETWEEN dw_start_date AND dw_end_date`；取当前口径用 `dw_is_current = 1`。

> **SCD2 的时间精度天花板**：拉链要"变更时刻"，但源数据只有**观测时刻**（用户看到这个商品是什么属性），
> 没有**变更时刻**（商品何时被改的）。所以 `dw_start_date` 只能到天——这是数据源的边界，不是实现偷懒。

---

## 四、时间维度的设计取舍（哪里加时间有价值）

**先看数据里有没有信号**（跨 31 天按小时聚合）：

| 小时 | PV | 占比 | 订单 | 客单价 |
|---|---|---|---|---|
| 2 | 1,068,141 | 2.52% | 13,968 | 277 |
| **9** | 2,349,158 | 5.54% | **55,182（峰值）** | 318 |
| **16** | **3,053,027（峰值）** | **7.20%** | 35,772 | 297 |
| **21** | 438,183 | 1.03% | 6,278 | **374（峰值）** |
| 23 | 221,922（谷值） | 0.52% | 2,802 | 355 |

**峰谷比 13.76×**，且三个信号错位。全月分时段汇总（`ads_traffic_hour_daily`）：

| 时段 | PV 占比 | 订单占比 | 客单价 | 读数 |
|---|---|---|---|---|
| 凌晨(0-6) | 23.07% | 26.02% | 296.96 | 流量不小、客单价最低 |
| **上午(7-12)** | 32.28% | **41.04%** | 313.72 | **转化最高**，投放主战场 |
| 下午(13-18) | **37.53%** | 27.61% | 306.59 | **流量最大但转化最差** |
| 晚间(19-23) | 7.11% | 5.33% | **354.48** | 流量最少、**客单价最高** |

流量峰值小时分布：**16 点当了 26 天峰值**，15 点 4 天，10 点 1 天。

### 结论：加在这三处

1. **DWS 加一张分时汇总表** `dws_traffic_hour_daily`（粒度 `event_date × event_hour`）
   —— `event_hour` 已在事实表里，一次 GROUP BY 即得，**1.37M 行/天 压到 24 行/天**，成本几乎为零；
2. **ADS 加分时大盘** `ads_traffic_hour_daily` —— 在 DWS 之上加报表语义：时段分桶 / PV 与订单占比 / 峰值小时标记 / 分时转化率；
3. **`dim_user` 挂时间属性** —— `first_seen_date / last_seen_date / active_days / first_purchase_date / purchase_days`，支撑新老客与生命周期。

### 结论：不加在这两处

- **ODS / DWD 加 hour 分区**：判据是**分区粒度要匹配"到达粒度"和"重跑粒度"**。
  这份数据是**按天批量**落的（不是小时流），DWD 重跑一天只要 45 秒；1.37M 行/天 ÷ 24 ≈ 57k 行、10MB 一个分区，
  加了只是把 31 个目录变成 744 个碎分区，小文件问题、扫得更慢，还要重导 9GB。
- **丢掉 `event_time` 只留 `event_hour`**：会话时长、行为先后、漏斗路径全靠 `event_time`，`event_hour` 是便利列不是替代品。

---

## 五、幂等与续跑

| 机制 | 实现 |
|---|---|
| 分区级覆盖 | 全链路静态分区 `INSERT OVERWRITE TABLE t PARTITION (event_date='D')`，重跑只重写当天 |
| 动态覆盖兜底 | `spark.sql.sources.partitionOverwriteMode=dynamic`（`etl/utils.py::get_spark`） |
| 时区固定 | `spark.sql.session.timeZone=UTC`（源时间戳是 UTC；不锁死会让 `to_date` 把每天 16:00-23:59 挪到次日，见 `../legacy/PITFALLS.md` A2） |
| 断点续跑 | `--skip-existing`：DWD 分区已有文件则跳过，批量回补可从断点接上 |
| 重建前清目录 | `utils.drop_path()`：外部表 DROP 不删文件，残留目录会被推断进新表导致 arity 不匹配 |

**去重必须是确定性的**：去重键 `(user_id, event_time, product_id, event_type)` 的所有列都参与分组，
**组内 `event_time` 必然相同**，只用它排序时保留哪条取决于扫描顺序 → 重跑结果漂移（会话数/类目快照跟着变）。
已追加 `category_id, category_code, brand, price, user_session` 做兜底排序，保证同一输入必得同一输出。

---

## 六、对账结果（全绿）

`./run.sh check --start 2019-10-01 --end 2019-10-31`：

```
DWD 明细 42,413,557 行 / 购买 742,773 / GMV 229,933,212.63
✅ 购买次数守恒 DWD=DWS用户=DWS商品=DWS会话=ADS: 742,773
✅ GMV 守恒     DWD=DWS用户=DWS商品=ADS: 229,933,212.63
✅ 漏斗不变量（每日 view_uv 为上游最大值）: 0 天违规
✅ SCD2 每商品单一当前版本: 0 个违规
✅ 分时守恒 DWS=ADS=DWD明细: PV 42,413,557=42,413,557=42,413,557 / 购买 742,773
✅ 留存不变量（D+n 人数 ≤ D0）: 0 天违规；其中 7 天 D+7 右删失（NULL）
```

DWD 质量闸门 `etl/dwd/dwd_event_fact_dqc.py`（逐日跑，失败即阻断 Airflow 下游）：
去重键唯一性 / 核心字段空值 / 枚举越界 / price 负值 / **分区与事件 UTC 日期一致性** —— 抽检日全部 PASS。

**关于 `dwd_event_dirty` 为 0 行**：这不是漏跑。源数据本身干净——无空键、无非法枚举、无负价格，
所以拦下的脏数据是 0，ODS 行数 = DWD 行数 + 去重行数 逐日对平（如 10-01：1,244,245 = 1,243,663 + 582）。
隔离表与 DQC 规则都留着，脏数据一旦出现就会被拦下并留痕。

**留存**（例：基准日 2019-10-16，D0 = 230,199 用户）：
D+1 **17.68%** / D+3 **13.20%** / D+7 **10.74%** —— 单调递减，符合预期。
月末 7 天（10-25 起）的 D+7 输出 **NULL 而非 0**：右删失（数据没到那天）不能当"留存归零"汇报。

**RFM 业务洞察**：347,118 购买用户中，「重要价值客户」占 **12.2%**，贡献 **55.6%** 的 GMV——二八法则显著。

---

## 七、与历史文档的交叉验证

`../legacy/DWS_ADS_TABLES.md` 记录过**旧实现**跑 34 天（2019-10-01~11-03）的总量。本次只跑了 10 月 31 天，
两者相减应等于 11-01~11-03 三天的量，可用来反向验证本次结果：

| 指标 | 旧实现 34 天 | 本次 31 天（10 月） | 差值 = 11-01~03 | 3 日日均 | 本次 10 月日均 |
|---|---|---|---|---|---|
| DWD 行数 | 46,908,364 | 42,413,557 | 4,494,807 | 1,498,269 | 1,368,179 |
| 购买次数 | 809,238 | 742,773 | 66,465 | 22,155 | 23,961 |
| GMV | 249,929,113.38 | 229,933,212.63 | 19,995,900.75 | 6,665,300 | 7,417,200 |

三天的日均与 10 月日均**同量级**（11 月前 3 天的购买/GMV 略低），说明本次重建的量级与口径与历史实现一致。

> 精确到单日会有 0.0x% 的差异，来源是**去重兜底排序**：例如 10-01 漏斗 `view_uv` 本次 190,158、
> 旧文档 190,037（差 0.06%）。旧实现的去重保留哪条不确定，本次固定了确定性排序，
> 因此单日 UV 有极小偏移，但月级守恒指标完全对得平。

---

## 八、怎么跑

```bash
cd /path/to/ecom-warehouse

./run.sh ddl                                   # 建 5 库 19 表 + MSCK 注册 ODS 分区
./run.sh build --start 2019-10-01 --end 2019-10-31   # 全链路（DWD→DWS→DIM→ADS + 对账）
./run.sh build --dt 2019-11-01                 # 单日增量（DIM 走增量）
./run.sh build --only dwd,dws                  # 只跑指定层
./run.sh check --start 2019-10-01 --end 2019-10-31   # 只对账，不构建

# 单张表（一张表一个进程，便于单表重试/定位；都经串行闸门排队）
./run.sh dwd --dt 2019-11-01                   # etl/dwd/dwd_event_fact.py
./run.sh dws --dt 2019-11-01                   # etl/dws/dws_user_behavior_daily.py
./run.sh dim --dt 2019-11-01                   # etl/dim/dim_user.py（增量）
./run.sh dim-scd2 --dt 2019-11-01              # etl/dim/dim_product_scd2.py（SCD2 增量 merge）
./run.sh ads --dt 2019-11-01                   # etl/ads/ads_trade_daily.py
./run.sh table etl/dim/dim_category.py --dt 2019-11-01   # 任意单表

# Airflow：airflow/dags/ 里已有指向本项目的软链接；
#         一表一个 task，任务间串行（max_active_tasks=1 + etl/run_locked.sh 的 flock 闸门）
airflow backfill create --dag-id incremental_warehouse_dag \
    --from-date 2019-11-01 --to-date 2019-11-30 --max-active-runs 1
```

脚本对应关系（**每一层一个文件夹、每一张表一个文件**，每张表都能单独跑）：

| 路径 | 职责 |
|---|---|
| `etl/ddl.py` | 全链路建表 + MSCK 注册分区（`--drop-tables` 单表重建） |
| `etl/pipeline.py` | 编排：一个进程一个 SparkSession 跑完一段区间 + 跨层对账 |
| `etl/run_locked.sh` | **串行闸门**：flock 保证同一时刻只有一个 SparkSession（跨 DAG、跨手动执行） |
| `etl/utils.py` | 共享工具：`get_spark` / `drop_path` / `date_range` / `hdfs_partition_count` |
| `etl/ods/ods_event_log.py` | ODS 单日导入（幂等跳过已存在分区） |
| `etl/ods/ods_event_log_bulk.py` | ODS 批量导入（单次扫描整份 CSV，切出全部日期分区） |
| `etl/ods/ods_event_log_dqc.py` | ODS 接入校验闸门（行数波动 / 非空率 / price 合法性） |
| `etl/dwd/dwd_event_fact.py` | ODS→DWD 事实表（去重 + 派生列，`--skip-existing` 断点续跑） |
| `etl/dwd/dwd_event_dirty.py` | 脏数据隔离表（拦下留痕，保证账对平） |
| `etl/dwd/dwd_event_fact_dqc.py` | DWD 质量闸门（账对平/唯一性/空值/枚举/负价/时区） |
| `etl/dwd/rules.py` | 本层共享清洗规则（两张表共用，保证账对平） |
| `etl/dws/dws_user_session_daily.py` | DWS 日×用户×会话 |
| `etl/dws/dws_product_daily.py` | DWS 日×商品（星型宽表，聚合不 JOIN 维度） |
| `etl/dws/dws_user_behavior_daily.py` | DWS 日×用户 |
| `etl/dws/dws_traffic_hour_daily.py` | DWS 日×小时（分时分析入口） |
| `etl/dim/dim_product_scd2.py` | 商品 SCD2 拉链（`--dt` 增量 merge / `--start --end` 全量重建） |
| `etl/dim/dim_user.py` | 用户维度（含时间属性；`--dt` 增量 / 全量重建） |
| `etl/dim/dim_session.py` | 会话维度 |
| `etl/dim/dim_category.py` | 品类维度 |
| `etl/dim/dim_date.py` | 日期维度（静态，不参与日调度） |
| `etl/dim/staging.py` | 本层共享增量写回助手（快照落 parquet，绕开自读自写守卫） |
| `etl/ads/ads_trade_daily.py` | 交易日报（GMV/订单/客单价/ARPPU） |
| `etl/ads/ads_conversion_funnel_daily.py` | 转化漏斗日报 |
| `etl/ads/ads_product_hot_rank_daily.py` | 商品热榜 Top100 |
| `etl/ads/ads_session_behavior_daily.py` | 会话行为日报 |
| `etl/ads/ads_traffic_hour_daily.py` | 分时大盘 |
| `etl/ads/ads_user_retention_daily.py` | 留存日报（区间型，右删失为 NULL） |
| `etl/ads/ads_user_rfm_snapshot.py` | RFM 八分群（全量快照，`as_of_dt` 分区） |

---

## 九、错误复盘（现象 → 根因 → 修复 → 教训）

### 🔴 A. 数据正确性类

#### A1. 留存表列序错位，实际值全串位（本次最严重的 bug）
**现象**：留存表打印出 `D+1 30393.0%` —— 留存率不可能超过 100%。
**根因**：`INSERT OVERWRITE TABLE t PARTITION (...)` **没写列名列表**，Spark 按**位置**映射。
我的 SELECT 输出顺序是 `d0_uv, d1_uv, d3_uv, d7_uv, d1_rate, d3_rate, d7_rate`，
而表 DDL 是 `d0_uv, d1_uv, d1_rate, d3_uv, d3_rate, d7_uv, d7_rate`。
于是 `d1_rate` 列里装进了 `d3_uv` 的**计数**（30,393），被 Spark 按目标列类型 cast 成 double → 显示成 `30393.0%`。
`d3_rate` 列里装的其实是真正的 D+1 率（17.6799%），看起来"像对的"，掩盖了错误。
**修复**：输出顺序改为与 DDL 一致（uv/rate 交替），**并加上显式列名列表**：
```sql
INSERT OVERWRITE TABLE ads.ads_user_retention_daily PARTITION (event_date='D')
(d0_uv, d1_uv, d1_rate, d3_uv, d3_rate, d7_uv, d7_rate)
SELECT ...
```
**教训**：① 位置映射的 INSERT 是静默错误——同类型列串位不会报错，只会算错；
② **同类型列串位比类型不匹配更危险**（类型不匹配至少会报错）；③ 从此这类 INSERT 一律写列名列表。
另外这条是**靠量纲自检发现的**：留存率 >100% 是业务上不可能的值，所以"打印关键指标的一个合理区间"值得做。

#### A2. SCD2 有 92,844 个商品没有当前版本
**现象**：拉链校验 `当前版本数 ≠1 的商品 92,844`。
**根因**：`dw_is_current` 误用「该版本末次出现日 = 统计窗口末日」判定。
**商品中途不再出现 ≠ 属性变更**（缺观测不等于变更）——那些 10-31 之前就不再出现的商品，全部被判成"非当前"。
**修复**：改为「每商品的**最后一个版本**即当前版本」，与窗口末日无关：
`ROW_NUMBER() OVER (PARTITION BY product_id ORDER BY dw_start_date DESC) = 1`。
**教训**：SCD2 的"当前"是**版本序列**的属性，不是"和统计窗口对齐"的属性。修完 166,794 个商品各一个当前版本，0 违规。

#### A3. 去重不确定，重跑结果会漂移
**现象**：改完 DWD 去重逻辑重跑 10-01，会话数从 268,702 变 268,700。
**根因**：去重键 `(user_id, event_time, product_id, event_type)` 的**所有列都参与分组**，
所以**组内 `event_time` 必然相同**，`ORDER BY event_time` 根本分不出胜负 → 保留哪条取决于扫描顺序。
而组内 `user_session / brand / price` 可能不同，于是下游会话数和类目快照跟着漂。
**修复**：排序追加全部剩余列做兜底（`event_time, category_id, category_code, brand, price, user_session`）。
**教训**：**幂等的前提是确定性**。窗口函数里 `ORDER BY` 有并列时，"重跑可重复"就是假的。

#### A4. DQC 打印的事件时间看着像串日
**现象**：2019-10-01 分区打印 `事件时间 2019-10-01 08:00:00 ~ 2019-10-02 07:59:59`，像混进了次日数据（+8 小时）。
**根因**：数据没错（时区一致性校验 0 错位可证）。是 py4j 把 Timestamp **按 Python 进程本地时区（+8）渲染**了。
**修复**：`DATE_FORMAT(MIN(event_time), 'yyyy-MM-dd HH:mm:ss')` 转成字符串再取，显式 UTC → 打印 `00:00:00 ~ 23:59:59`。
**教训**：质量报告里凡是**时间**都要声明时区；跨语言边界（JVM↔Python）的时间对象别直接打印。

### 🟡 B. 工程实现类

#### B1. driver 6g 把 JVM 撑爆，被内核 OOM killer 杀掉
**现象**：跑到第 10 天整个进程挂掉，`Py4JNetworkError: Answer from Java side is empty` → `ConnectionRefusedError`。
**根因**：机器只有 7.6G 内存，StarRocks FE（1.2G）+ HDFS NN/DN 常驻；我给了 `driver.memory=6g`。
`dmesg` 实锤：`Out of memory: Killed process (java) total-vm:25GB anon-rss:3.6GB`。
**修复**：driver 降到 3g（逐日任务是分区级小作业，用不到大堆）；批量任务拆层、各起独立进程；
并加了 `--skip-existing` 断点续跑——正是靠它从第 10 天接上的。
**教训**：driver 内存要按**机器可用内存**给，不是按"任务看起来大"给。
另外：Spark 任务"死在半路 + py4j 连接断开"要先看 `dmesg | grep -i oom`，别急着怀疑代码。
```bash
dmesg | grep -iE "oom-kill|Killed process"     # 一眼定位是不是被系统杀的
```

#### B2. `drop_path` 把外部表 LOCATION 删了，建表报 PATH_NOT_FOUND
**现象**：`AnalysisException: [PATH_NOT_FOUND] Path does not exist: .../dim/dim_product_scd2`。
**根因**：全量重建前我清空了 HDFS 目录，但**表还注册在 metastore 上**，Spark 建表时校验 LOCATION 存在。
**修复**：`drop_path()` 删完立刻 `mkdirs()` 补回空目录（`keep_dir=True`）。
**教训**：外部表的"清数据"和"删表"是两件事；清了目录但表还在，就是个半残状态。

#### B3. 批量脚本漏加 `--memory`，任务还没跑就退出了
**现象**：`ads_report.py: error: unrecognized arguments: --memory 3g`，进程秒退。
**根因**：我给 `dwd_clean / dws_aggregate / build_dims` 都加了 `--memory`，漏了 `ads_report`。
**修复**：补齐参数。**教训**：同一套 CLI 约定要一次铺全，漏一个就是"静默秒退"（日志里只有一行 usage，容易被当"跑完了"）。
**后续**：这些 `--memory` 参数后来按"统一用 `get_spark()` 的配置、不要在各脚本里另设内存"的要求**全部移除**了；
教训本身通用（CLI 约定要一次铺全），留着当记录。

#### B4. Spark SQL 中文别名没加反引号
**现象**：`[PARSE_SYNTAX_ERROR] Syntax error at or near '行'`。
**根因**：验证查询里写了 `SELECT COUNT(*) AS 行数` —— 未加反引号的中文标识符解析失败。
**修复**：验证查询统一用 ASCII 别名（或给中文加反引号）。**教训**：临时验证 SQL 也照生产标准写。

#### B5. 删了外部表的 HDFS 目录，metastore 里留下「幽灵分区」
**现象**：`ads_user_rfm_snapshot` 在 metastore 里注册了 `as_of_dt=2019-10-01` 和 `2019-10-31` 两个分区，
但 HDFS 上只有后者 —— 目录不存在而注册还在。
**根因**：我用 `utils.drop_path()` 删了整张表的 HDFS 目录，但这张表是**分区外部表**：
- 外部表的元数据在 metastore 里，**删 HDFS 目录不会删分区注册** → 留下"幽灵分区"；
- 后果是查这张表（不带分区条件）可能报 `[PATH_NOT_FOUND]`；而 `MSCK REPAIR` 只补不删，救不了。

这跟 `../legacy/PITFALLS.md` 里「DROP TABLE 不删文件」是**同一个问题的两个面**：
```
外部表：DROP TABLE  → 删注册、不删文件   → 残留文件被推断进新表（arity 不匹配）
外部表：删文件      → 删文件、不删注册   → 幽灵分区（路径不存在）
```
**结论：目录和注册必须一起动。**
**修复**：`etl/ads/ads_user_rfm_snapshot.py` 不再删目录，改为让 metastore 自己删分区：
```python
for row in spark.sql(f"SHOW PARTITIONS {TABLE}").collect():   # as_of_dt=2019-10-01
    k, _, v = row[0].partition("=")
    spark.sql(f"ALTER TABLE {TABLE} DROP PARTITION ({k}='{v}')")
```
（外部表 `DROP PARTITION` 只清元数据、不删数据文件；我们紧接着重写该分区。）
**验证**：重跑后 metastore 只剩 1 个分区、HDFS 只剩 1 个目录，**一一对应**；快照数字与修复前完全一致
（347,118 用户 / GMV 229,933,212.63）→ 幂等重建。
**教训**：操作外部表时，先把"元数据 / 文件"两件事分开想清楚要动哪个；**永远不要用 `rm -rf` 的方式去清一个在 metastore 里有注册的分区表**。

### 🟢 C. 数据口径/表述类

#### C1. 把"单日观测"当结论写进了文档
**现象**：文档里写"实测单个商品单日内最多出现 2 种组合"，随后全月一查最大值是 **3**。
**根因**：我只在 2019-10-01 这一天查过就下了结论。
**修复**：改成全量统计后的 3，并把"样本范围"写进句子。
**教训**：写进文档的**任何数字都要有全量口径的出处**，单日/抽样观测必须标注范围（这条同样是面试里"基准要有边界说明"的一部分）。

#### C2. `dim_session` 有 349 个会话被多用户复用，不是 0
**现象**：我原始的打印写"一对多用户会话 {multi}，应为 0"，实测是 **349**。
**根因**：不是 bug，是**源数据噪声**：`user_session` 理论上是 per-user UUID，源里存在极少量复用。
**修复**：不假装它是 0——打印改成如实汇报占比（0.0038%），并注明归属已按 `MAX(user_id)` 兜底归一。
**教训**：校验语句里的"应为 0"是**断言**，断言失败时要先判断"是数据问题还是代码问题"，别顺手把阈值调松。

---

## 十、常见误读澄清

**问：CSV 里有 datetime，为什么 HDFS 上没有？**
HDFS 上有。直接读 parquet 的 schema 就是 `event_time: timestamp`，值与 CSV 的 `2019-10-01 00:00:00 UTC` 一一对应，
DWD 还额外派生了 `event_hour`。三个容易看错的地方：

1. **分区目录名只到天**：`hdfs dfs -ls` 看到的是 `event_date=2019-10-01`，时间在**文件内的列**里，不在目录名上；
2. **DWS / ADS 确实没有时间**：它们是日粒度聚合（`dws_product_daily` 一行 = 某商品某天），明细时间戳在 GROUP BY 时就被聚合掉了——这是汇总层该有的样子；要看分时用 `event_hour` 或 §四 的分时表；
3. **`hdfs dfs -cat` 看 parquet 是二进制**，得用 Spark/Hive 读。

---

## 十一、后续可接

- **StarRocks 侧**：`dwd` 库里 5 张分层表已随 09-29 重置 DROP，`starrocks/` 下的建表/装载脚本同批被删，需要重建
  （PK 模型做 SCD2 UPSERT、DUPLICATE+RANGE 做明细/汇总、Stream Load label 幂等）。
- **扩到 11 月**：`./run.sh backfill --start 2019-11-01 --end 2019-11-30` 补 ODS 后，改 `config.DATA_START/DATA_END` 重跑即可。
- **DIM 增量**：当前按窗口全量重建拉链；可改为「当日 delta 与当前版本比对 → 关旧开新」的增量 merge。
- **洞察落地**：把 §四 的分时结论做成看板（上午主投放、晚间高客单低流量、下午高流量低转化）。
