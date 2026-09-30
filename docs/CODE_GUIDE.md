# 代码解析：架构与实现

> 本文讲清楚**代码怎么组织的、每一层在做什么、关键设计为什么这么做**。
> 想先跑起来看 [GETTING_STARTED.md](GETTING_STARTED.md)。

---

## 目录

1. [整体架构](#一整体架构)
2. [目录组织原则](#二目录组织原则)
3. [公共层：config + utils](#三公共层config--utils)
4. [ODS 层：接入](#四ods-层接入)
5. [DWD 层：清洗与质量闸门](#五dwd-层清洗与质量闸门)
6. [DIM 层：SCD2 拉链](#六dim-层scd2-拉链)
7. [DWS 层：汇总](#七dws-层汇总)
8. [ADS 层：应用](#八ads-层应用)
9. [编排与幂等](#九编排与幂等)
10. [调度](#十调度)
11. [关键设计决策](#十一关键设计决策)

---

## 一、整体架构

```
                    ┌──── 源数据 CSV（HDFS）────┐
                    ▼
        ┌───────────────────────┐
        │   ODS   ods_event_log │  零修改，只按天分区
        └───────────┬───────────┘
                    ▼  清洗 / 去重 / 派生
        ┌───────────────────────┐
        │   DWD   dwd_event_fact│  42,413,557 行 × 13 列
        │         dwd_event_dirty│  脏数据隔离
        └───────────┬───────────┘
                    │  (DQC 质量闸门在这里卡住)
        ┌───────────┴───────────┐
        ▼                       ▼
┌───────────────┐      ┌───────────────┐
│ DIM (5 张)     │      │ DWS (4 张)     │
│ 商品SCD2拉链   │      │ 日×会话/商品   │
│ 用户/会话/     │      │ 用户/小时      │
│ 类目/日期      │      │               │
└───────┬───────┘      └───────┬───────┘
        └──────────┬───────────┘
                   ▼
        ┌───────────────────────┐
        │   ADS (7 张)           │  交易/漏斗/热榜/会话/留存/分时/RFM
        └───────────────────────┘
```

**数据量级**（2019-10 整月）：

| 层 | 表数 | 代表规模 |
|---|---|---|
| ODS | 1 | 47,017,436 行 |
| DWD | 2 | 42,413,557 行 |
| DIM | 5 | 172,794 版本（166,794 商品） |
| DWS | 4 | 9,280,709 行（最大表） |
| ADS | 7 | 347,118（RFM） |

---

## 二、目录组织原则

**核心原则：一层一个文件夹，一表一个文件。**

```
etl/
├── ddl.py            # 建表
├── pipeline.py       # 编排 + 对账
├── utils.py          # 共享工具
├── run_locked.sh     # 串行闸门
├── ods/  3 个文件
├── dwd/  4 个文件
├── dim/  6 个文件
├── dws/  4 个文件
└── ads/  7 个文件
```

**为什么一表一文件**：

1. **单表可重试** —— 某张表跑挂了，只重跑那一张，不用整条链路重来
2. **单表可定位** —— 出问题直接看对应文件，不用在几百行的巨型脚本里找
3. **依赖显式** —— `pipeline.py` 的 `STEPS` 列表就是依赖顺序，一眼看清

对比：旧实现把多张表挤在一个脚本（`build_dims.py`、`dws_aggregate.py`），
改一张表要重跑全部，且没法单表重试。本次重构的第一件事就是拆开。

**每个文件都是同一个骨架**：

```python
def build(spark, dt, ...):        # 核心逻辑：接收 SparkSession，处理一天
    ...

def main():                        # CLI 入口：解析参数，循环日期
    ...

if __name__ == "__main__":
    main()
```

`build(spark, dt)` 这个签名很关键 —— 它让 `pipeline.py` 能**复用同一个 SparkSession**
顺序调用所有表，省掉每天十几次会话启动开销；同时单文件也能独立跑。

---

## 三、公共层：config + utils

### `config/config.py` —— 唯一配置源

集中管理四类东西：

```python
# 1. HDFS 路径
HDFS_ROOT = "hdfs://localhost:8020"
WAREHOUSE = f"{HDFS_ROOT}/home/lst/hadoop-data/warehouse"
ODS_PATH  = f"{WAREHOUSE}/ods_event_log"
DWS = {"user_session": f"{WAREHOUSE}/dws_user_session_daily", ...}
ADS = {"funnel": f"{WAREHOUSE}/ads/ads_conversion_funnel_daily", ...}

# 2. 表结构（Spark Schema）
SRC_SCHEMA = StructType([StructField("event_time", TimestampType(), True), ...])

# 3. 业务口径常量
PARTITION_COL      = "event_date"
VALID_EVENT_TYPES  = ["view", "cart", "remove_from_cart", "purchase"]
DEDUP_KEYS         = ["user_id", "event_time", "product_id", "event_type"]
BEHAVIOR_CN        = {"view": "浏览", "cart": "加购", ...}
FACT_COLUMNS       = [...]              # 事实表 13 列口径

# 4. 连接配置（读环境变量）
MYSQL = {"password": os.environ.get("DW_MYSQL_PASSWORD", ""), ...}
```

**为什么集中**：口径一致性。`DEDUP_KEYS` 一改，ODS/DWD/所有下游都跟着变，
不会出现「某个脚本写死了一套旧口径」的情况。

### `etl/utils.py` —— 共享工具

四个函数，每个都对应一个踩过的坑：

#### `get_spark()` —— 统一 SparkSession

```python
def get_spark(app_name="WarehouseTask", memory="4g", shuffle_partitions=100):
    return (SparkSession.builder
        .master("local[6]")
        .config("spark.driver.memory", memory)
        .config("spark.hadoop.fs.defaultFS", HDFS_ROOT)
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.sql.session.timeZone", "UTC")                        # ★ 关键
        .config("spark.sql.sources.partitionOverwriteMode", "dynamic")      # ★ 关键
        .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
        .getOrCreate())
```

两个 `★` 配置是**正确性的前提**，不是优化项：

- **`timeZone=UTC`**：源时间戳是 UTC。不锁死的话，在 +8 时区下 `to_date`
  会把每天 16:00–23:59 UTC 的记录挪到次日分区 → 按日分区全错。
- **`partitionOverwriteMode=dynamic`**：动态分区覆盖。默认的 `overwrite` 会
  **先删整个输出目录再写**，重跑一天会清掉全部历史分区。改成 dynamic 后
  只覆盖本次写入涉及的分区。

#### `drop_path()` —— 清目录但保留空目录

```python
def drop_path(spark, path, keep_dir=True):
    fs.delete(jpath, True)      # 清空内容
    if keep_dir:
        fs.mkdirs(jpath)        # ★ 必须补回空目录
```

**为什么要补回**：外部表 `DROP TABLE` 不删 HDFS 文件，所以重建前必须手动清目录
（否则旧残留目录会被 Spark 推断进新表，导致 `INSERT_COLUMN_ARITY_MISMATCH`）。
但表还注册在 metastore 上，LOCATION 不存在会直接报 `PATH_NOT_FOUND` —— 所以删完立刻补空目录。

#### `date_range()` / `hdfs_partition_count()`

前者生成闭区间日期列表，后者统计分区行数或文件数（`row=False` 时只数文件，用于轻量存在性判断）。

---

## 四、ODS 层：接入

### `etl/ods/ods_event_log.py` —— 单日导入

```python
def build(spark, dt):
    # 幂等：分区已存在则跳过
    if hdfs_partition_count(spark, f"{ODS_PATH}/event_date={dt}", row=False) > 0:
        return None

    df = (spark.read
          .option("header", True)
          .schema(SRC_SCHEMA)
          .csv(SRC_CSV))

    # 按事件 UTC 日期切分区
    df.withColumn("event_date", F.to_date("event_time")) \
      .write.mode("overwrite").partitionBy("event_date") \
      .parquet(ODS_PATH)
```

**ODS 的职责边界**：只做「格式转换 + 分区」，**不做任何清洗**。
脏数据原样保留 —— 清洗是 DWD 的事。这样 ODS 可以随时重放，DWD 口径改了也不用重导。

### `etl/ods/ods_event_log_bulk.py` —— 批量导入

与单日版的区别：**单次扫描整份 CSV，一次切出全部日期分区**。

```python
df.write.mode("overwrite").partitionBy("event_date").parquet(ODS_PATH)
```

**为什么需要它**：逐日导入要把 9GB 的 CSV 扫 31 遍，批量版只扫 1 遍。
（配 `partitionOverwriteMode=dynamic`，只会重写有数据的那些分区）

### `etl/ods/ods_event_log_dqc.py` —— 接入校验闸门

检查三项，异常就阻断：

- 行数波动（对比 7 日均值，超阈值告警/阻断）
- 核心字段非空率
- `price` 合法性

**闸门放在 ODS 之后**：源数据有问题时，在入口就挡住，不让脏数据流进 DWD 再往回追。

---

## 五、DWD 层：清洗与质量闸门

这一层是三张表 + 一套共享规则。

### `etl/dwd/rules.py` —— 共享清洗规则

```python
# 脏数据打标：优先级 null_key > invalid_event_type > negative_price
DIRTY_CLASSIFY_SQL = """
    CASE
        WHEN event_time IS NULL OR user_id IS NULL OR product_id IS NULL THEN 'null_key'
        WHEN event_type IS NULL OR event_type NOT IN (...) THEN 'invalid_event_type'
        WHEN price < 0 THEN 'negative_price'
    END
"""

# 中文映射：展开成 CASE WHEN，避免 UDF 序列化开销
BEHAVIOR_CASE_SQL = "CASE event_type WHEN 'view' THEN '浏览' ... END"
```

**为什么两张表共用规则**：干净表和脏数据表必须用**同一套判定**，
否则「干净 + 脏 + 去重 = ODS」这条账目对不平。

**为什么用 `CASE WHEN` 而不是 UDF**：UDF 需要 Python ↔ JVM 序列化，每行都有开销；
`CASE WHEN` 是原生表达式，走向量化执行。

### `etl/dwd/dwd_event_fact.py` —— 事实表

处理流程：

```
读 ODS 分区
  → 打脏数据标签（rules.py 的 DIRTY_CLASSIFY_SQL）
      → 过滤出干净行（dirty_reason IS NULL）
          → 按去重键去重（含确定性兜底排序）
              → 加派生列
                  → INSERT OVERWRITE 静态分区
```

**六个清洗动作**（对应 docstring §1–6）：

| # | 动作 | 规则 |
|---|---|---|
| 1 | 空值过滤 | `event_time` / `user_id` / `product_id` 任一为空 → 剔除 |
| 2 | 枚举过滤 | `event_type` 不在 4 类枚举 → 剔除 |
| 3 | 异常值过滤 | `price < 0` → 剔除 |
| 4 | 去重 | 同 `(user_id, event_time, product_id, event_type)` 只留一条 |
| 5 | 时间标准化 | 拆出 `event_date` 分区列 + `event_hour` |
| 6 | 口径衍生 | `behavior_type`(中文) / `is_purchase`(0/1) |

**去重的确定性**（本层最微妙的地方）：

```python
dedup_keys = ", ".join(DEDUP_KEYS)
tiebreak = ", ".join(["event_time"] + [c for c in FACT_COLUMNS
                                       if c not in DEDUP_KEYS + ["behavior_type", "is_purchase", "event_hour"]])

ROW_NUMBER() OVER (PARTITION BY {dedup_keys} ORDER BY {tiebreak})
```

**问题**：去重键的**所有列都参与分组**，所以组内 `event_time` 必然相同 ——
只按 `ORDER BY event_time` 根本分不出胜负，保留哪条取决于扫描顺序。

**后果**：重跑同一天，会话数会漂（实测 268,702 → 268,700），类目快照跟着变。

**解法**：把**全部剩余列**追加进 `ORDER BY` 做兜底，保证同一输入必得同一输出。

> **幂等的前提是确定性。** 这是本项目最有价值的一条经验。

**落表用静态分区**：

```sql
INSERT OVERWRITE TABLE dwd.dwd_event_fact PARTITION (event_date = '{dt}')
```

把「只重写这一天」写死成 SQL 语义，重跑其他分区完全不动。

### `etl/dwd/dwd_event_dirty.py` —— 脏数据隔离

用同一套规则，但**取脏的那部分**（`dirty_reason IS NOT NULL`），落到隔离表留痕。

**为什么不直接丢弃脏数据**：留着能审计。而且有了隔离表，
`ODS 行数 = DWD 行数 + 去重行数 + 脏数据行数` 这条账目才能逐日对平。

> 本项目实测 `dwd_event_dirty` 是 **0 行** —— 这不是漏跑，而是源数据本身干净
> （无空键、无非法枚举、无负价格）。隔离表和规则都留着，脏数据一出现就会被拦下。

### `etl/dwd/dwd_event_fact_dqc.py` —— 质量闸门

六项检查，逐日跑，**失败即阻断 Airflow 下游**：

1. 去重键唯一性
2. 核心字段空值
3. 枚举越界
4. `price` 负值
5. **分区与事件 UTC 日期一致性**（防止时区错误导致串分区）
6. 账目对平（ODS = 干净 + 脏 + 去重）

---

## 六、DIM 层：SCD2 拉链

这是本项目技术含量最高的一层。

### `etl/dim/dim_product_scd2.py` —— 商品类型2拉链

**难点**：源数据里**没有商品主数据表**，商品属性只能从事实表反推。

**算法四步**：

```sql
WITH daily AS (          -- ① 日 × 商品 × 属性组合 的出现次数
    SELECT event_date, product_id, category_id, category_code, brand, COUNT(*) cnt
    FROM fact WHERE event_date BETWEEN ... GROUP BY 1,2,3,4,5
),
dominant AS (            -- ② 当日主导组合（同票按 category_id 兜底 → 确定性）
    SELECT ..., ROW_NUMBER() OVER (PARTITION BY event_date, product_id
                                   ORDER BY cnt DESC, category_id) rn
    FROM daily
),
marked AS (              -- ③ 与前一版本比对，标记版本起点
    SELECT *,
           LAG(combo) OVER (PARTITION BY product_id ORDER BY event_date) AS prev_combo
    FROM dominant WHERE rn = 1
),
versioned AS (           -- 累积求和 → 版本号
    SELECT *, SUM(CASE WHEN prev_combo IS NULL OR combo <> prev_combo THEN 1 ELSE 0 END)
              OVER (PARTITION BY product_id ORDER BY event_date
                    ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS version_no
    FROM marked
),
spans AS (               -- ④ 同版本连续日期折叠成区间
    SELECT product_id, ..., MIN(event_date) dw_start_date, MAX(event_date) last_seen
    FROM versioned GROUP BY product_id, version_no, ...
)
-- 末版本开放：dw_end_date = 9999-12-31, dw_is_current = 1
-- 其余闭链：dw_end_date = last_seen + 1（= 下一版本起始日，无重叠无间隙）
```

**关键设计点**：

**① 为什么要「日主导组合」**：单日噪声。实测一个商品一天最多出现 3 种属性组合，
直接拿原始记录会得到大量假版本。取当日出现次数最多的组合才稳定。
同票时按 `category_id` 兜底排序，保证结果确定。

**② 开链/闭链规则**：
- 每商品**最后一个版本**永远开放（`dw_end_date = 9999-12-31`，`dw_is_current = 1`）
- 其余版本闭链：`dw_end_date = last_seen + 1`，即下一版本起始日 → 区间**无重叠、无间隙**

**③ 「当前版本」的判定**：

```sql
ROW_NUMBER() OVER (PARTITION BY product_id ORDER BY dw_start_date DESC) = 1
```

**不是**「末次出现日 = 统计窗口末日」。

> 踩过的坑：曾用后者判定，结果 92,844 个商品被判成「没有当前版本」。
> 根因是**商品中途不再出现 ≠ 属性变更**（缺观测不等于变更）——
> 那些 10-31 之前就不再出现的商品全被判成非当前。
> **SCD2 的「当前」是版本序列的属性，不是与统计窗口对齐的属性。**

**④ 代理键**：`dim_product_sk` 按 `(product_id, dw_start_date)` 全局编号，
与业务键解耦。

**结果**：166,794 个商品 → 172,794 个版本，其中 **6,000 个商品在 10 月发生过属性变更**。
校验：当前版本数 ≠ 1 的商品 **0** 个，区间重叠 **0** 对。

**两种运行方式**：

```bash
--dt D                增量：当日 delta 与当前版本比对 → 关旧版 + 开新版（不回扫历史）
--start D --end D     全量重建：按窗口重算整张拉链表
```

### `etl/dim/staging.py` —— 增量写回助手

**解决的问题**：Spark 不允许「读一张表的同时写这张表」（自读自写守卫）。

**做法**：先把结果快照落成 parquet，再从 parquet 读回来写目标表，绕开守卫。

### 其他四张维表

| 表 | 类型 | 说明 |
|---|---|---|
| `dim_user` | 全量 + 时间属性 | 挂 `first_seen_date` / `last_seen_date` / `active_days` / `first_purchase_date` / `purchase_days`，支撑新老客与生命周期分析 |
| `dim_session` | 全量 | 会话维度（会话本身在事实表里也是退化维度） |
| `dim_category` | 全量 | 类目维度，624 行 |
| `dim_date` | 静态 | 365 行（2019 全年），**不参与日调度** |

---

## 七、DWS 层：汇总

四张表，粒度不同：

| 表 | 粒度 | 行数（31 天） |
|---|---|---|
| `dws_user_session_daily` | 日 × 用户 × 会话 | 9,280,709 |
| `dws_product_daily` | 日 × 商品 | 2,299,245 |
| `dws_user_behavior_daily` | 日 × 用户 | 6,473,723 |
| `dws_traffic_hour_daily` | 日 × 小时 | 744（31×24） |

**核心设计：DWS 聚合不 JOIN 维度。**

```python
df.groupBy("product_id", "category_id", "brand") \
  .agg(F.countDistinct(...).alias("exposure_uv"), ...)     # 直接聚合，不 JOIN dim
```

**为什么能不 JOIN**：类目 / 品牌已经在 DWD 事实表里**退化成普通列**了
（`category_id` / `category_code` / `brand` 直接进事实表 13 列）。

**这是星型模型的核心收益** —— 省掉一次 JOIN，列存下直接扫列聚合。

**分层本身就是倾斜预防**：`日×商品` 这样的大基数聚合，在 DWS 层就完成，
不必等到 ADS 层面对全量明细。

---

## 八、ADS 层：应用

七张表，面向具体分析场景：

| 表 | 粒度 | 关键指标 |
|---|---|---|
| `ads_trade_daily` | 日 | GMV / 订单数 / 购买用户 / 客单价 / ARPU |
| `ads_conversion_funnel_daily` | 日 | 浏览/加购/购买 UV + 转化率 |
| `ads_product_hot_rank_daily` | 日 × Top100 | 商品热榜（按销售额） |
| `ads_session_behavior_daily` | 日 | 会话数 / 平均时长 / 加购转化率 |
| `ads_traffic_hour_daily` | 日 × 小时 | 分时大盘（含峰值标记） |
| `ads_user_retention_daily` | 日 | D+1 / D+3 / D+7 留存 |
| `ads_user_rfm_snapshot` | 快照 | RFM 八分群（`as_of_dt` 分区） |

### 两个特殊设计

**① 漏斗回 DWD，不走 DWS**

```python
# ads_conversion_funnel_daily.py：直接从 DWD 算
```

**为什么**：漏斗要**跨商品精确去重 UV**。DWS 是商品级的，
拿商品级 UV 累加会重复计数（同一用户浏览多个商品）。

**② 留存右删失记 NULL，不记 0**

```sql
-- 数据没到那天 → NULL（右删失），而不是 0
```

**为什么**：月末最后 7 天的 D+7 是「数据还没到」，不是「留存归零」。
混为一谈会让报表显示虚假的留存暴跌。本项目实测有 **7 天** D+7 为 NULL。

**业务洞察**：
- **RFM 二八法则显著**：「重要价值客户」占购买用户 **12.2%**，贡献 **55.6%** GMV
- **留存**（基准日 10-16，D0=230,199）：D+1 **17.68%** / D+3 **13.20%** / D+7 **10.74%**
- **分时峰谷比 13.76×**：上午(7-12) 转化最高（贡献 41.04% 订单），晚间(19-23) 客单价最高（354 元）

---

## 九、编排与幂等

### `etl/pipeline.py` —— 全链路编排

```python
STEPS = [
    ("dwd_event_fact",       "dwd", dwd_event_fact.build),
    ("dwd_event_dirty",      "dwd", dwd_event_dirty.build),
    ("dws_user_session",     "dws", dws_user_session_daily.build),
    ...  # 顺序即依赖顺序
]

spark = get_spark("Pipeline")           # ★ 一个进程一个会话
for i, (name, layer, fn) in enumerate(wanted, 1):
    for d in days:
        fn(spark, d)                    # 复用同一会话
run_checks(spark, start, end)           # 自动跨层对账
```

**`run_checks()` 检查 6 项不变量**：

| 检查 | 含义 |
|---|---|
| 购买次数守恒 | DWD = DWS用户 = DWS商品 = DWS会话 = ADS |
| GMV 守恒 | DWD = DWS用户 = DWS商品 = ADS |
| 漏斗不变量 | 每日 `view_uv >= cart_uv` 且 `view_uv >= purchase_uv` |
| SCD2 单当前版本 | 每商品恰好一个 `dw_is_current=1` |
| 分时守恒 | DWS = ADS = DWD 明细 PV |
| 留存不变量 | D+n 人数 ≤ D0 人数 |

**为什么做对账而不是只看任务成功**：任务成功只说明「跑完了」，
不说明「算对了」。跨层守恒是**业务语义层面的正确性证明**。

### 幂等的四道保险

| 机制 | 实现 |
|---|---|
| 分区级覆盖 | 静态分区 `INSERT OVERWRITE TABLE t PARTITION (event_date='D')` |
| 动态覆盖兜底 | `spark.sql.sources.partitionOverwriteMode=dynamic` |
| 时区固定 | `spark.sql.session.timeZone=UTC` |
| **确定性去重** | 排序追加全部剩余列做兜底 |
| 断点续跑 | `--skip-existing`：分区已有文件则跳过 |
| 重建前清目录 | `drop_path()`：清除旧残留，但保留空目录 |

**「重跑可重复」的完整依赖链**：分区粒度 = 业务粒度（按天） **且** 写入语义是覆盖非追加
**且** 计算过程确定（去重不能有并列）。三条缺一不可。

### `etl/run_locked.sh` —— 串行闸门

```bash
exec flock -w "$WAIT" "$LOCK" "$@"
```

**为什么需要**：7.6G 内存的机器上，两个 Spark 作业并发会 OOM。
闸门保证同一时刻只有一个 SparkSession（跨 DAG、跨手动执行都生效）。
抢不到锁的排队等待（默认上限 7200 秒）。

> **不要绕过 `run.sh` 直接 `python xxx.py`**，否则闸门失效。

---

## 十、调度

### `scheduler/incremental_warehouse_dag.py`

一表一个 task，依赖链见 [GETTING_STARTED.md §九](GETTING_STARTED.md#九可选airflow-调度)。

**关键配置**：`max_active_tasks=1` —— 任务间串行，配合 flock 双保险。

**必须显式传 env（踩过的坑）**：

```python
# Airflow BashOperator.get_env() 的逻辑是：
#   env = self.env
#   if env is None: env = os.environ.copy()
#   elif self.append_env: system_env.update(env); env = system_env
```

**默认 `append_env=False`** —— 一旦传了 `env`，任务进程就**只拿到这个字典**，
`.bashrc` 里的 `SPARK_HOME` / `HADOOP_CONF_DIR` 全丢。

后果：PySpark 的 `_find_spark_home()` 回退到 venv 里的 pyspark 包目录
（那里没有 `conf/spark-defaults.conf`）→ 会话不挂 metastore →
报 `TABLE_OR_VIEW_NOT_FOUND: dwd.dwd_event_fact`。

**所以 DAG 里必须把 `JAVA_HOME` / `SPARK_HOME` / `HADOOP_CONF_DIR` 显式写进 env。**
（这也解释了当时「只有 DWD 挂」的现象 —— ODS 两步只读 HDFS 路径、不碰 metastore，所以不报错。）

### `scheduler/alerts.py`

见 [GETTING_STARTED.md §九](GETTING_STARTED.md#九可选airflow-调度) 与
[incremental/ALERTS.md](incremental/ALERTS.md)。

---

## 十一、关键设计决策

汇总本项目最重要的几个取舍：

### 1. 退化维度（不建表，直接进事实表）

`event_type` / `behavior_type` / `is_purchase` / `event_hour` / `user_session`
直接作为事实表的列，不做维表关联。

**判断框架（三问）**：
1. **有没有自己的属性？** 无 → 退化（`event_type` 只有 4 个值，建表纯属多余）
2. **基数大不大、要不要 JOIN？** 小枚举 → 退化
3. **要不要保留历史版本？** 不要 → 退化

**收益**：DWS 聚合免 JOIN，列存下 `GROUP BY event_type` 零开销。

### 2. 星型模型而非雪花

维度扁平、直接挂事实表，不向下拆层级。

**收益**：查询/预聚合快（JOIN 少）、ETL 与血缘简单、冗余成本可接受
（列存 + snappy 下，冗余几个低基数列的代价远小于每次查询多 JOIN）。

### 3. 加时分维度（但只加到 DWS/ADS，不加到 ODS/DWD）

**先看数据有没有信号**：跨 31 天按小时聚合，发现峰谷比 **13.76×**，
且三个信号错位（16 点 PV 峰值、9 点订单峰值、21 点客单价峰值）。

**结论：加在这三处**
1. DWS 加 `dws_traffic_hour_daily`（粒度 `日×小时`）
   —— `event_hour` 已在事实表，一次 GROUP BY 即得，**1.37M 行/天 压到 24 行/天**
2. ADS 加 `ads_traffic_hour_daily`（加报表语义：时段分桶、占比、峰值标记）
3. `dim_user` 挂时间属性（支撑新老客与生命周期）

**结论：不加在这两处**
- **ODS/DWD 加 hour 分区**：判据是**分区粒度要匹配「到达粒度」和「重跑粒度」**。
  数据是**按天批量**落的（不是小时流），DWD 重跑一天只要 45 秒；
  1.37M 行/天 ÷ 24 ≈ 57k 行、10MB 一个分区，加了只是把 31 个目录变成 744 个碎分区，
  小文件问题 + 扫得更慢 + 还要重导 9GB。
- **丢掉 `event_time` 只留 `event_hour`**：会话时长、行为先后、漏斗路径全靠 `event_time`。
  `event_hour` 是便利列，不是替代品。

### 4. 脏数据隔离而非静默丢弃

脏行落 `dwd_event_dirty` 留痕，保证「干净 + 脏 + 去重 = ODS」账目对得平，且可审计。

### 5. 跨层对账作为交付标准

不看「任务是否成功」，看「业务不变量是否成立」。

### 6. 时间精度到天的取舍（SCD2 的天花板）

拉链需要「变更时刻」，但源数据只有**观测时刻**（用户看到这个商品是什么属性），
没有**变更时刻**（商品何时被改的）。所以 `dw_start_date` 只能到天。

**这是数据源的边界，不是实现偷懒。** 面试时应主动说明这个限制。

---

## 下一步

- 想知道**怎么跑起来** → [GETTING_STARTED.md](GETTING_STARTED.md)
- 想知道**踩了哪些坑** → [incremental/PITFALLS.md](incremental/PITFALLS.md)（25 个）
- 想知道**数据字典与实测数字** → [incremental/WAREHOUSE_BUILD.md](incremental/WAREHOUSE_BUILD.md)
- 想知道**面试怎么讲** → [../interview/INTERVIEW_ANSWERS.md](../interview/INTERVIEW_ANSWERS.md)
