# 代码解析：架构与实现

> 本文说明代码的组织方式、各层职责与关键设计取舍。
> 运行方式见 [GETTING_STARTED.md](GETTING_STARTED.md)。

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

数据量级（2019-10 整月）：

| 层 | 表数 | 代表规模 |
|---|---|---|
| ODS | 1 | 47,017,436 行 |
| DWD | 2 | 42,413,557 行 |
| DIM | 5 | 172,794 版本（166,794 商品） |
| DWS | 4 | 9,280,709 行（最大表） |
| ADS | 7 | 347,118（RFM） |

---

## 二、目录组织原则

目录按层划分，一层一个文件夹，一表一个文件。

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

一表一文件的理由：

1. 单表可重试：某张表失败时只重跑该表，不必重跑整条链路
2. 单表可定位：问题可直接定位到对应文件，不必在大型脚本中查找
3. 依赖显式：`pipeline.py` 的 `STEPS` 列表即依赖顺序

旧实现把多张表放在同一个脚本（`build_dims.py`、`dws_aggregate.py`）中，
修改一张表需要重跑全部，也无法单表重试。

每个文件的骨架相同：

```python
def build(spark, dt, ...):        # 核心逻辑：接收 SparkSession，处理一天
    ...

def main():                        # CLI 入口：解析参数，循环日期
    ...

if __name__ == "__main__":
    main()
```

`build(spark, dt)` 的签名使 `pipeline.py` 能复用同一个 SparkSession 顺序调用所有表，
省去每天十几次会话启动开销；单个文件也能独立运行。

---

## 三、公共层：config + utils

### `config/config.py` —— 唯一配置源

集中管理四类配置：

```python
# 1. HDFS 路径
HDFS_ROOT = "hdfs://localhost:8020"
WAREHOUSE = f"{HDFS_ROOT}/home/warehouse/hadoop-data/warehouse"
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

集中配置保证口径一致：修改 `DEDUP_KEYS` 后 ODS/DWD 及所有下游同步生效，
不会出现某个脚本写死旧口径的情况。

### `etl/utils.py` —— 共享工具

四个函数，各自对应一条实现约束：

#### `get_spark()` —— 统一 SparkSession

```python
def get_spark(app_name="WarehouseTask", memory=None, shuffle_partitions=100):
    memory = memory or os.environ.get("SPARK_DRIVER_MEMORY", "1g")
    return (SparkSession.builder
        .master(os.environ.get("SPARK_MASTER", "spark://localhost:7077"))
        .config("spark.driver.memory", memory)
        .config("spark.executor.memory", os.environ.get("SPARK_EXECUTOR_MEMORY", "1g"))
        .config("spark.executor.cores", os.environ.get("SPARK_EXECUTOR_CORES", "2"))
        .config("spark.executor.instances", os.environ.get("SPARK_EXECUTOR_INSTANCES", "4"))
        .config("spark.hadoop.fs.defaultFS", HDFS_ROOT)
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.sql.session.timeZone", "UTC")                        # 关键
        .config("spark.sql.sources.partitionOverwriteMode", "dynamic")      # 关键
        .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
        .getOrCreate())
```

默认连 standalone 集群；`SPARK_MASTER=local[6]` 可切回本地模式。

其中两项配置关系到正确性，不是可选优化：

- `timeZone=UTC`：源时间戳为 UTC。若不固定时区，在 +8 时区下 `to_date`
  会把每天 16:00–23:59 UTC 的记录划到次日分区，导致按日分区错误。
- `partitionOverwriteMode=dynamic`：动态分区覆盖。默认的 `overwrite` 会
  先删除整个输出目录再写入，重跑一天会清掉全部历史分区；`dynamic` 只覆盖
  本次写入涉及的分区。

#### `drop_path()` —— 清目录但保留空目录

```python
def drop_path(spark, path, keep_dir=True):
    fs.delete(jpath, True)      # 清空内容
    if keep_dir:
        fs.mkdirs(jpath)        # 必须补回空目录
```

补回空目录的原因：外部表 `DROP TABLE` 不删除 HDFS 文件，因此重建前必须手动清理目录，
否则旧的残留目录会被 Spark 推断进新表，导致 `INSERT_COLUMN_ARITY_MISMATCH`。
但表仍注册在 metastore 上，LOCATION 不存在会直接报 `PATH_NOT_FOUND`，
所以删除后需要立即补回空目录。

#### `date_range()` / `hdfs_partition_count()`

前者生成闭区间日期列表；后者统计分区行数或文件数（`row=False` 时只统计文件数，用于轻量存在性判断）。

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

ODS 只负责格式转换与分区，不做任何清洗，脏数据原样保留，清洗由 DWD 完成。
因此 ODS 可以随时重放，DWD 口径修改时也不需要重新导入。

### `etl/ods/ods_event_log_bulk.py` —— 批量导入

与单日版的区别是一次扫描整份 CSV，切出全部日期分区。

```python
df.write.mode("overwrite").partitionBy("event_date").parquet(ODS_PATH)
```

逐日导入需要扫描 9GB 的 CSV 31 遍，批量版只扫 1 遍。
配合 `partitionOverwriteMode=dynamic`，只会重写有数据的分区。

### `etl/ods/ods_event_log_dqc.py` —— 接入校验闸门

检查三项，异常时告警或阻断：

- 行数波动（对比 7 日均值，超阈值告警/阻断）
- 核心字段非空率
- `price` 合法性

闸门位于 ODS 之后：源数据异常在入口拦截，避免脏数据流入 DWD 后再回溯。

---

## 五、DWD 层：清洗与质量闸门

本层包含三张表和一套共享规则。

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

两张表共用规则，是为了保证干净表和脏数据表使用同一套判定；
否则「干净 + 脏 + 去重 = ODS」这条账目无法对平。

使用 `CASE WHEN` 而非 UDF：UDF 需要 Python 与 JVM 之间的序列化，逐行产生开销；
`CASE WHEN` 是原生表达式，可向量化执行。

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

六个清洗动作（对应 docstring §1–6）：

| # | 动作 | 规则 |
|---|---|---|
| 1 | 空值过滤 | `event_time` / `user_id` / `product_id` 任一为空 → 剔除 |
| 2 | 枚举过滤 | `event_type` 不在 4 类枚举 → 剔除 |
| 3 | 异常值过滤 | `price < 0` → 剔除 |
| 4 | 去重 | 同 `(user_id, event_time, product_id, event_type)` 只留一条 |
| 5 | 时间标准化 | 拆出 `event_date` 分区列 + `event_hour` |
| 6 | 口径衍生 | `behavior_type`(中文) / `is_purchase`(0/1) |

去重的确定性：

```python
dedup_keys = ", ".join(DEDUP_KEYS)
tiebreak = ", ".join(["event_time"] + [c for c in FACT_COLUMNS
                                       if c not in DEDUP_KEYS + ["behavior_type", "is_purchase", "event_hour"]])

ROW_NUMBER() OVER (PARTITION BY {dedup_keys} ORDER BY {tiebreak})
```

去重键的所有列都参与分组，因此组内 `event_time` 必然相同，
只按 `ORDER BY event_time` 无法区分，保留哪条取决于扫描顺序。

后果是重跑同一天时会话数发生漂移（实测 268,702 → 268,700），类目快照也随之变化。

解法是把全部剩余列追加进 `ORDER BY` 作为兜底，保证同一输入产生同一输出。

> 幂等的前提是确定性。

落表使用静态分区：

```sql
INSERT OVERWRITE TABLE dwd.dwd_event_fact PARTITION (event_date = '{dt}')
```

静态分区把「只重写这一天」固化在 SQL 语义中，重跑其他分区不受影响。

### `etl/dwd/dwd_event_dirty.py` —— 脏数据隔离

使用同一套规则，取脏的那部分（`dirty_reason IS NOT NULL`），写入隔离表留痕。

保留脏数据便于审计；有了隔离表，`ODS 行数 = DWD 行数 + 去重行数 + 脏数据行数`
才能逐日对平。

> 实测 `dwd_event_dirty` 为 0 行，原因是源数据本身没有空键、非法枚举和负价格，
> 不是漏跑。隔离表和规则保留，出现脏数据时仍会拦截。

### `etl/dwd/dwd_event_fact_dqc.py` —— 质量闸门

七项检查逐日运行，失败即阻断 Airflow 下游：

| # | 检查 | 失败码 |
|---|---|---|
| 1 | 账对平（DWD + 脏 ≤ ODS，多了说明写重） | `count_overflow` |
| 2 | 分区非空 | `empty_partition` |
| 3 | 去重键唯一性 | `dup_key` |
| 4 | 核心字段空值 | `null_core_field` |
| 5 | 枚举越界 | `bad_event_type` |
| 6 | `price` 负值 | `negative_price` |
| 7 | 分区与事件 UTC 日期一致性（防止时区错误导致串分区） | `timezone_offset` |

判定逻辑集中在一条 SQL 中：每个检查项通过 `UNION ALL` 输出一行
`(seq, code, item, status, detail)`，由 `CASE WHEN` 产出 `PASS`/`BLOCK`；
Python 只负责建临时视图、执行 SQL、打印结果并按 `status` 确定退出码。
新增检查项即新增一个 `UNION ALL` 分支，判定口径集中在一处，
SQL 也可以单独在 beeline 或 Spark SQL 中执行。

三项实现约束（记录在文件头注释中）：

1. `SUM` 必须包 `COALESCE(..., 0)`。空分区时 `SUM` 返回 `NULL`，而
   `NULL = 0` 的结果是 `NULL`（既不真也不假），`CASE WHEN` 会落到 `ELSE`，
   把空分区误判成 `BLOCK`。
2. ODS 行数走临时视图（按 HDFS 路径读取），不要改成表读。表读依赖
   `MSCK REPAIR` 成功，而 ODS 导入中的 MSCK 是 `try/except` 只警告，
   一旦静默失败会读到 0 行，误报「分区为空」并阻断下游。
3. 时间戳用 `DATE_FORMAT` 转成字符串再取。直接 `collect` 时间戳会经 py4j
   按 Python 进程本地时区渲染（+8），打印成 `08:00 ~ 次日 07:59`，
   看起来像分区混入了次日数据，数据本身没有问题。

---

## 六、DIM 层：SCD2 拉链

### `etl/dim/dim_product_scd2.py` —— 商品类型2拉链

源数据没有商品主数据表，商品属性只能从事实表反推。

算法四步：

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

设计要点：

① 日主导组合。单日数据有噪声，实测一个商品一天最多出现 3 种属性组合，
直接使用原始记录会产生大量假版本，取当日出现次数最多的组合更稳定。
同票时按 `category_id` 兜底排序，保证结果确定。

② 开链/闭链规则：
- 每个商品的最后一个版本保持开放（`dw_end_date = 9999-12-31`，`dw_is_current = 1`）
- 其余版本闭链：`dw_end_date = last_seen + 1`，即下一版本的起始日，区间无重叠、无间隙

③ 当前版本的判定：

```sql
ROW_NUMBER() OVER (PARTITION BY product_id ORDER BY dw_start_date DESC) = 1
```

不使用「末次出现日 = 统计窗口末日」判定。

> 商品中途不再出现不等于属性变更（缺少观测不等于变更）。按「末次出现日 = 窗口末日」
> 判定，会把 10-31 之前就不再出现的商品全部误判为非当前（实测误判 9 万余个）。
> SCD2 的当前版本是版本序列的属性，与统计窗口无关。

④ 代理键：`dim_product_sk` 按 `(product_id, dw_start_date)` 全局编号，
与业务键解耦。

结果：166,794 个商品生成 172,794 个版本，其中 6,000 个商品在 10 月发生过属性变更。
校验结果：当前版本数不等于 1 的商品 0 个，区间重叠 0 对。

两种运行方式：

```bash
--dt D                增量：当日 delta 与当前版本比对 → 关旧版 + 开新版（不回扫历史）
--start D --end D     全量重建：按窗口重算整张拉链表
```

### `etl/dim/staging.py` —— 增量写回助手

Spark 不允许在读一张表的同时写这张表（自读自写守卫）。
实现方式是先把结果落成 parquet 快照，再从 parquet 读回写入目标表。

### 其他四张维表

| 表 | 类型 | 说明 |
|---|---|---|
| `dim_user` | 全量 + 时间属性 | 挂 `first_seen_date` / `last_seen_date` / `active_days` / `first_purchase_date` / `purchase_days`，支撑新老客与生命周期分析 |
| `dim_session` | 全量 | 会话维度（会话本身在事实表里也是退化维度） |
| `dim_category` | 全量 | 类目维度，624 行 |
| `dim_date` | 静态 | 365 行（2019 全年），不参与日调度 |

---

## 七、DWS 层：汇总

四张表，粒度不同：

| 表 | 粒度 | 行数（31 天） |
|---|---|---|
| `dws_user_session_daily` | 日 × 用户 × 会话 | 9,280,709 |
| `dws_product_daily` | 日 × 商品 | 2,299,245 |
| `dws_user_behavior_daily` | 日 × 用户 | 6,473,723 |
| `dws_traffic_hour_daily` | 日 × 小时 | 744（31×24） |

DWS 聚合不 JOIN 维度。

```python
df.groupBy("product_id", "category_id", "brand") \
  .agg(F.countDistinct(...).alias("exposure_uv"), ...)     # 直接聚合，不 JOIN dim
```

类目和品牌在 DWD 中已退化为事实表的普通列
（`category_id` / `category_code` / `brand` 属于事实表 13 列），因此不需要 JOIN。

这是星型模型的主要收益：省掉一次 JOIN，列存下直接按列聚合。

大基数聚合（如日×商品）在 DWS 层完成，ADS 层不需要面对全量明细。

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

① 漏斗回 DWD，不走 DWS

```python
# ads_conversion_funnel_daily.py：直接从 DWD 算
```

漏斗需要跨商品精确去重 UV。DWS 是商品级的，直接累加商品级 UV 会重复计算
同一用户浏览多个商品的情况。

② 留存右删失记 NULL，不记 0

```sql
-- 数据没到那天 → NULL（右删失），而不是 0
```

月末最后 7 天的 D+7 属于数据尚未到达，不是留存归零。记为 0 会导致报表出现
虚假的留存下跌。实测有 7 天 D+7 为 NULL。

业务指标：
- RFM 二八法则显著：「重要价值客户」占购买用户 12.2%，贡献 55.6% GMV
- 留存（基准日 10-16，D0=230,199）：D+1 17.68% / D+3 13.20% / D+7 10.74%
- 分时峰谷比 13.76×：上午(7-12) 转化最高（贡献 41.04% 订单），晚间(19-23) 客单价最高（354 元）

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

spark = get_spark("Pipeline")           # 一个进程一个会话
for i, (name, layer, fn) in enumerate(wanted, 1):
    for d in days:
        fn(spark, d)                    # 复用同一会话
run_checks(spark, start, end)           # 自动跨层对账
```

`run_checks()` 检查 6 项不变量：

| 检查 | 含义 |
|---|---|
| 购买次数守恒 | DWD = DWS用户 = DWS商品 = DWS会话 = ADS |
| GMV 守恒 | DWD = DWS用户 = DWS商品 = ADS |
| 漏斗不变量 | 每日 `view_uv >= cart_uv` 且 `view_uv >= purchase_uv` |
| SCD2 单当前版本 | 每商品恰好一个 `dw_is_current=1` |
| 分时守恒 | DWS = ADS = DWD 明细 PV |
| 留存不变量 | D+n 人数 ≤ D0 人数 |

任务成功只说明执行完成，不说明结果正确；跨层守恒检查验证的是业务语义层面的正确性。

### 幂等机制

| 机制 | 实现 |
|---|---|
| 分区级覆盖 | 静态分区 `INSERT OVERWRITE TABLE t PARTITION (event_date='D')` |
| 动态覆盖兜底 | `spark.sql.sources.partitionOverwriteMode=dynamic` |
| 时区固定 | `spark.sql.session.timeZone=UTC` |
| 确定性去重 | 排序追加全部剩余列做兜底 |
| 断点续跑 | `--skip-existing`：分区已有文件则跳过 |
| 重建前清目录 | `drop_path()`：清除旧残留，但保留空目录 |

重跑结果可重复依赖三个条件：分区粒度等于业务粒度（按天）、写入语义为覆盖而非追加、
计算过程确定（去重无并列）。三者缺一不可。

### `etl/run_locked.sh` —— 串行闸门

```bash
exec flock -w "$WAIT" "$LOCK" "$@"
```

7.6G 内存的机器上，两个 Spark 作业并发会 OOM。闸门保证同一时刻只有一个
SparkSession，跨 DAG、跨手动执行均生效。抢不到锁的任务排队等待，默认上限 7200 秒。

> 不要绕过 `run.sh` 直接执行 `python xxx.py`，否则闸门失效。

---

## 十、调度

### `scheduler/` —— 配置与代码分离

四个文件，职责不重叠：

| 文件 | 角色 | 说明 |
|---|---|---|
| `airflow_env.sh` | 配置入口（唯一） | 生成 Variable `warehouse_env`（自动回读校验）+ `airflow-env.sh`；`--show` 只读查看 |
| `check_env.sh` | DAG 的环境守卫脚本 | 由 DAG 替换占位符后执行，不嵌在 Python 里 |
| `incremental_warehouse_dag.py` | 编排定义（TaskFlow API） | `@dag` / `@task`；只写"怎么编排"，不写路径常量 |
| `alerts.py` | 失败/重试告警 | 4+1 通道，见 [incremental/ALERTS.md](incremental/ALERTS.md) |

写法：TaskFlow API（`@task`）+ 独立子进程

```python
from airflow.sdk import dag, task          # Airflow 3 推荐路径
                                           # （airflow.decorators 已废弃）

@task(**_step_options("ods_import"))
def ods_import(ds=None, upstream=None):    # ds 由 Airflow 自动注入；upstream = 上游依赖
    return _run_step("ods_import", ds)     # 内部 subprocess 跑 etl/run_locked.sh
```

采用 `@task` + subprocess 而不把 ETL 逻辑搬进函数的原因：

> 宿主 16G 内存被 WSL2 限制为约 7.6G，HDFS、MySQL、Airflow 常驻约 2.5G，
> 必须保证同一时刻只有一个 SparkSession（JVM 运行结束后释放）。
> 如果把 21 张表写成一个进程里的 Python 函数，SparkSession 会累积，
> 最终被内核 OOM killer 终止。因此 `@task` 只负责编排、传参、重试、超时和告警，
> 计算仍交给 `etl/run_locked.sh`（内含 `flock` 串行闸门）这个独立子进程。

配置优先级链（高 → 低）：`os.environ` → Airflow Variable `warehouse_env` → 代码默认值。

```python
def _cfg(key, default=None):
    for src in (os.environ.get(key.upper()), _CFG.get(key)):   # _CFG = Variable 里的 JSON
        if src:
            return str(src)
    return default
```

Airflow Variable 存储在元数据库中，DAG 进程加载时读取一次，配置因此不进入代码。
官方文档在 Variables 一节建议把可变参数放在 Variable 中，而不是写死在 DAG 里。
超时表、告警分级名单这类策略参数也一并放入了 Variable：
`timeout_overrides_min` / `quality_tasks` / `quality_warn_tasks`。

> 导入格式必须保留外层 key。
>
> `airflow variables import <file>` 会把 JSON 顶层 key 逐个当成独立 Variable 名。
> DAG 读取的是单个 `warehouse_env`，所以 JSON 必须是
> `{"warehouse_env": { ...真正的配置... }}`，不能铺平成 `{"spark_home": ...}`。
>
> 铺平后，库里会多出一批 `spark_home`、`data_start` 之类的扁平 key，
> `airflow variables get warehouse_env` 报 `does not exist`，
> 而 DAG 会静默回退默认值、不报错。
> `airflow_env.sh` 在导入后会回读 `warehouse_env` 校验，失败时显式告警。

`airflow.cfg` 不做修改。它是 `airflow config` 升级或迁移时会重写的生成文件。
本机实测 cfg 与官方默认输出逐字节相同。配置全部放在 `airflow-env.sh` 的 8 个
`AIRFLOW__<SECTION>__<KEY>` 环境变量中，优先级高于 cfg，且可以纳入版本管理。

子进程环境必须显式合并：

DAG 使用 TaskFlow API（`@dag` / `@task`），每个 task 内部通过
`subprocess.run(...)` 启动独立子进程执行 ETL 脚本，环境变量需要显式传入：

```python
p = subprocess.run(cmd, env={**os.environ, **TASK_ENV}, capture_output=True, text=True)
```

> 必须显式传 `TASK_ENV`：`.bashrc` 里的 `SPARK_HOME` / `HADOOP_CONF_DIR`
> 在 Airflow 任务进程里通常不存在（非交互 shell）。子进程拿不到 `SPARK_HOME` 时，
> PySpark 的 `_find_spark_home()` 会回退到 venv 里的 pyspark 包目录
> （那里没有 `conf/spark-defaults.conf`），导致会话不挂 metastore，
> 报 `TABLE_OR_VIEW_NOT_FOUND: dwd.dwd_event_fact`。
>
> `{**os.environ, **TASK_ENV}` 中 `TASK_ENV` 在后，优先级更高，
> 既保留 `HOME`/`PATH` 等系统变量，也让 Spark/JDK 路径覆盖任何同名变量。

其余编排要点：
- 一表一 task（20 张表 + `check_env` = 21 个 task），`max_active_tasks=1` 配合 `etl/run_locked.sh` 的 flock 双保险
- TaskFlow 的依赖通过函数签名表达：`def dwd_event_fact_dqc(fact=None, dirty=None)` 声明
  它等待 `fact` / `dirty` 两个上游；调用处 `dwd_event_fact_dqc(fact=dwd_event_fact(...), dirty=...)`
  决定连接关系。参数个数等于上游个数，多传一个位置参数会直接报
  `TypeError: too many positional arguments`。
- `ds` 是保留参数名：函数签名里写 `ds=None`，Airflow 运行时自动注入当前逻辑日期
  （等价于旧版的 `{{ ds }}`，但它是字符串，不需要 `{{{{ }}}}` 转义）。
- `schedule="@daily"` + `end_date=DATA_END`，不能用 `schedule=None`（Airflow 3 的 backfill 拒绝 NullTimetable）
- 区间型任务（`RANGE_TASKS`）传 `--start DATA_START --end <ds>`，其余传 `--dt <ds>`
  （`_run_step()` 里按 `RANGE_TASKS` 分支拼参数）

### `scheduler/alerts.py`

见 [GETTING_STARTED.md §九](GETTING_STARTED.md#九可选airflow-调度) 与
[incremental/ALERTS.md](incremental/ALERTS.md)。

---

## 十一、关键设计决策

主要取舍如下。

### 1. 退化维度（不建表，直接进事实表）

`event_type` / `behavior_type` / `is_purchase` / `event_hour` / `user_session`
直接作为事实表的列，不做维表关联。

判断依据：
1. 是否有自己的属性：无自己的属性则退化（`event_type` 只有 4 个值，单独建表没有收益）
2. 基数大小与是否需要 JOIN：小枚举则退化
3. 是否需要保留历史版本：不需要则退化

收益：DWS 聚合免 JOIN，列存下 `GROUP BY event_type` 开销很低。

### 2. 星型模型而非雪花

维度扁平、直接挂事实表，不向下拆分层级。

收益：查询和预聚合快（JOIN 少）、ETL 与血缘简单、冗余成本可接受
（列存 + snappy 下，冗余几个低基数列的代价远小于每次查询多 JOIN）。

### 3. 加时分维度（但只加到 DWS/ADS，不加到 ODS/DWD）

跨 31 天按小时聚合，峰谷比 13.76×，三个峰值错位
（16 点 PV 峰值、9 点订单峰值、21 点客单价峰值）。

加在这三处：
1. DWS 加 `dws_traffic_hour_daily`（粒度 `日×小时`）
   —— `event_hour` 已在事实表，一次 GROUP BY 即得，1.37M 行/天 压到 24 行/天
2. ADS 加 `ads_traffic_hour_daily`（增加报表语义：时段分桶、占比、峰值标记）
3. `dim_user` 挂时间属性（支撑新老客与生命周期）

不加在这两处：
- ODS/DWD 加 hour 分区：判据是分区粒度要匹配数据到达粒度和重跑粒度。
  数据按天批量落盘（不是小时流），DWD 重跑一天只要 45 秒；
  1.37M 行/天 ÷ 24 ≈ 57k 行、10MB 一个分区，加小时分区只是把 31 个目录
  变成 744 个碎分区，带来小文件问题、扫描更慢，并且需要重导 9GB。
- 丢掉 `event_time` 只留 `event_hour`：会话时长、行为先后、漏斗路径都依赖 `event_time`。
  `event_hour` 是便利列，不是替代品。

### 4. 脏数据隔离而非静默丢弃

脏行落 `dwd_event_dirty` 留痕，保证「干净 + 脏 + 去重 = ODS」账目对得平，且可审计。

### 5. 跨层对账作为交付标准

验收标准是业务不变量成立，而不是任务执行成功。

### 6. 时间精度到天的取舍（SCD2 的天花板）

拉链需要变更时刻，但源数据只有观测时刻（用户看到这个商品时是什么属性），
没有变更时刻（商品何时被修改）。所以 `dw_start_date` 只能精确到天。

这是数据源的边界，不是实现问题。

---

## 下一步

- 运行方式见 [GETTING_STARTED.md](GETTING_STARTED.md)
- 数据字典与实测数字见 [incremental/WAREHOUSE_BUILD.md](incremental/WAREHOUSE_BUILD.md)
