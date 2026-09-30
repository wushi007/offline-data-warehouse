# 数仓面试问答（Spark + StarRocks 双引擎，代码实证版）

> 本回答基于你项目里**真实跑通**的代码与数据，双引擎各给证据：
> - **Spark 侧**：`etl/spark_dims_dws_ads/build_dims.py`、`build_dws.py`、`build_ads.py`、`etl/dws_aggregate.py`、`etl/ads_report.py`、`optimize/run_bench.py`
> - **StarRocks 侧**：`starrock/create_dim_starrocks.py`、`build_inc_starrocks.py`、`build_dws_starrocks.py`、`build_ads_starrocks.py`、`load_dwd_starrocks.py`
> - 数据事实：46,908,364 行行为明细 / 34 天（2019-10-01 ~ 2019-11-03）/ 四类事件（view/cart/remove_from_cart/purchase）/ 源 purchase 809,238 行 / 购买用户 373,611 / GMV 249,929,113.38

---

## Q2 星型 vs 雪花；为什么选星型；什么是退化维度

### 2.1 概念对比

| 维度 | 星型模型 | 雪花模型 |
|---|---|---|
| 维度表形态 | **扁平**：维度表直接挂事实表，属性冗余在维度表内 | **规范化**：维度表拆多级（category → category_group），层层外键 |
| 查询路径 | 事实表 → 1 次 JOIN 到维度 | 事实表 → 多跳 JOIN |
| 冗余 | 有冗余（列存 + 压缩可接受） | 少冗余 |
| 查询性能 | 快（JOIN 少，可预聚合免 JOIN） | 慢（多 JOIN） |
| ETL 复杂度 | 低，血缘简单 | 高，要维护多级维度 |
| 适用场景 | OLAP 分析、BI 报表（主流，Kimball 默认形态） | 强一致性、存储敏感、维度层次稳定 |

### 2.2 你项目为什么选星型（两引擎证据）

**证据① 事实表直接冗余维度属性，DWS 聚合不 JOIN 维度。**

Spark 侧 `dwd_event_fact` 13 列里直接带 `category_id / category_code / brand`（`etl/dwd_clean.py` 的 `FACT_COLUMNS`），DWS 直接对事实表 GROUP BY：

```python
# etl/spark_dims_dws_ads/build_dws.py
df.groupBy("product_id", "category_id", "brand")
  .agg(F.countDistinct(...).alias("exposure_uv"), ...)   # 不 JOIN dim，直接聚合
```

StarRocks 侧同一个事实表 `dwd.dwd_event_fact`（`starrock/load_dwd_starrocks.py` 13 列）在 StarRocks 内全 SQL 聚合，同样不 JOIN 维度：

```sql
-- starrock/build_dws_starrocks.py
INSERT INTO dwd.dws_product_daily
SELECT event_date, product_id, category_id, brand,
    COUNT(DISTINCT IF(event_type='view',user_id,NULL)) AS exposure_uv, ...
FROM dwd.dwd_event_fact
GROUP BY event_date, product_id, category_id, brand
```

**证据② 维度表扁平、冗余类目属性，不向下拆层级。**

Spark 版（`build_dims.py` DDL）与 StarRocks 版（`create_dim_starrocks.py`）的 `dim_product_scd2` 都直接含 `category_id / category_code / brand`，没有单独的 `dim_category` 挂到商品之下（`dim_category` 只作为独立基础维度存在）：

```sql
-- starrock/create_dim_starrocks.py  （Spark 版 build_dims.py 同结构）
CREATE TABLE IF NOT EXISTS dwd.dim_product_scd2 (
    dim_product_sk bigint not null,      -- 代理键
    product_id     bigint,
    category_id    bigint,               -- 类目冗余进商品维
    category_code  varchar(64),
    brand          varchar(64),
    dw_start_date  date,
    dw_end_date    date,
    dw_is_current  tinyint
) PRIMARY KEY (dim_product_sk) ...        -- StarRocks PK 模型（SCD-2 靠它 UPSERT）
```

**结论**：这是标准星型。选它的三个理由（面试直接讲）：
1. **查询/预聚合性能**——DWS 层不带维度表就能完成 `日×商品` 聚合，省掉 JOIN；
2. **ETL 与血缘简单**——扁平维度好建、好对账、好解释；
3. **冗余成本可接受**——列式存储 + snappy 下，冗余几个低基数列的代价远小于每次查询多 JOIN 的代价。

### 2.3 退化维度（Degenerate Dimension）

**定义**：没有独立维度表、属性直接退化存进事实表成为普通列的维度。本质是判断"这个维度没有值得建表的属性/低基数/稳定 → 不建表，直接放事实里"。

**你项目里的退化维度**（都在 `dwd_event_fact`，13 列里占了一半以上）：

| 列 | 值域 | 为什么退化成列 |
|---|---|---|
| `event_type` | view/cart/remove_from_cart/purchase | 4 个值、无属性，建表纯属多余 |
| `behavior_type` | 浏览/加购/移出购物车/购买 | 中文衍生，同枚举 |
| `is_purchase` | 0/1 | 派生分析标记，支撑"购买行为"过滤 |
| `event_hour` | 0~23 | 派生分析维度，支撑分时分析 |
| `user_session` | 会话ID | 无额外属性（虽有 dim_session 存归属，会话本身仍是退化维度）|

**为什么要用**（面试话术）：
1. **低基数 + 无属性**的维度，建表只增加 JOIN 无收益（event_type 就 4 个值）；
2. **性能**：`GROUP BY event_type` 直接扫事实表列，列存下零开销；
3. **派生即用**：is_purchase/event_hour 是清洗时算好的，放事实表即取即用，避免每次聚合再推导。

**进阶对比（加分项）**：你的 `dim_user`（StarRocks 版 `create_dim_starrocks.py`）表结构只有一列 `user_id`——一个"接近退化维度"的维度表。面试可以主动讲这个判断：*"当维度表只剩主键没有属性时，说明它其实是个退化维度；我保留它是因为要支持`dim_user`作为维度表参与星型关联、以及以后挂用户属性，属于预留。"*

### 2.4 面试追问：怎么判断一个属性该建维表还是退化？

判断框架（三问）：
1. **有没有自己的属性？** 无 → 退化；有（商品有品牌/类目/价格档）→ 建维度表；
2. **基数大不大、要不要被 JOIN/过滤？** 大且要关联 → 维度表；小枚举 → 退化；
3. **要不要保留历史版本（SCD）？** 要 → 维度表（如 dim_product_scd2）；不要 → 退化。

---

## Q3 缓慢变化维（SCD）

### 3.1 四种主流处理方式

| 方式 | 做法 | 保留历史 | 适用 |
|---|---|---|---|
| SCD0 | 不处理 | — | 属性永不变化的维度 |
| SCD1 | 直接覆盖更新 | ❌ | 错误修正、无需回溯 |
| **SCD2** | 代理键 + 生效时间窗 + 当前标记，变更=关旧版+开新版 | ✅ 完整历史 | **需要回溯"当时是什么"**（你的商品类目归属） |
| SCD3 | 只留"上一条值 + 当前值"两列 | 仅最近 1 次 | 只看上一次变化 |

### 3.2 你项目的 SCD-2 实现（两引擎对照）

**为什么商品维度用 SCD-2**：源数据里同一个 `product_id` 存在多个 `(category_id, category_code, brand)` 组合（建维时用 `ROW_NUMBER() OVER (PARTITION BY product_id ORDER BY cnt DESC, category_id)` 取**主导组合**收敛）——说明**商品属性会变化**，而按天报表必须还原"当天这个商品属于哪个类目"。

**① Spark 版**（`etl/spark_dims_dws_ads/build_dims.py`，`INSERT OVERWRITE` 写外部表）：

```sql
INSERT OVERWRITE TABLE dim_product_scd2
SELECT
    ROW_NUMBER() OVER (ORDER BY product_id) AS dim_product_sk,   -- 代理键
    product_id, category_id, category_code, brand,
    DATE '2019-10-01' AS dw_start_date,     -- 初始版本生效起点（数据首日）
    DATE '9999-12-31' AS dw_end_date,       -- 开放上界 = 当前有效
    CAST(1 AS TINYINT) AS dw_is_current     -- 当前版本标记
FROM (
    SELECT product_id, category_id, category_code, brand,
           ROW_NUMBER() OVER (PARTITION BY product_id
                              ORDER BY cnt DESC, category_id) AS rn  -- 主导组合收敛
    FROM (SELECT product_id, category_id, category_code, brand, COUNT(*) AS cnt
          FROM fact_events GROUP BY product_id, category_id, category_code, brand) g
) t
WHERE rn = 1
```

**② StarRocks 版**（`starrock/create_dim_starrocks.py` 建表 + `starrock/build_inc_starrocks.py` 装载，逻辑与上完全一致，但表用 **PRIMARY KEY 模型**）：

```sql
-- starrock/create_dim_starrocks.py
CREATE TABLE IF NOT EXISTS dwd.dim_product_scd2 (
    dim_product_sk bigint not null,
    product_id     bigint,
    category_id    bigint, category_code varchar(64), brand varchar(64),
    dw_start_date  date, dw_end_date date, dw_is_current tinyint
) PRIMARY KEY (dim_product_sk)      -- PK 模型：UPSERT 支持"关旧版+开新版"
```

```sql
-- starrock/build_inc_starrocks.py  load_dims()：同一份主导组合收敛逻辑
INSERT INTO dwd.dim_product_scd2
SELECT ROW_NUMBER() OVER (ORDER BY product_id) AS dim_product_sk,
       product_id, category_id, category_code, brand,
       DATE '2019-10-01', DATE '9999-12-31', 1
FROM (SELECT ..., ROW_NUMBER() OVER (PARTITION BY product_id ORDER BY cnt DESC, category_id) AS rn
      FROM (SELECT product_id, category_id, category_code, brand, COUNT(*) AS cnt
            FROM dwd.dwd_event_fact GROUP BY product_id, category_id, category_code, brand) g) t
WHERE rn = 1
```

> **为什么 StarRocks 选 PRIMARY KEY 模型做 SCD-2**（这是 StarRocks 侧建模选型的核心考点）：SCD-2 的增量更新 = "UPDATE 关掉旧版本 + INSERT 开新版本"，PRIMARY KEY 模型天然支持 UPSERT；而 DUPLICATE KEY 模型只能追加、无法覆盖历史行。你 StarRocks 侧一句话总结：*"维度（要按主键更新）→ PRIMARY KEY；明细/汇总（要保留全量、按天重算）→ DUPLICATE KEY + RANGE 分区。两类模型各用长板。"*

### 3.3 增量拉链怎么演进（面试要讲清"已做/演进中"边界）

**当前状态**：`dim_product_scd2` 是**初始快照**（每商品 1 条当前版本）。

**增量语义**（面试主动讲出来，这是设计能力体现）：
1. 每天新分区入库后，提取当天各商品的属性组合；
2. 与该商品**当前版本**（`dw_is_current=1`）对比：
   - 无变化 → 不动；
   - 有变化 → **UPDATE 旧版本**：`dw_end_date = 变化日`, `dw_is_current = 0`；**INSERT 新版本**：`dw_start_date = 变化日`, `dw_end_date = 9999-12-31`, `dw_is_current = 1`。

StarRocks 侧这个能力已经"写死在建模里"——PK 模型 `INSERT ... ON DUPLICATE KEY UPDATE` 或直接 UPSERT 就能关旧开新；Spark 侧演进方式是"当前维 + 变更表 merge（full outer join 打标）再重写拉链表"。诚实表述：*"当前落地的是初始快照；增量拉链在 StarRocks 侧靠 PK 模型演示了关闭机制，Spark 侧是演进点。"*

### 3.4 面试口头例子（背下来）

> 商品 P1001，2019-10-01~10-15 归类目 C1，10-16 起改为 C2。SCD-2 维护两行：
> - `sk=1, product_id=1001, category_id=C1, start=2019-10-01, end=2019-10-16, is_current=0`
> - `sk=2, product_id=1001, category_id=C2, start=2019-10-16, end=9999-12-31, is_current=1`
>
> 查某天数据：`event_date BETWEEN dw_start_date AND dw_end_date` 关联还原当天归属；取当前值：`WHERE dw_is_current=1`。

---

## Q4 幂等（Idempotency）

### 4.1 定义

**同一任务、同样的输入，无论跑多少次，最终状态一致。** 核心 = **重跑是"覆盖"，不是"追加"**。

### 4.2 Spark 侧：动态分区覆写 + 静态分区 INSERT

统一配置在 `etl/utils.py::get_spark`：

```python
.config("spark.sql.sources.partitionOverwriteMode", "dynamic")   # 动态分区覆写
```

老链路（`etl/dws_aggregate.py`）按天写，**只覆写当天分区**：

```python
out.coalesce(4).write.mode("overwrite").partitionBy("event_date") \
    .option("compression", "snappy").parquet(DWS["user_session"])
```

新链路（`etl/spark_dims_dws_ads/build_dws.py`）用**静态分区 INSERT OVERWRITE**，把"只重写这一天"写死成 SQL 语义：

```python
spark.sql(f"INSERT OVERWRITE TABLE {name} "
          f"PARTITION ({PARTITION_COL} = '{d}') "
          f"SELECT {cols} FROM t_agg")
```

**机制拆解（面试必讲）**：
- 默认 `overwrite` 是**先删整个输出目录再写** → 重跑会清掉全部历史分区（危险）；
- `dynamic` 模式（或静态分区 spec）**只覆盖本次写入数据所在的分区** → 重跑当天，Spark 先删 `event_date=2019-11-01` 目录下的旧文件，再写新结果，其他 33 个分区不动。

### 4.3 StarRocks 侧：RANGE 分区 + TRUNCATE 重灌 + Stream Load label

**① DWS/ADS 按 `event_date` RANGE 分区，支持分区级 TRUNCATE 后重灌单天（幂等重建）**（`starrock/build_dws_starrocks.py`）：

```sql
CREATE TABLE IF NOT EXISTS dwd.dws_user_session_daily (...) 
DUPLICATE KEY(event_date, user_id, user_session)
PARTITION BY RANGE(event_date)(PARTITION p20191001 VALUES LESS THAN ('2019-10-02'), ...)
```

逐天灌入 = 每天一个独立小任务，天然可单独重跑：

```sql
-- starrock/build_dws_starrocks.py：每天 WHERE event_date=... 只扫单分区
INSERT INTO dwd.dws_user_session_daily
SELECT event_date, user_id, user_session, SUM(IF(event_type='view',1,0)) AS view_cnt, ...
FROM dwd.dwd_event_fact
WHERE event_date = date'2019-11-01'       -- 只重算这一天
GROUP BY event_date, user_id, user_session
```

**② 明细灌入用 Stream Load + label 去重**（`starrock/load_dwd_starrocks.py`）：每个文件一个唯一 label，StarRocks **label 级别幂等**（同 label 重试不重复导入）：

```python
label = f"{TABLE}_{int(time.time())}_{i}"
res = stream_load_csv(str(f), label)
```

### 4.4 重跑当天数据会重复吗？—— 不会

因为语义是"**先清该分区/覆盖，再写**"，不是 append。只有误用 `append` / `insertInto`（不带覆盖）才会重复。

### 4.5 工程细节（面试加分）

- **幂等的两个前提**：① 分区粒度 = 业务粒度（按 `event_date` 分天，重跑才可能"只覆盖当天"）；② 写入语义必须是覆盖而非追加。你把它固化在统一 SparkSession 配置里，全链路可重跑。
- **DROP+CREATE 后旧分区要重新注册**：Spark 外部表 DROP 不删文件，增量重建时新表只登记本次 INSERT 的分区，需 `MSCK REPAIR TABLE` 补注册旧分区目录（`etl/spark_dims_dws_ads/build_dws.py`）——这也是"重跑可重复"的工程细节。
- **全量重建要先清旧文件**：外部表 DROP 不清文件，旧实现残留的 schema（如旧 RFM 的 `as_of_dt` 分区目录）会被 Spark 推断进重建的表导致 `INSERT_COLUMN_ARITY_MISMATCH` → 用 Hadoop FS 先删目标目录（`drop_path()`）。

---

## Q5 数据倾斜：发现 + 加盐两阶段聚合

### 5.1 你怎么发现的（现象 → 指标 → 你的诊断经验）

**现象层**：
- Spark UI stage 的 task 列表：**绝大多数 task 秒级完成，1~2 个 task 耗时极长**；
- 看 **Shuffle Read Size**：热点 key 所在 reducer 拉取几百 MB~几 GB，其他 reducer 只有几十 KB。

**指标层**（你项目 `optimize/run_bench.py` + `docs/PITFALLS.md C1` 的真实经验）：
- 用 `SparkMetricsCollector` 采集 `taskMetrics.shuffleReadMetrics.bytesRead`，算 **reducer 侧 max/median** → 全量数据热点 key 达 **1198×**；
- **两个关键教训（面试直接讲）**：
  1. 要看 **reducer 侧 `shuffleRead`**，不是 map 侧 `shuffleWrite`（map 侧按分区均匀，看不出倾斜）；
  2. **诊断前先关 AQE 的 `coalescePartitions`** + 固定分区数，否则 reducer 被合并掩盖，max/median 恒为 1.0×：

```python
# optimize/run_bench.py：让热点 key 冲突显性化
spark.conf.set("spark.sql.adaptive.coalescePartitions.enabled", "false")
spark.conf.set("spark.sql.shuffle.partitions", "30")
```

### 5.2 加盐 + 两阶段聚合（你的真实代码 + 适用边界）

**思路**：热点 key 随机拼 N 个盐值 → 拆成 N 份分散到 N 个 reducer → 先局部聚合 → 去盐 → 全局聚合。因 **count/sum 可加**，结果与直接聚合一致。

**真实代码**（`optimize/run_bench.py` 实验 2，盐数 N=10）：

```python
# 阶段1：加盐打散热点 key → 局部聚合
salted = (
    df.withColumn("salt", (F.rand(seed=42) * salt_num).cast("int"))       # key 随机分到 0~N-1
      .withColumn("skey",
                  F.concat(F.col("category_id").cast("string"),
                           F.lit("_"), F.col("salt")))                    # 热点key拆成N份
)
phase1 = salted.groupBy("skey").agg(F.count("*").alias("cnt"))            # 局部小聚合

# 阶段2：去盐 → 全局聚合（count 可加 → 结果与直接聚合一致）
result = (
    phase1.withColumn("category_id", F.split(F.col("skey"), "_")[0].cast("bigint"))
          .groupBy("category_id")
          .agg(F.sum("cnt").alias("cnt"))
)
```

**结果一致性校验**（简历/面试加分点：优化不能改变业务结果）：

```python
r_base = {r["category_id"]: r["cnt"] for r in baseline()}
r_opt  = {r["category_id"]: r["cnt"] for r in optimized()}
assert r_base == r_opt            # 你项目实测 True
```

**适用边界（面试体现深度）**：
1. 只对**可加性聚合**（count/sum/max/min）有效；**count distinct 不能直接 sum**，要换 `approx_count_distinct` 或二次去重；
2. **小样本反而更慢**：你项目实测 0.1 样本两阶段 1.03s vs 直接 0.54s（多一次 shuffle 的开销盖过收益）；**全量才显性**（1198×）；
3. 因此**按需开启**，不是无脑全上——你当前是 A/B 基准演示，未接入生产链路（`docs/README.md` 如实标注）。

### 5.3 StarRocks 侧对比（说明双引擎的差异）

- **StarRocks 无 shuffle**：MPP 向量化 + 分布（HASH/分桶），热点 key 由分布策略处理，不涉及 Spark 式 shuffle 倾斜；它的"倾斜"风险在**单 BE 内存**。
- 你的应对是**逐分区灌入**（`starrock/build_dws_starrocks.py` 注释 + `docs/STARROCKS_SUMMARY.md`）：

> 单 BE 内存上限约 5.2GB，全表一次 GROUP BY（10M 分组）会 OOM（错误码 5609）；改为**按 event_date 分区逐天灌入**（`WHERE event_date=...`），哈希表缩小 34 倍，同时天然符合按天幂等。

### 5.4 其他手段（可补充答）

- AQE 的 **skewJoin**（`spark.sql.adaptive.skewJoin.enabled`）处理 JOIN 倾斜；
- 动态分区 / 手动 `repartition(col)` 把热点 key 打散；
- 小表**广播**（`broadcast join`）避免 shuffle；
- 预聚合（DWS 提前按天聚合，把倾斜发生在大基数之前化解）——你项目的分层本身就是一道预防。

---

## 附：一句话"锚点"速记

- **Q2**：星型少 JOIN、DWS 免维度预聚合；`event_type/is_purchase/event_hour` 是退化维度，判断框架=有无属性/基数/是否需要 SCD。
- **Q3**：`dim_product_scd2` 用 SCD-2（代理键+时间窗+当前标记），Spark=`INSERT OVERWRITE` 初始快照，StarRocks=PRIMARY KEY 模型 UPSERT 支持关旧开新。
- **Q4**：Spark `partitionOverwriteMode=dynamic` + 按天分区 / StarRocks RANGE 分区 + TRUNCATE 重灌 + Stream Load label = 重跑只覆盖当天、不重复。
- **Q5**：reducer 侧 `shuffleRead` + 关 AQE 诊断；加盐两阶段只对 count/sum 可加聚合有效，结果一致性已校验；StarRocks 无 shuffle、靠逐分区灌入防 BE OOM。
