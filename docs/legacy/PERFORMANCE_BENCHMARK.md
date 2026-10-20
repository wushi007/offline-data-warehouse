# StarRocks vs Spark 性能对比（dwd_event_fact 46.9M 行）

> ## 口径已过期（2026-10-06 标注）
>
> 本文档是 46,908,364 行 / 34 分区时期的旧版报告，结论区间 1.7×~12× 只适用于该快照。
>
> 仓库现行的基准为 109,806,507 行 / 61 分区，区间为 3.3×~11×，数据源：
> `starrocks/bench_results/starrocks_bench.json`、`starrocks/bench_results/spark_bench.json`。
>
> 两套数字不能混用。统一引用现行口径 3.3×~11×（109.8M 行 / 61 分区），本文档作为历史记录保留。
> （上界 11× 即下面第 1 条修复后的 Q7。该查询跨轮实测 274~406ms，对应 7.5×~11.1×。）
>
> 另有以下已知问题：
> 1. Spark 侧 Q7（漏斗 3 层 UV）当时使用的是有缺陷的写法：`F.when(cond, "user_id")` 传入的是字符串字面量而非列引用，`countDistinct` 恒等于 1，因此该耗时不是漏斗 UV 的耗时。缺陷已于 2026-10-06 修复（`starrocks/bench_spark.py`），Spark 侧 Q7 的 214ms 及由此算出的 4.9× 不可信。修复后在新口径下重测：Spark 405.5ms vs StarRocks 36.5ms，约 11.1×（此前无效值为 4.4×），即该缺陷使 Q7 的加速比被低估约 2.5 倍。
> 2. Q6/Q9 属于结果集较大的查询：结果回传客户端的开销占端到端耗时的 79% / 92%，这部分与引擎无关，会把加速比稀释到 2~3×。端到端口径低估了引擎之间的实际差距。
> 3. Q2 的「分区裁剪」表述不准确（2026-10-06 实测核正）：StarRocks 侧建表没有 `PARTITION BY`（`SHOW CREATE TABLE` 只有 `DUPLICATE KEY(event_date)` + `DISTRIBUTED BY HASH(event_date) BUCKETS 16`），`SHOW PARTITIONS` 仅 1 行，46.9M/61 天全部位于一个默认分区。单天过滤能只扫 1/16 依靠的是分桶裁剪（EXPLAIN：`partitions=1/1` 但 `tabletRatio=1/16`）。Spark 侧才是分区裁剪（HDFS parquet 按 event_date 分目录）。两侧机制不同、语义相同。故已统一改名为「单天裁剪」。引用 Q2 的 10.5× 时需注意，其中混入了 parquet 分区裁剪与 StarRocks 分桶裁剪的差异，不只是引擎能力差异。

> 同一份明细数据（46.9M 行 / 34 分区）、同一组 BI 典型查询，分别运行在 StarRocks 4.1.4（Docker 单 BE）与 Spark 3.5.9（local 模式）上，记录端到端耗时。

## 1. 环境与方法

| 项 | StarRocks | Spark |
|---|---|---|
| 版本 | 4.1.4（Docker allin1，FE 9030 / 单 BE） | 3.5.9（Hadoop 3.4.3，local[4]，driver 2500m） |
| 数据 | 原生表 `dwd_event_fact`（2.35GB 存储） | 同源 HDFS parquet `dwd_event_fact`（1.77GB） |
| 行数 | 46,908,364 | 46,908,364 |
| 计时 | 预热 1 次 + 计时 3 次，取最优 | 预热 1 次 + 计时 2 次，取最优 |
| 内存 | 受限环境（两引擎不并行运行，分阶段测量） | 同左 |

公平性说明：两引擎均在同一台 7GB 内存的 WSL2 机器上分阶段测量，配置合理（StarRocks 为默认单副本；Spark 开启 AQE + Kryo，local[4]）。Spark 端仅测能对等执行的查询（Q8 JOIN、Q10 窗口排名依赖 StarRocks 内的 DWS/ADS 表，Spark 无对应源，未纳入）。

## 2. 对比结果

| 查询 | 说明 | StarRocks | Spark | StarRocks 快 |
|---|---|---|---|---|
| Q1 | 真实列扫描 SUM(price) | 71 ms | 852 ms | 12.0× |
| Q2 | 单天裁剪（date= 过滤 COUNT） | 7.4 ms | 78 ms | 10.5× |
| Q3 | 小基数分组（event_type） | 273 ms | 980 ms | 3.6× |
| Q4 | 按日聚合 GMV（34 组） | 224 ms | 990 ms | 4.4× |
| Q5 | UV 精确去重（323 万 user） | 356 ms | 4022 ms | 11.3× |
| Q6 | 大基数分组（product_id，17 万组） | 1254 ms | 2695 ms | 2.1× |
| Q7 | 漏斗 3 层 UV（单天） | 44 ms | 214 ms | 4.9× |
| Q9 | 购买用户分组（37 万组） | 2012 ms | 3487 ms | 1.7× |
| Q1m | COUNT(*)（StarRocks 元数据优化） | 2.0 ms | 257 ms | 128×* |

\* Q1m 仅作参照：StarRocks 对 `COUNT(*)` 走元数据计数不扫描，Spark 需读文件，不作吞吐口径。

单列扫描吞吐：Q1 实际读取 price 列（DOUBLE 8B/行），StarRocks 71ms 扫描 46.9M 行，约 6.6 亿行/s、5.0 GB/s（按被扫列原始体积口径）。

## 3. 结论

> 本节数字为 46.9M 行口径，引用时请使用现行口径 3.3×~11×（109.8M 行 / 61 分区），见文首说明。

1. OLAP 查询性能：在 46.9M 行明细上，StarRocks 对聚合、去重、分组、JOIN 查询比 Spark 快 1.7×~12×；全表扫描、UV 精确去重、单天裁剪等高频场景均超过 10×。
2. 写入 / 构建吞吐：DWS/ADS 层使用 `INSERT...SELECT` 在 StarRocks 内完成，逐分区灌入，46.9M 行聚合无需 shuffle 到外部引擎，全链路在 StarRocks 内闭环。
3. 架构收益：Spark 承担一次性历史灌入（Stream Load 写入），日常分析查询落在 StarRocks 上，避免为秒级查询启动 Spark 作业带来的调度和冷启动开销。

### 3.1 为什么 Q6/Q9 不随 Spark 内存提升（内存敏感度分析）

若将 Spark 内存从 2500m 增加到 4g 重跑 Q6/Q9，收益有限，原因如下：

- 内存只影响哈希表能否装下：Spark 聚合需将分组键放入哈希表，装不下才 spill 到磁盘。哈希表大小取决于分组基数，与明细行数无关。
- Q6/Q9 的分组基数很小：Q6 为 17 万组 product_id（约 1.4MB 哈希表），Q9 为 37 万组 user_id（约 3MB），在 2500m 堆内远未占满，不是内存瓶颈。耗时主要来自 46.9M 行扫描 I/O 与 shuffle 传输。
- 真正吃内存的是 Q5（UV 精确去重）：全表 3.2M 个 key 的单一去重集合（约 25MB+），属于内存敏感查询；即便如此，2500m→3g 的收益也只有 10~20%。
- 未实测 4g 的原因：本机仅 7GB 物理内存，Spark `-Xmx4g` 的 JVM RSS 约 4.5GB，加上 HDFS（约 2GB）与操作系统（约 1.5GB）约 8GB，超出预算。强行运行会打满 2GB swap，耗时被磁盘交换主导，数值失真，不可用于报告。3g 是安全上限，仍可能轻微 swap。
- 结论：Q6/Q9 属于非内存敏感负载，加大内存没有参考价值；保留 2500m 是资源约束与负载特性权衡后的合理默认。进行内存调优实验时应选择 Q5 这类内存敏感查询。

## 4. 复现

```
# StarRocks 侧
<项目根目录>/.venv/bin/python starrocks/bench_starrocks.py
# Spark 侧（需 HDFS 在线）
JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64 \
SPARK_HOME=<你的 Spark 安装路径> \
PYSPARK_PYTHON=<项目根目录>/.venv/bin/python \
  <项目根目录>/.venv/bin/python starrocks/bench_spark.py
# 原始数据
starrocks/bench_results/starrocks_bench.json
starrocks/bench_results/spark_bench.json
```
