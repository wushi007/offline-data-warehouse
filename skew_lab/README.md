# 数据倾斜实验：复现、错解、正解、AQE 对照

```bash
bash skew_lab/run_all.sh 50000000      # 造数 + 四种方案依次跑 + 汇总
```

## 一、实验设计

场景：5000 万行商品销量明细，按 `product_id` 聚合 GMV / 销量 / 行数，
其中一个爆款商品占 45% 的行。

数据分布（`gen_data.py` 造）：

```
product_id = 9000001          22,499,950 行   45.0%   爆款，热点 key
product_id = 9000002~9000005   各 150 万行     各 3.0%   腰部
其余 40 万个商品                 长尾各 ~90 行
```

四种方案各自独立进程执行，避免缓存与预热相互污染：

| | 方案 | 说明 |
|---|---|---|
| A | `naive` | 原始 `GROUP BY`，复现倾斜 |
| B | `salted_all` | 全量加盐：所有 key 加 64 个盐桶 |
| C | `salted_hot` | 定向加盐：采样定位热点，只给热点 key 加盐 |
| D | `rebalance` | AQE 的 `/*+ REBALANCE(product_id) */` |

控制变量：A/B/C 关闭 AQE 的分区合并，否则 AQE 会把 200 个分区并成约 9 个，
四种方案不在同一起跑线上。

## 二、结果

| 方案 | 总耗时 | task 数 | 最慢 task | 中位 task | 倾斜倍数（全部） | 倾斜倍数（reduce 侧）|
|---|---|---|---|---|---|---|
| A. 原始 groupBy | 4.28 s | 432 | 1,936 ms | 13 ms | 148.92× | 7.50× |
| B. 全量加盐 | 14.13 s | 422 | 6,426 ms | 75 ms | 85.68× | 4.49× |
| C. 定向加盐 + 采样 | 8.63 s | 632 | 2,131 ms | 17 ms | 125.35× | 7.12× |
| D. AQE REBALANCE | 5.38 s | 34 | 2,645 ms | 256 ms | 10.31× | 2.53× |

> 倾斜倍数 = 最慢 task ÷ 中位 task。倾斜时 Spark 只等少数慢 task，总耗时会被稀释，
> 因此以 task 粒度而非总耗时作为判据。

## 三、结论

### 1. 倾斜存在且极端（148.92×）

432 个 task 中 431 个在 13 ms 左右结束，1 个耗时 1,936 ms，5/6 的算力处于等待。

### 2. 全量加盐是反例，慢 3.3 倍

B 方案给所有 key 加 64 个盐桶。表中有 40 万个长尾商品，每个约 90 行，
加桶后中间结果从 40 万组膨胀到 2560 万组（400,005 × 64），
耗时由 4.28 s 增至 14.13 s。加盐只适用于热点 key（C 方案）。

### 3. AQE REBALANCE 最优（148.92× → 10.31×）

D 方案一行 hint，均衡度提升 14 倍，task 数最少（34 个）；
中位 task 从 13 ms 升至 256 ms，不再有大量空转碎 task。
C 方案消除了中间结果膨胀，但两轮 shuffle 的开销抵消了收益，
reduce 侧最慢 task 为 2,131 ms，未见改善。

## 四、规模与代价

在 5000 万行 / 单机 6 核 / 4 秒作业的规模下，倾斜的墙钟代价很小：
最慢 task 1.9 s、总耗时 4.3 s，仅多约 2 秒，任何治理方案引入的额外一轮 shuffle
都比这 2 秒大。倾斜真正致命的情形是热点分区放不下内存发生 spill（10~100 倍劣化），
或作业运行几十分钟、热点 task 独占 executor。

处理顺序：

1. 先按 task 粒度量化，判断倾斜是否真的在拖慢作业
2. 优先考虑架构，把聚合前置到汇总层（本项目 DWS 层即此思路）
3. 再考虑 AQE REBALANCE（零代码改动，均衡度提升 14 倍）
4. 最后才是手写加盐，且必须定向

## 五、文件

| 文件 | 作用 |
|---|---|
| `gen_data.py` | 造带热点 key 的数据（热点占比可调） |
| `common.py` | SparkSession（开事件日志）、任务级指标解析、造数 |
| `bench_skew.py` | 四种方案，`--mode naive\|salted_all\|salted_hot\|rebalance` |
| `run_all.sh` | 一键跑完 |
| `results/skew_bench.json` | 原始指标 |

## 六、已知问题

1. `agg.count()` 会被 Spark 优化掉：行数只取决于分组键时，`sum()` 会被剪掉，
   必须用 `collect()` 强制物化。
2. pip 版 PySpark 没有 `SparkListener`，改用 driver 的事件日志
   （`spark.eventLog.enabled=true`）解析 `SparkListenerTaskEnd`。
   日志中 `Task Info` 无 `Duration` 字段，需用 `Finish Time - Launch Time`；
   shuffle 指标需从 `Total Bytes Read` 拆出 `Local Bytes Read` + `Remote Bytes Read`。
3. REBALANCE hint 不能用 DataFrame API：传字符串报
   `Hint parameter should include columns`，传 `F.col` 报
   `parameters should only contain [str, list, float, int]`，
   只能用 SQL 的 `/*+ REBALANCE(col) */`。
