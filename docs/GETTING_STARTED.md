# 从零跑通：配置到运行

> 目标：拿到这份代码后，从环境准备到跑出数据，一步步走完。
> 本文假设你在一台 Linux / WSL2 机器上操作。

---

## 目录

1. [环境要求](#一环境要求)
2. [安装依赖](#二安装依赖)
3. [启动大数据环境](#三启动大数据环境)
4. [准备源数据](#四准备源数据)
5. [修改配置](#五修改配置)
6. [建库建表](#六建库建表)
7. [跑数据](#七跑数据)
8. [验证结果](#八验证结果)
9. [可选：Airflow 调度](#九可选airflow-调度)
10. [可选：MySQL 落地与 StarRocks 基准](#十可选mysql-落地与-starrocks-基准)
11. [常见问题](#十一常见问题)

---

## 一、环境要求

| 组件 | 本项目验证过的版本 | 说明 |
|---|---|---|
| 操作系统 | Ubuntu 22.04.5（WSL2 亦可） | 其他 Linux 发行版同理 |
| **JDK** | **17** | Spark 3.5 要求 JDK 8/11/17，本项目用 17 |
| Python | 3.10.12 | 3.9+ 均可 |
| **Spark** | **3.5.9**（hadoop3 版） | PySpark 3.5.8 |
| **Hadoop / HDFS** | **3.4.3** | 只需 HDFS，MR 用不到 |
| Hive Metastore | Spark 内嵌 | 通过 `spark.sql.catalogImplementation` 挂载 |
| MySQL | 8.x（端口 3307） | 仅 ADS 落地用到，可选 |
| Airflow | 3.3.0 | 仅调度用到，可选 |

**最低可行配置**：JDK 17 + Spark + HDFS + Python 环境。MySQL / Airflow / StarRocks 都不是跑通链路的前提。

**内存要求**：单机跑 31 天全链路，driver 给 3~4G 即可。
> ⚠️ **driver 内存按机器可用内存给，不要按「任务看起来大」给。**
> 给到 6g 时，7.6G 内存的机器上 JVM 会被内核 OOM killer 杀掉
> （`dmesg` 可见 `Out of memory: Killed process (java)`）。单机逐日任务给 3~4g 足够。

---

## 二、安装依赖

### 1. 创建虚拟环境

```bash
cd /path/to/ecom-warehouse
python3 -m venv .venv
.venv/bin/pip install -r requirements.txt
```

`requirements.txt` 的内容：

```
pyspark==3.5.8      # 核心
pandas / numpy      # 数据处理
matplotlib / streamlit / altair   # 可视化
pymysql             # StarRocks / MySQL 连接
networkx
```

> Airflow 依赖**不在**这里 —— 它装在独立的 `airflow-venv`，避免和 Spark 的依赖打架。

### 2. 确认 Java

```bash
java -version          # 应为 17.x
echo $JAVA_HOME        # 应指向 JDK 17
```

如果不是 17：

```bash
export JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
export PATH="$JAVA_HOME/bin:$PATH"
```

`run.sh` 已经内置了这两行，所以用 `run.sh` 跑任务时不用管。

---

## 三、启动大数据环境

本项目需要 **HDFS**（存数据）+ **Hive Metastore**（Spark 用来注册表）。

### 如果你有启停脚本

仓库自带一套参考脚本的调用方式（脚本本身在开发环境目录，未纳入本仓库）：

```bash
./start-env.sh            # 启动 MySQL → HDFS → Spark
./start-env.sh status     # 查看状态
./stop-env.sh             # 停止
```

### 如果是全新环境，手动起 HDFS

```bash
# 1. 格式化 NameNode（仅首次）
$HADOOP_HOME/bin/hdfs namenode -format

# 2. 启动 HDFS
$HADOOP_HOME/sbin/start-dfs.sh

# 3. 验证
jps                       # 应看到 NameNode / DataNode / SecondaryNameNode
curl -s localhost:9870 | head -3
```

**验证清单**（跑数据前务必确认）：

```bash
ss -ltn | grep -E ':(8020|9870|3307)'    # 8020 NameNode / 9870 WebUI / 3307 MySQL
```

---

## 四、准备源数据

数据来自 Kaggle [eCommerce Behavior Data](https://www.kaggle.com/datasets/mkechinov/ecommerce-behavior-data-from-multi-category-store)：
`2019-Oct.csv`（约 4,200 万行）与 `2019-Nov.csv`。

### 1. 上传到 HDFS

```bash
hdfs dfs -mkdir -p /home/lst/hadoop-data/eCommerce_behavior
hdfs dfs -put 2019-Oct.csv /home/lst/hadoop-data/eCommerce_behavior/
hdfs dfs -ls /home/lst/hadoop-data/eCommerce_behavior/
```

### 2. 源数据字段（9 列）

```
event_time, event_type, product_id, category_id, category_code,
brand, price, user_id, user_session
```

`event_type` 有 4 个枚举值：`view` / `cart` / `remove_from_cart` / `purchase`。

> ⚠️ **时间戳是 UTC**。这一点直接影响分区正确性，见下文「时区」一节。

---

## 五、修改配置

**所有配置集中在一个文件**：`config/config.py`。你需要改的通常只有前几行：

```python
# ---------------- HDFS 路径 ----------------
HDFS_ROOT = "hdfs://localhost:8020"                          # ← 改成你的 NameNode 地址
WAREHOUSE = f"{HDFS_ROOT}/home/lst/hadoop-data/warehouse"     # ← 改成你的数仓根目录
SRC_CSV_2019NOV = f"{HDFS_ROOT}/home/lst/hadoop-data/eCommerce_behavior/2019-Nov.csv"
```

### 其他关键配置

```python
PARTITION_COL = "event_date"        # 全链路统一分区列，不要改
DATA_START = "2019-10-01"           # 默认处理区间
DATA_END   = "2019-10-31"

VALID_EVENT_TYPES = ["view", "cart", "remove_from_cart", "purchase"]
DEDUP_KEYS = ["user_id", "event_time", "product_id", "event_type"]   # 去重键

# DQC 质量闸门阈值
DQC_DIRTY_RATIO_LIMIT = 0.02        # 脏数据比例 >2% 阻断
DQC_GMV_VOLATILITY    = 0.30        # GMV 相对 7 日均波动 >30% 阻断
DQC_ROWS_WARN         = 0.50        # 行数波动 50% 告警
DQC_ROWS_BLOCK        = 0.80        # 行数波动 80% 阻断
```

### 数据库凭据（从环境变量读，不要写死）

```python
MYSQL = {
    "host": os.environ.get("DW_MYSQL_HOST", "127.0.0.1"),
    "port": int(os.environ.get("DW_MYSQL_PORT", "3306")),
    "password": os.environ.get("DW_MYSQL_PASSWORD", ""),   # ← 不写明文
    ...
}
```

复制模板并填写：

```bash
cp .env.example .env
vim .env
```

`.env` 已被 `.gitignore` 忽略，**不会入库**。

### 解释器路径

`run.sh` 会自动探测，顺序：

```
PYTHON 环境变量 → ./.venv → ../.venv → ~/my-spark/.venv → python3
```

需要指定别的解释器时：

```bash
PYTHON=/path/to/python ./run.sh build --dt 2019-11-01
```

---

## 六、建库建表

```bash
./run.sh ddl
```

做了什么：

- 建 **5 个库**（`ods` / `dwd` / `dim` / `dws` / `ads`）
- 建 **19 张表**，全部是 **Hive 外部表 + LOCATION 指向 HDFS**
  （外部表的好处：`DROP TABLE` 不删文件，便于重建）
- 存储格式 **PARQUET + SNAPPY**
- 对 ODS 执行 `MSCK REPAIR TABLE` 注册已有分区

**为什么用外部表**：数仓重建时只需 `DROP` + `CREATE`，HDFS 上的数据文件仍在，
不用担心误删。代价是重建前必须手动清目录（见「常见问题」）。

只重建某张表：

```bash
./run.sh ddl --drop-tables dim.dim_user
```

---

## 七、跑数据

### 方式一：全链路（推荐首次）

```bash
./run.sh build --start 2019-10-01 --end 2019-10-31
```

**一个进程、一个 SparkSession** 跑完 DWD → DWS → DIM → ADS，最后自动跨层对账。
（复用会话是为了省掉每天十几次的 SparkSession 启动开销）

### 方式二：单日增量

```bash
./run.sh build --dt 2019-11-01
```

### 方式三：只跑某层

```bash
./run.sh build --only dwd,dws
```

### 方式四：单张表（调试用）

一张表一个进程，便于单表重试和定位：

```bash
./run.sh dwd --dt 2019-11-01
./run.sh dws --dt 2019-11-01
./run.sh dim --dt 2019-11-01          # dim_user 增量
./run.sh dim-scd2 --dt 2019-11-01     # 商品 SCD2 拉链
./run.sh ads --dt 2019-11-01

# 任意单表
./run.sh table etl/ads/ads_trade_daily.py --dt 2019-11-01
```

### ODS 接入（源数据进数仓的第一步）

```bash
./run.sh ods-import --dt 2019-11-01   # 单日导入（幂等：分区已存在则跳过）
./run.sh ods-bulk --csv <hdfs路径>     # 批量：单次扫描整份 CSV，切出全部日期分区
./run.sh day 2019-11-01               # 单日完整：导入 → DQC
./run.sh backfill --start D --end D    # 批量回补（逐日）
```

> **`ods-bulk` 更快**：整份 CSV 只扫一遍，按日期切分区。逐日导入会重复扫 31 次。

### 串行闸门（重要）

所有 Spark 调用都要经过 `etl/run_locked.sh`：

```bash
exec flock -w "$WAIT" "$LOCK" "$@"
```

**同一时刻只允许一个 SparkSession**。原因：7.6G 内存的机器上并发跑两个 Spark 作业会 OOM。
抢不到锁的进程会排队等待（默认最多等 7200 秒）。

所以：**不要绕过 `run.sh` 直接 `python xxx.py`**，否则闸门失效。

---

## 八、验证结果

### 跨层对账

```bash
./run.sh check --start 2019-10-01 --end 2019-10-31
```

输出（本项目实测）：

```
DWD 明细 42,413,557 行 / 购买 742,773 / GMV 229,933,212.63
✅ 购买次数守恒 DWD=DWS用户=DWS商品=DWS会话=ADS: 742,773
✅ GMV 守恒     DWD=DWS用户=DWS商品=ADS: 229,933,212.63
✅ 漏斗不变量（每日 view_uv 为上游最大值）: 0 天违规
✅ SCD2 每商品单一当前版本: 0 个违规
✅ 分时守恒 DWS=ADS=DWD明细: PV 42,413,557 / 购买 742,773
✅ 留存不变量（D+n 人数 ≤ D0）: 0 天违规；其中 7 天 D+7 右删失（NULL）
```

### 直接查数据

```sql
-- 交易日报
SELECT * FROM ads.ads_trade_daily ORDER BY event_date DESC LIMIT 10;

-- RFM 分群分布
SELECT segment, COUNT(*) users, SUM(monetary) gmv
FROM ads.ads_user_rfm_snapshot GROUP BY segment ORDER BY gmv DESC;

-- SCD2 拉链
SELECT * FROM dim.dim_product_scd2 WHERE product_id = 1004856;
```

### 看某天某商品的历史归属（SCD2 的用法）

```sql
-- 查某天的口径
SELECT * FROM dim.dim_product_scd2
WHERE product_id = 1004856
  AND DATE '2019-10-20' BETWEEN dw_start_date AND dw_end_date;

-- 取当前口径
SELECT * FROM dim.dim_product_scd2
WHERE product_id = 1004856 AND dw_is_current = 1;
```

---

## 九、可选：Airflow 调度

### 1. 安装

```bash
python3 -m venv airflow-venv
airflow-venv/bin/pip install "apache-airflow==3.3.0"
export AIRFLOW_HOME=/path/to/airflow
airflow-venv/bin/airflow db migrate        # 初始化元数据库
airflow-venv/bin/airflow standalone        # 启动（含 Web UI）
```

### 2. 挂 DAG

DAG 文件在 `scheduler/`。Airflow 只读 `$AIRFLOW_HOME/dags/`，所以要做软链接：

```bash
ln -s /path/to/ecom-warehouse/scheduler/incremental_warehouse_dag.py \
      $AIRFLOW_HOME/dags/incremental_warehouse_dag.py
```

用软链接而不是复制，好处是**改仓库里的 DAG 就是改 Airflow 的 DAG**，一份代码两处用。

### 3. DAG 结构

一表一个 task，依赖链如下：

```
check_env
  → ods_import → ods_dqc
      → [dwd_event_fact, dwd_event_dirty]
          → dwd_event_fact_dqc
              → dws_user_session / dws_product / dws_user_behavior / dws_traffic_hour
              → ads_conversion_funnel / ads_user_rfm   （区间型，直接依赖 DQC）
              dws_user_behavior → ads_trade / ads_user_retention
              dws_product       → ads_product_hot_rank
              dws_user_session  → ads_session_behavior
              dws_traffic_hour  → ads_traffic_hour
```

**质量闸门失败即阻断下游** —— `dwd_event_fact_dqc` 不过，DWS/ADS 全部不跑。

### 4. 触发回补

```bash
airflow backfill create --dag-id incremental_warehouse_dag \
    --from-date 2019-11-01 --to-date 2019-11-30 --max-active-runs 1
```

> `max-active-runs 1` 配合 flock 闸门，双保险保证串行。

### 5. 告警

`scheduler/alerts.py` 挂了 4+1 个通道，由 DAG 的 `on_failure_callback` 触发：

| 通道 | 默认 | 说明 |
|---|---|---|
| 本地台账 | 开 | `scheduler/alert_records/alerts-YYYY-MM-DD.jsonl` |
| Windows 文件 | 开 | 写到 `/mnt/c/.../数仓告警.txt`（WSL 场景） |
| 桌面弹窗 | 尽力 | 装了 BurntToast 才有 |
| HTTP Webhook | 需配置 | `DW_ALERT_WEBHOOK=<企业微信/钉钉/飞书机器人>` |
| 邮件 SMTP | 需配置 | `DW_ALERT_SMTP_HOST/_PORT/_USER/_PASS/_TO` |

设计原则：**绝不抛异常**（告警本身出错不能搞乱任务状态）、**分级推送**
（只有「失败/质量阻断/超时」推给人，「重试中」只进台账，避免噪音淹没真问题）、
**每条告警自带重跑命令 + 日志尾部**（收到就能直接动手）。

自测：

```bash
python scheduler/alerts.py --selftest     # 往台账写一条测试记录
python scheduler/alerts.py                # 查看当前状态
```

详见 [incremental/ALERTS.md](incremental/ALERTS.md)。

---

## 十、可选：MySQL 落地与 StarRocks 基准

### MySQL（ADS 结果供报表读取）

在 `.env` 里配好 `DW_MYSQL_*`，然后建库：

```sql
CREATE DATABASE dw_ads CHARACTER SET utf8mb4;
CREATE USER 'warehouse'@'%' IDENTIFIED BY '<你的密码>';
GRANT ALL ON dw_ads.* TO 'warehouse'@'%';
```

### StarRocks 性能基准

```bash
PYTHON=.venv/bin/python ./run.sh table starrocks/bench_starrocks.py
```

对比结果见 [legacy/PERFORMANCE_BENCHMARK.md](legacy/PERFORMANCE_BENCHMARK.md)。
连接信息从环境变量读（`SR_HOST` / `SR_PORT` / `SR_USER` / `SR_PASSWORD`）。

### 导出给 Power BI

```bash
python starrocks/export_ads_powerbi.py [输出目录]
```

输出 UTF-8 **带 BOM** 的 CSV（保证 Excel / Power BI 打开中文不乱码）。

---

## 十一、常见问题

### Q1：`AnalysisException: [PATH_NOT_FOUND] Path does not exist`

**原因**：全量重建前清了 HDFS 目录，但表还注册在 metastore 上，Spark 建表时校验 LOCATION 存在。

**解法**：`etl/utils.py::drop_path()` 已经处理了 —— 删完立刻 `mkdirs()` 补回空目录：

```python
fs.delete(jpath, True)     # 清内容
if keep_dir:
    fs.mkdirs(jpath)       # 补回空目录，否则建表报 PATH_NOT_FOUND
```

如果你手动清了目录，补一次即可：`hdfs dfs -mkdir -p <path>`。

### Q2：`INSERT_COLUMN_ARITY_MISMATCH`

**原因**：外部表 DROP 不删文件，旧实现的残留目录
（比如旧 RFM 的 `as_of_dt` 分区）会被 Spark 推断进重建的表，列数对不上。

**解法**：重建前用 `drop_path()` 清干净目标目录。

### Q3：日期分区串了（应该 10-01 的数据跑到 10-02）

**原因**：时区。源时间戳是 UTC，如果 Spark 会话用本地时区（+8），
`to_date` 会把每天 16:00–23:59 UTC 的记录挪到次日。

**解法**：`etl/utils.py::get_spark()` 里固定了时区：

```python
.config("spark.sql.session.timeZone", "UTC")
```

**这个配置不能删。**

### Q4：`Py4JNetworkError: Answer from Java side is empty`

**原因**：大概率是 JVM 被内核 OOM killer 杀了，不是代码问题。

**排查**：

```bash
dmesg | grep -iE "oom-kill|Killed process"
```

**解法**：把 driver 内存降下来（`get_spark(memory="3g")`）；
批量任务拆层、各起独立进程；用 `--skip-existing` 断点续跑。

### Q5：重跑同一天，结果和上次不一样（会话数、类目快照漂移）

**原因**：去重的排序**不确定**。去重键 `(user_id, event_time, product_id, event_type)`
的所有列都参与分组，所以**组内 `event_time` 必然相同**，只按它排序分不出胜负 ——
保留哪条取决于扫描顺序。

**解法**：`dwd_event_fact.py` 已把**全部剩余列**追加进排序做兜底：

```python
tiebreak = ", ".join(["event_time"] + [c for c in FACT_COLUMNS if c not in DEDUP_KEYS + [...]])
```

> **幂等的前提是确定性。** 窗口函数里 `ORDER BY` 有并列时，「重跑可重复」就是假的。

### Q6：`fatal: detected dubious ownership`（WSL 场景）

**原因**：在 Windows 侧用 `wsl` 命令操作时**默认以 root 运行**，而文件属主是普通用户。

**解法**：始终指定用户：

```bash
wsl -d Ubuntu-22.04 -u <你的用户名> -- bash -lc 'cd ~/repo && git ...'
```

属主已被污染时修复：

```bash
sudo chown -R $USER:$USER /path/to/repo
```

### Q7：`run.sh` 报找不到 Python

**原因**：仓库里没有 `.venv`，且自动探测没命中。

**解法**：显式指定：

```bash
PYTHON=/home/<user>/my-spark/.venv/bin/python ./run.sh build --dt 2019-11-01
```

---

## 下一步

- 想知道**代码怎么组织的、每层在干什么** → 看 [CODE_GUIDE.md](CODE_GUIDE.md)
- 想知道**数据字典与实测数字** → 看 [incremental/WAREHOUSE_BUILD.md](incremental/WAREHOUSE_BUILD.md)
- **日常怎么干活** → 看 [WORKFLOW.md](WORKFLOW.md)
