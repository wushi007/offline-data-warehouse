# 从零跑通：配置到运行

> 本文描述从环境准备到产出数据的完整流程。
> 操作环境为 Linux 或 WSL2。

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

验证环境为一台 16 GB 内存、1 TB 硬盘的 Windows 笔记本，WSL2 + Ubuntu 22.04。单机完成 1.1 亿行源数据的 DWD 构建，其中 2019-10 的 31 天跑通 ODS→ADS 全链路。

| 组件 | 本项目验证过的版本 | 说明 |
|---|---|---|
| 操作系统 | Ubuntu 22.04.5 on WSL2 | 其他 Linux 发行版同理 |
| 内存 | 16 GB（宿主） | WSL2 实际可见约 7.6 GB，见下方「内存说明」 |
| 硬盘 | 1 TB | 源 CSV 约 9 GB；数仓中间结果约 15 GB |
| JDK | 17 | Spark 3.5 要求 JDK 8/11/17，本项目用 17 |
| Python | 3.10.12 | 3.9+ 均可 |
| Spark | 3.5.9（hadoop3 版） | PySpark 3.5.8 |
| Hadoop / HDFS | 3.4.3 | 只需 HDFS，MR 用不到 |
| Hive Metastore | Spark 内嵌 | 通过 `spark.sql.catalogImplementation` 挂载 |
| MySQL | 8.x（端口 3307） | 仅 ADS 落地用到，可选 |
| Airflow | 3.3.0 | 仅调度用到，可选 |

最低可行配置：JDK 17 + Spark + HDFS + Python 环境。MySQL / Airflow / StarRocks 均不是跑通链路的前提。

### 内存说明（WSL2 用户尤其注意）

宿主内存 16 GB，WSL2 默认向虚拟机分配约一半物理内存（未调优 `/etc/wsl.conf` 时实测可见 7.6 GB）。扣除常驻的 HDFS、MySQL、Airflow（约 2.5 GB），Spark 可用余量为 4~5 GB。由此确定以下约束：

| 约束 | 取值 | 原因 |
|---|---|---|
| driver 内存 | `1g`（`SPARK_DRIVER_MEMORY`） | standalone 的 driver 在客户端进程内，按可用内存设置 |
| executor | `4 × 1g × 2 核`（`SPARK_EXECUTOR_*`） | Worker 8 核 / 4g，并行度 8 |
| 同一时刻一个 SparkSession | `etl/run_locked.sh` 的 `flock` 闸门 | 两个作业并存会 OOM |
| 调度并发 = 1 | `max_active_tasks=1` + `max_active_runs=1` | 与闸门共同保证串行 |

> 路径（如 `/home/lst/hadoop-data/warehouse`）集中在 `config/config.py`，换机器时修改该文件即可。内存相关取值可按新机器的可用内存调整。

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

> Airflow 依赖不在此文件中，它安装在独立的 `airflow-venv`，以避免与 Spark 依赖冲突。

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

`run.sh` 已内置这两行，使用 `run.sh` 运行任务时无需另行设置。

---

## 三、启动大数据环境

运行需要 HDFS（存储数据）和 Hive Metastore（Spark 用于注册表）。

### 如果有启停脚本

仓库提供启停脚本的调用方式（脚本本身位于开发环境目录，未纳入本仓库）：

```bash
./start-env.sh            # 启动 MySQL → HDFS → Spark
./start-env.sh status     # 查看状态
./stop-env.sh             # 停止
```

### 如果是全新环境，手动启动 HDFS

```bash
# 1. 格式化 NameNode（仅首次）
$HADOOP_HOME/bin/hdfs namenode -format

# 2. 启动 HDFS
$HADOOP_HOME/sbin/start-dfs.sh

# 3. 验证
jps                       # 应看到 NameNode / DataNode / SecondaryNameNode
curl -s localhost:9870 | head -3
```

验证（运行前确认）：

```bash
ss -ltn | grep -E ':(8020|9870|3307)'    # 8020 NameNode / 9870 WebUI / 3307 MySQL
```

### 手动启动 Spark standalone

```bash
cp conf/spark-env.sh.example $SPARK_HOME/conf/spark-env.sh   # 首次，按本机路径调整
$SPARK_HOME/sbin/start-master.sh
$SPARK_HOME/sbin/start-worker.sh spark://localhost:7077
jps                                                          # 应看到 Master / Worker
ss -ltn | grep -E ':(7077|8081)'                             # Master / Worker WebUI
```

默认连 `spark://localhost:7077`。切本地模式：`SPARK_MASTER=local[6]`。

---

## 四、准备源数据

数据来自 Kaggle [eCommerce Behavior Data](https://www.kaggle.com/datasets/mkechinov/ecommerce-behavior-data-from-multi-category-store)：
`2019-Oct.csv`（约 4,200 万行）与 `2019-Nov.csv`（约 6,700 万行），合计约 1.1 亿行。

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

> 源时间戳为 UTC，影响分区正确性，参见「常见问题」Q3。

---

## 五、修改配置

配置集中在 `config/config.py`，通常只需修改前几行：

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

### 数据库凭据（从环境变量读取）

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

`.env` 已被 `.gitignore` 忽略，不会提交到仓库。

### 环境解析

`run.sh` 启动时会自动解析四类路径，通常无需配置：

| 变量 | 探测顺序 |
|---|---|
| `JAVA_HOME` | 环境变量 → `/usr/lib/jvm/java-17-openjdk-*` → `/usr/lib/jvm/default-java` |
| `SPARK_HOME` | 环境变量 → `~/apps/spark-*` → `/opt/spark*` → `/usr/local/spark*` |
| `HADOOP_HOME` | 环境变量 → `~/apps/hadoop-[0-9]*` → `/opt/hadoop*` → `/usr/local/hadoop*` |
| `HADOOP_CONF_DIR` | 环境变量 → `$HADOOP_HOME/etc/hadoop` |
| Python 解释器 | `PYTHON` 环境变量 → `./.venv` → `../.venv` → `~/.venv` → `python3` |

（只匹配目录，并按版本号取最新，`spark-*.tgz` 安装包不会被选中。）

`SPARK_HOME` 必须解析成功：pyspark 找不到它时会回退到 venv 中的 pyspark 包目录，该目录没有 `conf/spark-defaults.conf`，而 `spark.sql.catalogImplementation=hive` 与 metastore 的 JDBC 连接都配置在该文件中，结果是建表时报 `NOT_SUPPORTED_COMMAND_WITHOUT_HIVE_SUPPORT`。探测全部失败时，`run.sh` 会输出包含原因和处理方式的提示。

> 交互式终端中 `.bashrc` 已导出这些变量；脚本、cron、CI 等非交互场景不加载 `.bashrc`（开头的 "If not running interactively, don't do anything" 会直接 return），此时由 `run.sh` 自行探测。

安装在非常规位置时显式指定：

```bash
SPARK_HOME=/your/spark PYTHON=/path/to/python ./run.sh build --dt 2019-11-01
```

---

## 六、建库建表

```bash
./run.sh ddl
```

执行内容：

- 创建 5 个库（`ods` / `dwd` / `dim` / `dws` / `ads`）
- 创建 19 张表，均为 Hive 外部表 + LOCATION 指向 HDFS
- 存储格式 PARQUET + SNAPPY
- 对 ODS 执行 `MSCK REPAIR TABLE` 注册已有分区

使用外部表时，重建只需 `DROP` + `CREATE`，HDFS 数据文件保留；代价是重建前必须手动清理目录（见「常见问题」）。

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

单个进程、单个 SparkSession 完成 DWD → DWS → DIM → ADS，最后自动跨层对账。复用会话可减少每日多次启动 SparkSession 的开销。

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

### ODS 接入（源数据进入数仓的第一步）

```bash
./run.sh ods-import --dt 2019-11-01   # 单日导入（幂等：分区已存在则跳过）
./run.sh ods-bulk --csv <hdfs路径>     # 批量：单次扫描整份 CSV，切出全部日期分区
./run.sh day 2019-11-01               # 单日完整：导入 → DQC
./run.sh backfill --start D --end D    # 批量回补（逐日）
```

> `ods-bulk` 更快：整份 CSV 只扫描一次，按日期切分分区；逐日导入会重复扫描 31 次。

### 串行闸门（重要）

所有 Spark 调用都经过 `etl/run_locked.sh`：

```bash
exec flock -w "$WAIT" "$LOCK" "$@"
```

同一时刻只允许一个 SparkSession：7.6G 内存的机器上并发运行两个 Spark 作业会 OOM。未获得锁的进程排队等待，默认最长 7200 秒。

不要绕过 `run.sh` 直接执行 `python xxx.py`，否则闸门失效。

---

## 八、验证结果

### 跨层对账

```bash
./run.sh check --start 2019-10-01 --end 2019-10-31
```

输出（实测）：

```
DWD 明细 42,413,557 行 / 购买 742,773 / GMV 229,933,212.63
OK 购买次数守恒 DWD=DWS用户=DWS商品=DWS会话=ADS: 742,773
OK GMV 守恒     DWD=DWS用户=DWS商品=ADS: 229,933,212.63
OK 漏斗不变量（每日 view_uv 为上游最大值）: 0 天违规
OK SCD2 每商品单一当前版本: 0 个违规
OK 分时守恒 DWS=ADS=DWD明细: PV 42,413,557 / 购买 742,773
OK 留存不变量（D+n 人数 ≤ D0）: 0 天违规；其中 7 天 D+7 右删失（NULL）
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

DAG 文件在 `scheduler/`。Airflow 只读 `$AIRFLOW_HOME/dags/`，因此需要创建软链接：

```bash
ln -s /path/to/ecom-warehouse/scheduler/incremental_warehouse_dag.py \
      $AIRFLOW_HOME/dags/incremental_warehouse_dag.py
```

使用软链接而非复制，仓库中的 DAG 修改会直接反映到 Airflow。

### 3. 配置（只需修改一个脚本）

配置集中在 `scheduler/airflow_env.sh`，该脚本生成两项内容：

| 产物 | 谁读它 | 内容 |
|---|---|---|
| Airflow Variable `warehouse_env`（JSON） | DAG | `project_root` / `venv_python` / `java_home` / `spark_home` / `hadoop_home` / `hadoop_conf_dir` / `data_start` / `data_end`（可选覆盖：`timeout_overrides_min` / `quality_tasks` / `quality_warn_tasks` / `default_timeout_min` / `owner`） |
| `$AIRFLOW_HOME/airflow-env.sh` | Airflow 三进程 | `AIRFLOW__*` 环境变量（元数据库、端口、并发、摘代理） |

```bash
bash scheduler/airflow_env.sh --show     # 先看当前解析成什么（只读，不写任何东西）
bash scheduler/airflow_env.sh            # 生成 Variable + env 文件
```

每一项都可以用同名环境变量覆盖，例如 `SPARK_HOME=/opt/spark bash scheduler/airflow_env.sh`。

> `airflow variables import` 的格式要求
>
> `airflow variables import <file>` 会把 JSON 顶层的每个 key 当作独立的 Variable 名，因此配置文件必须包一层，写成：
>
> ```json
> { "warehouse_env": { "spark_home": "/home/lst/apps/spark-3.5.9-bin-hadoop3", "data_start": "2019-10-01" } }
> ```
>
> 不能直接铺平：
>
> ```json
> { "spark_home": "...", "data_start": "..." }   ← 错误
> ```
>
> 铺平后，库中会出现 `spark_home`、`data_start` 等扁平 key，而 DAG 读取的是单个 Variable `warehouse_env`，读不到时静默回退到代码默认值，不报错。症状：`airflow variables get warehouse_env` 报 `does not exist`，但 `variables list` 中有大量相似 key。
>
> `airflow_env.sh` 已按此格式生成，并在导入后回读校验；校验失败会打印 `WARN: ... 导入或回读校验失败`。

> 使用环境变量 / Variable 而非直接修改 `airflow.cfg`（官方推荐做法）：
> `airflow.cfg` 是生成文件，`airflow config` 升级或迁移时会被重写，手工修改会丢失；环境变量（`AIRFLOW__<SECTION>__<KEY>`）优先级最高，可版本化，也可按进程隔离。本机实测 `airflow.cfg` 与官方默认输出逐字节相同，即配置未写入 cfg，全部依赖 `airflow-env.sh` 的 8 个环境变量生效。

DAG 内部的配置优先级（高 → 低），任一层可覆盖下一层：

| 层 | 用途 |
|---|---|
| ① `os.environ` | 临时调试：`export SPARK_HOME=...` |
| ② Airflow Variable `warehouse_env` | 正式配置：WebUI 修改后生效（DAG 重解析后带入） |
| ③ 代码默认值 | clone 到任何目录、Spark/Hadoop 安装在常规位置时无需配置 |

③ 的探测顺序：

| 变量 | 探测顺序 |
|---|---|
| `project_root` | Variable/env → 从 DAG 文件位置推导（`<repo>/scheduler/` 的上两级） |
| `venv_python` | Variable/env → `<repo>/.venv/bin/python` |
| `java_home` | Variable/env → `/usr/lib/jvm/java-17-openjdk-*` → `/usr/lib/jvm/java-17-*` |
| `spark_home` | Variable/env → `~/apps/spark-*` → `/opt/spark*` → `/usr/local/spark*` |
| `hadoop_home` | Variable/env → `~/apps/hadoop-[0-9]*` → `/opt/hadoop*` → `/usr/local/hadoop*` |

> venv 位于仓库外时，推荐在仓库根目录创建软链接，而不是设置变量：
> ```bash
> ln -s /path/to/your/venv .venv
> ```
> `.gitignore` 已包含 `.venv`（含不带斜杠的写法，覆盖软链接的情况），不会被提交。

### 4. 环境守卫脚本

`check_env` 任务执行的 shell 单独放在 `scheduler/check_env.sh`。DAG 加载时会把配置值替换进 `__VENV__` / `__JAVA__` / `__SPARK__` / `__HADOOP__` / `__CONF__` / `__DAGS__` / `__AIRFLOW_HOME__` / `__OWNER_ROOT__` 占位符后执行。

该脚本可单独用 `bash -n` 校验，也可单独运行调试，无需处理 Python 字符串转义。

检查四项，任一不通过即提前失败，并打印需要修改的配置项：
① 解释器存在且包含 pyspark；② JDK / Spark / Hadoop 目录；③ Hadoop 配置目录；④ HDFS NameNode 监听 8020。
启动时会打印解析到的全部路径。

### 5. DAG 结构

一表一个 task（共 21 个 task / 21 条依赖边），依赖链如下：

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

质量闸门失败会阻断下游：`dwd_event_fact_dqc` 不通过时，DWS 和 ADS 均不运行。

### 6. 触发回补

```bash
airflow backfill create --dag-id incremental_warehouse_dag \
    --from-date 2019-11-01 --to-date 2019-11-30 --max-active-runs 1
```

> `max-active-runs 1` 与 flock 闸门共同保证串行。

### 7. 告警

`scheduler/alerts.py` 接入 4+1 个通道，由 DAG 的 `on_failure_callback` 触发：

| 通道 | 默认 | 说明 |
|---|---|---|
| 本地台账 | 开 | `scheduler/alert_records/alerts-YYYY-MM-DD.jsonl` |
| Windows 文件 | 开 | 写到 `/mnt/c/.../数仓告警.txt`（WSL 场景） |
| 桌面弹窗 | 尽力 | 装了 BurntToast 才有 |
| HTTP Webhook | 需配置 | `DW_ALERT_WEBHOOK=<企业微信/钉钉/飞书机器人>` |
| 邮件 SMTP | 需配置 | `DW_ALERT_SMTP_HOST/_PORT/_USER/_PASS/_TO` |

设计原则：告警本身出错不影响任务状态；按级别推送，仅「失败 / 质量阻断 / 超时」推送给人，「重试中」只写入台账；每条告警附带重跑命令和日志尾部。

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

### 性能基准

两侧查询一一对应（Q1~Q7、Q9 共 9 条；Q8/Q10 依赖 StarRocks 内部的 DWS/ADS 表，Spark 侧无对等数据源）。每条查询带结果行数校验：实测行数与预期不符时报错，该行不参与耗时对比。

```bash
# StarRocks 侧（需容器在线：docker start starrocks）
PYTHON=.venv/bin/python ./run.sh table starrocks/bench_starrocks.py

# Spark 侧（需 HDFS 在线；*先停 StarRocks*，两引擎不并行测，避免 7GB 内存被顶爆）
export JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
export SPARK_HOME=<你的 Spark 安装路径>
export PYSPARK_PYTHON=$PWD/.venv/bin/python
.venv/bin/python starrocks/bench_spark.py              # --mode df（默认）→ bench_results/spark_bench.json
.venv/bin/python starrocks/bench_spark.py --mode sql   # Spark SQL 写法 → bench_results/spark_bench_sql.json
```

`--mode df|sql` 仅为写法差异（DataFrame API 与 Spark SQL），语义一致：实测两者结果行数 9/9 相同，耗时差异 −7.4%~+12.7%，无系统性偏差。使用 `--mode sql` 时两侧均为 SQL，对比条件更一致。

单条重测（无需运行全套）：

```bash
.venv/bin/python starrocks/bench_spark.py --only Q7 --mode sql --out /tmp/q7.json
```

> `--only` 不带 `--out` 时只打印、不落盘，避免整套结果被覆盖成一条。

结果 JSON 位于 `starrocks/bench_results/`（现行口径 109.8M 行 / 61 分区）。
[legacy/PERFORMANCE_BENCHMARK.md](legacy/PERFORMANCE_BENCHMARK.md) 为 46.9M 行时期的旧报告，其 1.7×~12× 区间与现行的 3.3×~11× 基于不同数据，不能混用。
连接信息从环境变量读取（`SR_HOST` / `SR_PORT` / `SR_USER` / `SR_PASSWORD`）。

### 导出给 Power BI

```bash
python starrocks/export_ads_powerbi.py [输出目录]
```

输出 UTF-8 带 BOM 的 CSV（保证 Excel / Power BI 打开中文不乱码）。

---

## 十一、常见问题

### Q1：`AnalysisException: [PATH_NOT_FOUND] Path does not exist`

现象：建表时报此错误。

原因：全量重建前清理了 HDFS 目录，但表仍注册在 metastore 中，Spark 建表时校验 LOCATION 是否存在。

解法：`etl/utils.py::drop_path()` 在删除后立即调用 `mkdirs()` 补回空目录：

```python
fs.delete(jpath, True)     # 清内容
if keep_dir:
    fs.mkdirs(jpath)       # 补回空目录，否则建表报 PATH_NOT_FOUND
```

手动清理目录后，执行一次 `hdfs dfs -mkdir -p <path>` 补回即可。

### Q2：`INSERT_COLUMN_ARITY_MISMATCH`

现象：插入数据时报列数不匹配。

原因：外部表 DROP 不删除文件，残留目录（例如旧 RFM 的 `as_of_dt` 分区）会被 Spark 推断进重建后的表，导致列数不一致。

解法：重建前用 `drop_path()` 清理目标目录。

### Q3：日期分区串了（应该 10-01 的数据跑到 10-02）

现象：日期分区偏移一天。

原因：时区差异。源时间戳为 UTC，Spark 会话若使用本地时区（+8），`to_date` 会把每天 16:00–23:59 UTC 的记录归到次日。

解法：`etl/utils.py::get_spark()` 中固定时区：

```python
.config("spark.sql.session.timeZone", "UTC")
```

该配置不可删除。

### Q4：`Py4JNetworkError: Answer from Java side is empty`

现象：作业执行中断，报此错误。

原因：通常为 JVM 被内核 OOM killer 终止，而非代码问题。

排查：

```bash
dmesg | grep -iE "oom-kill|Killed process"
```

解法：降低 driver 内存（`SPARK_DRIVER_MEMORY=1g`）或减少 executor 数；批量任务按层拆分，各起独立进程；使用 `--skip-existing` 断点续跑。

### Q5：重跑同一天，结果和上次不一样（会话数、类目快照漂移）

现象：同一日期重复运行，结果不一致。

原因：去重排序不确定。去重键 `(user_id, event_time, product_id, event_type)` 的所有列都参与分组，组内 `event_time` 必然相同，仅按该列排序无法确定保留哪一条，结果取决于扫描顺序。

解法：`dwd_event_fact.py` 将全部剩余列追加到排序中作为兜底：

```python
tiebreak = ", ".join(["event_time"] + [c for c in FACT_COLUMNS if c not in DEDUP_KEYS + [...]])
```

> 幂等的前提是确定性。窗口函数的 `ORDER BY` 存在并列值时，重跑结果不可重复。

### Q6：`fatal: detected dubious ownership`（WSL 场景）

现象：git 命令报 dubious ownership。

原因：在 Windows 侧通过 `wsl` 命令操作时默认以 root 运行，而文件属主为普通用户。

解法：显式指定用户：

```bash
wsl -d Ubuntu-22.04 -u <你的用户名> -- bash -lc 'cd ~/repo && git ...'
```

属主已被更改时修复：

```bash
sudo chown -R $USER:$USER /path/to/repo
```

### Q7：`run.sh` 报找不到 Python

现象：`run.sh` 启动时报找不到 Python。

原因：仓库中不存在 `.venv`，自动探测未命中。

解法：显式指定：

```bash
PYTHON=/home/<user>/my-spark/.venv/bin/python ./run.sh build --dt 2019-11-01
```

---

## 下一步

- 代码组织与各层职责：见 [CODE_GUIDE.md](CODE_GUIDE.md)
- 数据字典与实测数字：见 [incremental/WAREHOUSE_BUILD.md](incremental/WAREHOUSE_BUILD.md)
- 日常操作：见 [WORKFLOW.md](WORKFLOW.md)
