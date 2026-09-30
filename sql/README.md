# sql/ —— 纯 SQL 通道

把「一条 SQL 就能表达完」的任务写成 `.sql` 文件，交给 `spark-sql` 直接执行，
不经过 Python。

```
sql/
├── README.md                        ← 本文件
└── dqc/
    └── dwd_event_fact_dqc.sql       ← DWD 质量闸门（7 项检查）
```

## 一、三种「提交」方式的区别

| 方式 | 它到底是什么 | 本项目适用性 |
|---|---|---|
| **`spark-sql -f x.sql`** | Spark 自带的 SQL CLI。起一个 SparkSession → 顺序执行文件里的语句 → 退出 | ✅ **推荐**。与「单会话、低内存」的约束一致，跑完即释放 |
| `spark-submit` | 提交 **JVM / Python 应用**（打好的 jar 或 `.py`）。它**不认识 `.sql`** | ❌ 想用这条路跑 SQL，必须先写一个读文件再 `spark.sql()` 的 main —— 等于回到 Python |
| `beeline -f x.sql` | JDBC 客户端，要连一个**常驻**的 HiveServer2 / Spark Thrift Server | ⚠️ 多一个常驻 Driver（1G+），与本机内存约束冲突；且要额外维护这个服务 |

结论：**写 `.sql` + `spark-sql -f`**，走 `./run.sh sql`。

## 二、用法

```bash
# 仓库根目录下执行
./run.sh sql sql/dqc/dwd_event_fact_dqc.sql --dt 2019-10-01
```

`./run.sh sql` 会自动做四件事：

1. 把 `--dt D` 转成 `--hivevar dt=D`（SQL 里用 `${dt}` 引用）
2. 从 `config/config.py` 读出 `ODS_PATH`，注入成 `--hivevar ods_path=...`
3. 带上 6 个必需的会话配置（见第四节「配置缺口」）
4. 过 `etl/run_locked.sh` 的 flock 串行闸门 —— `spark-sql` 起的也是完整
   SparkSession，一样吃内存，必须和 Python 通道共用同一把锁

其余参数原样透传给 `spark-sql`，所以也可以自己加东西：

```bash
./run.sh sql sql/dqc/dwd_event_fact_dqc.sql --dt 2019-10-01 --hivevar env=prod
```

**不走 run.sh 时**要自己把参数和配置带全 —— 并且**用 `$SPARK_HOME/bin/spark-sql`，
别用裸命令 `spark-sql`**（原因见下方「另一个坑」）：

```bash
export SPARK_HOME=/home/lst/apps/spark-3.5.9-bin-hadoop3
"$SPARK_HOME/bin/spark-sql" \
    --hivevar dt=2019-10-01 \
    --hivevar ods_path=hdfs://localhost:8020/home/lst/hadoop-data/warehouse/ods_event_log \
    --conf spark.sql.session.timeZone=UTC \
    --conf spark.sql.sources.partitionOverwriteMode=dynamic \
    --conf spark.hadoop.fs.defaultFS=hdfs://localhost:8020 \
    -f sql/dqc/dwd_event_fact_dqc.sql
```

> **另一个坑：裸 `spark-sql` 可能不是你想的那个 Spark。**
> 实测这台机器上装着**两份 Spark**：
>
> | 命令 | 版本 | 有 `conf/spark-defaults.conf`？ |
> |---|---|---|
> | `$SPARK_HOME/bin/spark-sql` ← 项目用的 | 3.5.9 | ✅ 有（Hive metastore 配置在里面） |
> | 裸 `spark-sql`，命中 `~/.local/bin/spark-sql` ← pip 装 pyspark 时带进来的 | 3.5.8 | ❌ 没有（`/usr/local/bin` 下还有一份同源的） |
>
> 非交互 shell 里 `SPARK_HOME` 会被 `.bashrc` 开头那句
> 「If not running interactively, don't do anything」守卫拦掉，
> 于是裸 `spark-sql` 顺着 PATH 落到 `~/.local/bin/spark-sql`
> （它排在 `/usr/local/bin` 前面）。那一份没有 `spark-defaults.conf`，
> **没有 Hive metastore**，查询直接报
> `TABLE_OR_VIEW_NOT_FOUND: dwd.dwd_event_fact`
> —— 看着像「表没建」，实际是找错了 Spark，极容易被误导。
> `./run.sh sql` 用的是 `$SPARK_HOME/bin/spark-sql`，不会踩到。

> **`.sql` 的固有代价：它没法 `import config/config.py`。**
> 任何环境相关的值（路径、连接串）只有两个选择：写死在 SQL 里，或者由调用方
> 通过 `--hivevar` 注入。本项目选后者 —— 所以 `run.sh sql` 才需要去 config.py
> 读 `ODS_PATH`。写新 SQL 时如果用到别的路径，照这个模式加一个参数即可，
> **不要在文件里写死第二份**。

## 三、三个纯 SQL 机制（替代原 Python 写法）

| 需求 | Python 版 | 纯 SQL 版 |
|---|---|---|
| 传参 | `argparse --dt` | `--hivevar dt=X` + SQL 里 `${dt}` |
| 读配置里的路径 | `from config.config import ODS_PATH` | **做不到** —— 由 `run.sh sql` 读出后 `--hivevar ods_path=X` 注入 |
| 按 HDFS 路径读成视图 | `spark.read.parquet(p).createOrReplaceTempView("t")` | `CREATE OR REPLACE TEMPORARY VIEW t USING parquet OPTIONS (path '...')` |
| 校验不通过 → 进程非 0 退出 | `sys.exit(1)` | `SELECT raise_error('...') FROM (只有失败时才产生行的子查询) x` |

实测确认的几个细节：

- **`--hivevar` 能用，`--conf` 不能用。** `--conf dt=X` 会警告
  `Ignoring non-Spark config property: dt`；`--conf spark.sql.variable.dt=X`
  被接受但不参与替换（`${dt}` 被替换成空串，**不报错**，最坑）。
  会话内 `SET dt=X;` 也可以，但 `-f` 文件方式用不上。
- **`${dt}` 会替换在字符串字面量里**，所以 `OPTIONS (path '.../event_date=${dt}')`
  这种写法直接可用。
- **`raise_error()` 是惰性求值** —— 只有被求值到的行才会抛。
  这是纯 SQL 能自己决定进程成败的关键：把闸门写成
  「子查询只有存在 BLOCK 行时才出结果行」，没失败就一行不出、不抛、退出码 0。
  反例：把 `raise_error` 放在无条件 `SELECT` 的投影里，会每次都失败。
- **Spark 3.5 不支持 `ASSERT` 语句**（实测 `PARSE_SYNTAX_ERROR`），别想着用它做断言。
- **SQL 报错会立即终止 `-f` 的执行**并让进程以非 0 退出，后面的语句不再跑。
  所以闸门放在倒数第二个语句、成功提示放最后，语义就对了。

## 四、★ 配置缺口（最容易踩的地方）

`$SPARK_HOME/conf/spark-defaults.conf` 里只有这些：

```
spark.master / spark.driver.memory / spark.sql.adaptive.* /
spark.sql.shuffle.partitions / spark.serializer /
spark.sql.catalogImplementation=hive + metastore JDBC 那一组
```

而下面 4 项**原本只写在 `etl/utils.py` 的 `get_spark()` 里** —— 那是 Python
通道的会话配置，`spark-sql` 完全不知道：

| 配置 | 不设时的实际默认 | 后果（实测） |
|---|---|---|
| `spark.sql.session.timeZone` | `Asia/Shanghai`（系统时区） | `TO_DATE(event_time)` 把每天 16:00-23:59 UTC 的事件算到次日。**2019-10-01 分区上，第 7 项检查报出 337,080 行「错位」**，而设成 UTC 后是 0 |
| `spark.sql.sources.partitionOverwriteMode` | `STATIC` | 增量 `INSERT OVERWRITE`（不带 `PARTITION` 子句那种写法）会**覆盖整张表**，历史分区全丢 |
| `spark.hadoop.fs.defaultFS` | 本地文件系统 | `parquet` 路径按本地文件系统解析，读不到 HDFS |
| `spark.driver.memory` | `3g` | 与 Python 通道的 `4g` 不一致 |

`./run.sh sql` 已把这 6 项固定带上（`run.sh` 里的 `SQL_CONF`）。

> **维护约定**：改 `etl/utils.py` 的 `get_spark()` 时，必须同步改 `run.sh` 的
> `SQL_CONF`。两个通道的会话配置不一致，同一条逻辑就会算出两种结果 ——
> 而且是**静默**不一致，只会在对账时以「两边数字对不上」的形式暴露。

## 五、什么适合写成 `.sql`，什么不适合

| ✅ 适合 | 原因 |
|---|---|
| 单条 SQL 能表达完的转换 | 它本来就是一条 `SELECT` / `INSERT OVERWRITE` |
| 质量校验（DQC） | 判定逻辑 + `raise_error` 闸门就够，不需要 Python |
| 例行聚合报表（ADS 日报） | 纯聚合、无状态、无副作用 |

| ❌ 不适合 | 原因 |
|---|---|
| 需要先清 HDFS 目录的（`utils.drop_path`） | SQL 没有「删目录再补回空目录」这种操作 |
| 区间 / 快照型（留存、RFM） | 要跨日循环、按天迭代，本身就是迭代逻辑 |
| 全链路编排（`pipeline.py`） | 编排是逻辑，不是 SQL |
| ODS 导入 | 要先扫 CSV 切分、自己管幂等 |

判断标准一句话：**如果这个任务需要「先做一件 SQL 之外的事」，它就不适合纯 SQL。**

## 六、加一个新的 `.sql`

1. 放到 `sql/<用途>/<名字>.sql`
2. 文件头写清五件事：**用法 / 退出码 / 参数 / 不可动的实现约束 / 依赖的会话配置**
3. 需要日期就用 `'${dt}'`；别的参数走 `--hivevar key=val`
4. 需要「失败即非 0 退出」就照抄 `dqc/dwd_event_fact_dqc.sql` 的闸门写法：

```sql
SELECT raise_error(msg) AS gate
FROM (
    SELECT CONCAT('❌ BLOCK [', '${dt}', ']：', CONCAT_WS(', ', COLLECT_LIST(code))) AS msg
    FROM v_dqc WHERE status = 'BLOCK'
    HAVING COUNT(*) > 0        -- ← 这一行保证「没有失败项时一行都不出」
) x;
```

5. 测试至少覆盖三种情况：**正常通过 / 应阻断 / 参数缺失**。
   前两种可以直接用现成数据：
   - 正常：`--dt 2019-10-01`（ODS 与 DWD 都有）
   - 应阻断：`--dt 2019-11-05`（ODS 有、DWD 未建）

## 七、在 Airflow 里挂一个 SQL 任务

`run.sh` 内部已 `cd` 到仓库根，所以传相对路径即可：

```python
# scheduler/incremental_warehouse_dag.py
SQL_STEPS = {"dwd_dqc_sql": "sql/dqc/dwd_event_fact_dqc.sql"}

BashOperator(
    task_id="dwd_dqc_sql",
    bash_command=f"{PROJECT_ROOT}/run.sh sql {SQL_STEPS['dwd_dqc_sql']} --dt {{{{ ds }}}}",
    env=TASK_ENV,
    append_env=True,          # 见 DAG 里 TASK_ENV 上方的说明，别漏
    retries=1,
    execution_timeout=timedelta(minutes=20),
)
```

非 0 退出会被 DAG 当作任务失败；若它承担质量闸门职责，记得把 task_id 加进
`QUALITY_TASKS`，这样告警措辞才会是「数据质量不达标，已阻断下游」而不是「脚本崩了」。
