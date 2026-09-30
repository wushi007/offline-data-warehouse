# 增量数仓建设的踩坑复盘（2026-09-29）

> 本轮把数仓从 0 重建并接上 Airflow 增量调度，共踩 **25 个坑**。每条：**现象 → 根因 → 修法（→ 教训）**。
> 分五类：A 数据正确性 / B 元数据一致性 / C Airflow 与环境 / D 资源性能 / E 我自己犯的方法类。
>
> 姊妹文档：[[WAREHOUSE_BUILD]]（建设过程与结果）；旧项目的坑见 `../legacy/PITFALLS.md`。

---

## 🔴 A. 数据正确性（最严重，会算错数）

### A1. SCD2 有 9.2 万个商品没有「当前版本」
**现象**：拉链校验 `当前版本数 ≠1 的商品 92,844`。
**根因**：用「该版本末次出现日 = 统计窗口末日」判定 `is_current`。
但**商品中途不再出现 ≠ 属性变更**（缺观测不等于变更）——10-31 之前就不再出现的商品全被判成"非当前"。
**修复**：改为「每商品**最后一个版本**即当前版本」，与窗口末日无关：
`ROW_NUMBER() OVER (PARTITION BY product_id ORDER BY dw_start_date DESC) = 1`。
**教训**：SCD2 的"当前"是**版本序列**的属性，不是"和统计窗口对齐"的属性。

### A2. 留存表的列整体串位（本轮最狠）
**现象**：打印出 `D+1 30393.0%` —— 留存率不可能 >100%。
**根因**：`INSERT OVERWRITE TABLE ... PARTITION (...)` **没写列名列表** → Spark 按**位置**映射。
我的 SELECT 是 `d0_uv, d1_uv, d3_uv, d7_uv, d1_rate, d3_rate, d7_rate`，
而表 DDL 是 `d0_uv, d1_uv, d1_rate, d3_uv, d3_rate, d7_uv, d7_rate`
→ `d1_rate` 列里装进了 `d3_uv` 的**计数**（30,393），被按目标列类型 cast 成 double 才显示成 `30393.0%`。
**修复**：输出顺序改为与 DDL 一致，**并加上显式列名列表**，双保险。
**教训**：
1. 位置映射的 INSERT 是**静默错误**——同类型列串位不报错、只算错，比类型不匹配更危险；
2. 能发现它是靠**量纲自检**（留存率 >100% 是业务上不可能的值）→ 打印关键指标时要带一个"合理区间"的意识。

### A3. 去重不确定，重跑结果会漂移
**现象**：改完去重逻辑重跑 10-01，会话数从 268,702 变 268,700。
**根因**：去重键 `(user_id, event_time, product_id, event_type)` 的**所有列都参与分组** →
组内 `event_time` 必然相同 → `ORDER BY event_time` 根本分不出胜负 → 保留哪条取决于扫描顺序。
而组内 `user_session / brand / price` 可能不同 → 下游会话数与类目快照跟着漂。
**修复**：排序追加全部剩余列做兜底（`event_time, category_id, category_code, brand, price, user_session`）。
**教训**：**幂等的前提是确定性**。窗口函数里 `ORDER BY` 有并列时，"重跑可重复"就是假的。

### A4. 右删失被当成 0
**现象**：月末 7 天没有 D+7 数据，留存率算出来是 0。
**根因**：把"未观测"当成了"留存为零"。
**修复**：超出数据区间的窗口输出 **NULL**，如实表达右删失。
**教训**：`NULL`（未观测）和 `0`（观测到为零）在报表上是两件事。

### A5. 把单日观测当结论写进文档
**现象**：文档里写"实测单个商品单日内最多出现 2 种属性组合"，全月一查最大值是 **3**。
**根因**：只在 2019-10-01 一天查过就下了结论。
**修复**：改成全量统计后的 3，并把"样本范围"写进句子。
**教训**：写进文档的**任何数字都要有全量口径的出处**，单日/抽样观测必须标注范围。

---

## 🟡 B. 元数据一致性（同一族：**目录和注册必须一起动**）

外部表的"元数据"在 metastore、"数据"在 HDFS，是两件事。三个坑分别是三个方向：

```
DROP TABLE  → 删注册、不删文件  → 残留文件被推断进新表（arity 不匹配）        B1 的镜像
删文件      → 删文件、不删注册  → 幽灵分区（路径不存在）                      B2
直写路径    → 建文件、不加注册  → 物理有、metastore 没有                      B3
```

### B1. 删了外部表 LOCATION 目录 → 建表报 `[PATH_NOT_FOUND]`
**现象**：`AnalysisException: [PATH_NOT_FOUND] Path does not exist: .../dim/dim_product_scd2`。
**根因**：全量重建前清了 HDFS 目录，但**表还注册在 metastore 上**，Spark 建表时校验 LOCATION 存在。
**修复**：`drop_path()` 删完立刻 `mkdirs()` 补回空目录。

### B2. 幽灵分区（RFM 的 `as_of_dt=2019-10-01`）
**现象**：metastore 注册了 2 个分区，HDFS 上只有 1 个目录。
**根因**：用 `drop_path` 删了整表目录，但**删目录不会删分区注册** →
查这张表（不带分区条件）可能报路径不存在；`MSCK REPAIR` 只补不删，救不了。
**修复**：改让 metastore 自己删 ——
```python
for row in spark.sql(f"SHOW PARTITIONS {TABLE}").collect():   # as_of_dt=2019-10-01
    k, _, v = row[0].partition("=")
    spark.sql(f"ALTER TABLE {TABLE} DROP PARTITION ({k}='{v}')")
```
**教训**：**永远不要用 `rm -rf` 的方式去清一个在 metastore 里有注册的分区表**。

### B3. 直写路径不做分区注册（物理 61 个 / metastore 34 个）
**现象**：`ods-bulk` 导入完，HDFS 上有 61 个分区目录，metastore 里只有 34 个。
**根因**：`df.write.partitionBy("event_date").parquet(路径)` **不是** `INSERT INTO TABLE`，
Hive metastore 不会自动登记这些分区。（ODS 的两个导入脚本都是直写路径 → 都不注册。）
**修复**：写完补 `MSCK REPAIR TABLE`，并**把这一步写进 ODS 脚本自动执行**。
**教训**：只要涉及"外部表 + 直接写文件"，就要问一句"注册跟上了吗"。

### B4. Hive 表「自读自写」被 Spark 拦下
**现象**：`[UNSUPPORTED_OVERWRITE.TABLE] Can't overwrite the target that is also being read from.`
**根因**：增量 merge 要读现有表再覆盖它。
**关键细节**：把表 `cache()` 成 DataFrame 再注册成视图**没用** —— 视图只是别名，分析器照样穿透到表本身。
**修复**：**物化到临时路径**，再从路径 `INSERT OVERWRITE` 回表
（这正是"增量计算 + 小表整表重写"的落地形态；Hive 外部表没有 MERGE/UPSERT）。

---

## 🟠 C. Airflow 与环境（最隐蔽，占了本轮一半时间）

### C1. SOCKS 代理导致 executor 启动不了任何任务（最致命）
**现象**：任务"失败"但 `pid=None`、`run_start_date=None`，**连任务日志都没生成**；CLI 的 `dags trigger` 也失败。
**根因**：`.bashrc` 里 `all_proxy=socks5://127.0.0.1:7897`（clash）。
Airflow 3 的 LocalExecutor 靠 **httpx 调 execution API**（localhost:8090）来启动任务，
httpx 拿去走 SOCKS，而 venv 里没 `socksio`：
```
ImportError: Using SOCKS proxy, but the 'socksio' package is not installed.
```
**注意**：`no_proxy` 白名单**拦不住** `all_proxy`（已实测），只能 unset。
**修复**：写进 `airflow/airflow-env.sh`（`start-airflow.sh:9` 会 source 它 → 组件与 CLI 一处覆盖）：
```bash
unset all_proxy ALL_PROXY http_proxy https_proxy HTTP_PROXY HTTPS_PROXY
```
**教训**：这类问题会让**整台机器上的 Airflow 跑不了任何任务**——本仓库另一个项目的 DAG 一直在这么失败（6 次 failed）。

### C2. `append_env=False` 把 `SPARK_HOME` 弄丢 → 看不到 metastore 的表
**现象**：`AnalysisException: [TABLE_OR_VIEW_NOT_FOUND] dwd.dwd_event_fact cannot be found`，
而且 **只有 DWD 挂、ODS 不挂**。
**根因**：Airflow 的 `BashOperator.get_env()` 是——
```python
env = self.env
if env is None: env = os.environ.copy()
elif self.append_env: system_env.update(env); env = system_env   # ← 默认 append_env=False
```
传了 `env=TASK_ENV` 而默认 `append_env=False` → **任务只拿到这个字典，丢掉系统环境** →
`SPARK_HOME` 丢 → PySpark 的 `_find_spark_home()` 回退到 venv 里的 pyspark 包（**那里没有 `conf/spark-defaults.conf`**）
→ 会话不挂 Hive metastore → 找不到表。
（ODS 两步只读 HDFS 路径、不碰 metastore，所以不报错 —— **"只有某层挂"正是定位的关键线索**。）
**修复**：`append_env=True` **+** 把 `SPARK_HOME`/`HADOOP_CONF_DIR` 显式写进 `TASK_ENV`（不依赖 Airflow 是怎么被启动的）。

### C3. uvloop 在 WSL2 间歇性 abort
**现象**：api-server 崩在 `port:8090: src/unix/linux.c:1441: uv__io_poll: Assertion 'errno == EINTR' failed` 并 core dumped；
3 次启动崩 2 次。api-server 一死，execution API 就没了。
**根因**：uvloop（走 libuv）在 WSL2 上的已知毛刺；uvicorn 是"能 import 就用，否则回退 asyncio"。
**修复**：摘掉 uvloop（`pip show uvloop` 显示 `Required-by:` 为空 → 无依赖方）：
`airflow-venv/bin/pip uninstall -y uvloop`。
**教训**：可选加速组件在受限环境里是**收益小、风险大**。

### C4. 重复 task id → DAG 加载直接报错
**现象**：`DuplicateTaskIdFound: Task id 'ads_user_retention' has already been added to the Dag`。
**根因**：改"一表一 task"时，先用字典推导把所有任务建了一遍（已含 retention/rfm），
后面又用不同日期参数**重建了一次** → 同名 task 加两次。
**修复**：先排除区间型任务再建。
**教训**：**`py_compile` 只查语法，查不出 Airflow 的语义错误**。
正确检查是**在 Airflow 环境里真正加载 DAG 模块**再看 dag 对象（本次就是这么抓到的）。

### C5. `schedule=None` 会被 backfill 拒绝（我第一版就改错了）
**现象**：想把 `schedule` 改成 `None`（历史批数据不该自动跑"今天"），结果 backfill 会失败。
**根因**：Airflow 3 的 `DagNonPeriodicScheduleException` 明确把 **`NullTimetable`（= `schedule=None`）** 列为
"与 backfill 根本不兼容"。改了 `None` 等于把回填这条路堵死。
**修复**：保留周期调度，但把 **DAG 时间窗关在数据区间内**：
```python
schedule="@daily", start_date=datetime(2019,10,1), end_date=datetime(2019,11,30)
```
窗口一关，调度器就不再生成"真实的今天"的 run；而 2019-11 的回填仍在窗口内。
**验证方法**：backfill 的判据就一句 `if not dag.timetable.periodic: raise` ——
直接验运行时类的 `periodic` 属性（`CronTriggerTimetable=True`、`NullTimetable=False`）。

### C6. 新 DAG 默认是暂停的
**现象**：DAG 出现在 UI 里但 backfill 建出的 run 不执行。
**根因**：`dags_are_paused_at_creation = True`（本机实际生效值）。
**修复**：`airflow dags unpause <dag_id>`（或 UI 里点开关）。

### C7. `(dag_id, logical_date)` 唯一 → 同一天不能建两条 run
**现象**：`Duplicate entry ... for key 'dag_run.dag_run_dag_id_logical_date_key'`。
**修复**：重跑要**清掉原 run**而不是再 trigger 一条：
`airflow dags clear <dag_id> --run-id <run_id> -y`。
（顺带：`clear` 会 SIGTERM 掉正在跑的任务，日志里会看到 `exit code -15` —— 属预期。）

### C8. 暂停 DAG 会让进行中的 run 半途而废
调度器不再给暂停 DAG 的 run 派任务，还会去重算它的状态 → **别在 backfill 跑到一半时暂停**。

### C9. `dags test` 绕过 executor，验证不到真实链路
`airflow dags test` 是**进程内**直接执行任务，不经过 LocalExecutor / execution API。
所以验证 C1 的修复时，必须用"clear + 真实调度"，而不是 `dags test`。

---

## 🔵 D. 资源 / 性能

| # | 现象 | 根因 | 修法 |
|---|---|---|---|
| D1 | 反复被内核 OOM killer 杀掉（`dmesg` 实锤 `Killed process (java) anon-rss 3.4~3.6G`） | **多 SparkSession 并存** + 内存账超配 | flock 串行闸门 + `max_active_tasks=1` |
| D2 | `dim_session` 增量必崩 | `cache()` 了 **924 万行**维表 | 改落 parquet 快照，不 cache |
| D3 | 跑到第 10 天整进程挂 | driver 给 **6g**，机器只有 7.6G | 用统一 `get_spark` 配置（不各脚本另设） |
| D4 | 补 30 天要扫 30 遍 9GB CSV ≈ 270GB IO | `ods_import` 每天读整份文件 | `ods_bulk` 一次扫描切出所有分区 |
| D5 | 可用内存只剩 2.8G | 常驻：vscode-server 1.72G + StarRocks 1.33G + Kafka 0.70G + MySQL 0.38G | 停 Kafka/StarRocks（腾 2G） |

**内存账存一笔**（这是所有 OOM 的根）：
```
机器总内存                              7.6 G
基础设施常驻（vscode/StarRocks/Kafka/…） 4.7 G
→ 留给 Spark 的只有                     2.8 G
一个 -Xmx4g 的 Spark 任务实际需要：4G 堆 + 0.5G 元空间/堆外 + 0.3G Python ≈ 4.8 G
→ 超配约 2 G → 换页（swap 用掉 1.7G）→ 内核 OOM
```
关键是**被杀时堆还没满**（RSS 3.4G < 4G 上限）→ 说明是**系统级内存耗尽**，不是 Spark 堆内 OOM。

**CPU 澄清**（本轮的次要收获）：实测 Spark JVM 峰值 **5.87 核**（`local[6]` 上限），整机峰值 6.5/32 核 = 20%。
"CPU 跑满"复现不出来；最可能来自 ① 之前的多会话并发 ② 看的是单进程视角（`top` 显示 590% / htop 单核条形图）
③ 换页抖动被算进系统态。

---

## ⚪ E. 我自己犯的方法类（容易被忽略，但很值得记）

### E1. 查错目录，差点建议跑一个会清库的脚本
**现象**：我报警"NameNode 的 VERSION 文件丢了、可能会被 format 清库"。
**根因**：我查了 `/home/lst/hadoop-data/`（那是个空的本地目录），而 `hdfs-site.xml` 里真实配置是
`/home/lst/data/hadoop/{namenode,datanode}`。**虚惊一场，数据从头到尾是好的。**
**教训**：断言前**先核对配置里的真实路径**。更危险的是——用户的 `start-env.sh` 逻辑是
"VERSION 不存在就 `hdfs namenode -format -force`"，我差一点就让它把整个数仓清掉。

### E2. 测量工具本身没验证
**现象**：CPU 采样器报告 Spark JVM 只用 0.2 核，与"RSS 涨到 3.1G、线程从 92 涨到 169"自相矛盾。
**根因**：我把"jiffies 差 ÷ **全部核**的 jiffies 总和"当成了核数 → 读数被缩小 **32 倍**（真实 6.4 核）。
**定位手段**：用**已知 1 核的负载**校准（实测 103.6 ticks/秒 → 单位是"单核百分比"，公式该除以墙钟秒）。
**教训**：**先验证仪器，再相信数据**。

### E3. `pkill -f` 自伤两次（exit 144）
**根因**：模式字符串出现在自己的命令行里（例如命令里别处也写了 `spark_cpu_mon.py`）→ 把自己的 shell 杀了。
**修法**：用 `pkill -f "[c]pu_mon"` 这种规避写法，或直接用 PID。

### E4. `export PATH=$A/bin:$B/bin` 忘加 `:$PATH` → 基础命令全找不到
`date/sleep/tail/sed/free` 集体 "command not found"。

### E5. stdout 缓冲被误判成"卡住了"
**现象**：`nohup python x.py > log` 之后日志半天不动，以为在等锁。
**根因**：不加 `-u` 时 stdout 攒够一块才刷。
**修法**：改用 **HDFS 产物 / 进程状态**判断真实进度（命令行加 `-u` 也行）。

### E6. 把"照搬"做成了"顺手重构"
**现象**：搬运 ODS DQC 时，我把注释掉的死代码和用户的自留备注一并清理了，被纠正。
**教训**：**搬运 ≠ 重构**。移动文件时只改必需的（import 路径），别动别人留下的笔记。

---

## 附：面试怎么讲这一段

**三个最值得讲的**（都带可复现的证据）：

1. **A2 留存列串位** —— "位置映射的 INSERT 是静默错误：同类型列串位不报错、只算错。
   我靠'留存率不可能 >100%'这个量纲自检发现，修法是显式列名列表。"
2. **C2 `append_env=False`** —— "线索是**只有 DWD 挂而 ODS 不挂**，因为 ODS 只读 HDFS 路径、不碰 metastore；
   顺着这条线查到 `SPARK_HOME` 丢失导致 Spark 会话没挂 metastore。"
3. **B 类三连（B1/B2/B3）** —— 归纳成一句：
   "外部表的元数据和文件是两件事，**目录和注册必须一起动** ——
   DROP TABLE 不删文件、删文件不删注册、直写路径不注册。"

**一句总结本轮**：真正花时间的不是写 SQL，而是**把"看不见的拦路石"逐个挖出来**——
代理、环境变量继承、元数据一致性、内存账。每一步都靠"先取证据再下结论"才定位到根因。
