# 项目错误复盘（PITFALLS）

本项目从搭建到跑通踩过的真实坑，按严重度分级整理。每条：**现象 → 根因 → 修复 → 教训**。
这些既是自查清单，也是面试时"讲一个你排查过的坑"的素材。

---

## 🔴 A. 数据正确性类（最严重，直接导致数据错）

### A1. 分区覆盖默认"整表覆盖"，把历史分区删了
**现象**：第二天跑 DWD 后，第一天的分区从 HDFS 上消失了；RFM 快照永远只看到当天数据。
**根因**：`write.mode("overwrite").partitionBy("event_date")` 默认 `partitionOverwriteMode=static`，
overwrite 会把**整个表**清空再写，而不是只覆盖目标分区。
**修复**：统一开启动态分区覆盖：
```python
.config("spark.sql.sources.partitionOverwriteMode", "dynamic")   # 只覆盖 DataFrame 中存在的分区
```
**教训**：分区表增量写入前，先确认覆盖模式。这是增量数仓最经典的坑之一。
检测方法：写完第二天后 `hdfs dfs -ls <表>` 看分区数是否还在增长。

### A2. 会话时区没固定，`to_date` 跨日偏移（本项目最严重的坑）
**现象**：同一天 11-01，旧 DWD 有 33 万行、新 DWD 有 105 万行，ADS 指标对不上（3 倍差）。
**根因**：源 CSV 时间戳标注 `UTC`，但 Spark 会话默认时区是 `Asia/Shanghai (+8)`，
`to_date(event_time)` 在 +8 下把**每天 16:00-23:59 UTC（傍晚 8 小时）挪到次日分区**，
次日傍晚又被 `dt` 过滤丢弃 → 每天丢约 28% 数据 + 污染下一天。
**定位方法**（关键）：用**原始字符串**统计避开时区干扰——
```python
raw = spark.read.option("header", True).csv(...)          # 不解析时间戳
raw.filter(F.col("event_time").startswith("2019-11-01")).count()  # 1,445,360（真值）
# 对比 ODS 分区 1,047,126 → 确定丢了 398,234 行
```
再用 `spark.sql.session.timeZone="UTC"` 会话看 ODS 各分区的**真实 UTC 日期分布**，确认 11-02 混入了 11-01 的数据。
**修复**：源是 UTC 就锁死 UTC——
```python
.config("spark.sql.session.timeZone", "UTC")
```
**教训**：`to_date`/`date_format`/`hour` 都依赖会话时区。**源数据带时区标注时，必须先固定会话时区**。
面试话术：数据正确性问题 → 用无歧义的 raw-string 统计定位丢失量 → 锁定会话时区 → 重建验证。

### A3. 幂等"跳过"导致修复没生效
**现象**：改了 `utils.py` 加 UTC 时区后重跑 backfill，数据还是旧的错的。
**根因**：`ods_import.py` 有幂等判断——分区存在就跳过导入，所以修复后的导入逻辑根本没执行，
下游 DWD/DWS/ADS 读的还是旧的错误 ODS。
**修复**：先删掉受影响分区再重导——
```bash
hdfs dfs -rm -r /home/lst/hadoop-data/warehouse/ods_event_log/event_date=2019-11-01
```
**教训**：改了"幂等可跳过"的步骤后重跑，记得先确认它真的会重新执行；`hdfs dfs -rm` 定向删除是回补利器。

---

## 🟡 B. 代码实现类

### B1. `createDataFrame` 里传了 Column 对象（`F.lit`）
**现象**：`PySparkTypeError: DateType() can not accept object '2019-11-01' in type 'str'`。
**根因**：`spark.createDataFrame([(...)])` 传的是**具体值**，不是表达式；
日期列还必须传 `datetime.date` 对象，不能传字符串。
**修复**：
```python
from datetime import date as _date
spark.createDataFrame([(_date.fromisoformat(dt), n, ...)], schema=...)   # 不是 F.lit(dt)
```
**教训**：`createDataFrame` 用值，`select/agg` 用 Column，别混。

### B2. `spark.master` 属性不存在
**现象**：`AttributeError: 'SparkSession' object has no attribute 'master'`。
**根因**：master 在 `SparkContext` 上。
**修复**：`spark.sparkContext.master`。
**教训**：PySpark 属性拿不准时先查 API，`SparkSession` 与 `SparkContext` 的边界要清楚。

### B3. 清理死代码引入 `NameError`
**现象**：删掉一段冗余 `import config` 后，`NameError: name 'DWS' is not defined`。
**根因**：被删的 `import config` 恰好是 `DWS` 的间接来源，顶部 import 里只有 `DWD_FACT_PATH, ADS`。
**修复**：顶部补 `from config import DWD_FACT_PATH, DWS, ADS`。
**教训**：删"看起来没用"的 import 前，先全局搜一下它是否被隐式使用。

### B4. Streamlit `use_container_width` 弃用
**现象**：`Please replace use_container_width with width`（1.61 已废弃，2025-12 后移除）。
**修复**：`st.altair_chart(c, width="stretch")`。
**教训**：新版框架 API 变了，出现弃用警告就顺手改掉，别拖。

---

## 🟠 C. 性能调优/诊断类

### C1. 倾斜诊断指标选错 + AQE 掩盖
**现象**：两阶段加盐聚合实验，reducer 侧 max/median 读入比一直是 1.0x，看不出倾斜收益。
**根因**（两个叠加）：
1. **指标选错**：一开始采集 map 侧 `shuffleWriteMetrics`——map 侧写出是按分区均匀的；
   真正的倾斜体现在 **reducer 侧 `shuffleReadMetrics`**（热点 key 所在的 reducer 拉取量巨大）。
2. **AQE 掩盖**：AQE 的 `coalescePartitions` 把最终 reduce 合并到 1 个分区，掩盖了倾斜。
**修复**：采集 `taskMetrics.shuffleReadMetrics.bytesRead`，且实验时临时关 AQE 合并 + 固定分区数：
```python
spark.conf.set("spark.sql.adaptive.coalescePartitions.enabled", "false")
spark.conf.set("spark.sql.shuffle.partitions", "30")   # 让热点 key 冲突显性化
```
**教训**：诊断前先想清楚"这个指标在哪个阶段、哪个侧产生"；AQE 会掩盖问题，诊断倾斜要先关掉它。
这本身也是个知识点：**"诊断倾斜需先关 AQE 合并，否则看不到真实分布"**。

### C2. 小样本下优化收益不显性（诚实面对）
**现象**：倾斜实验用 10% 样本，两阶段聚合因多一次 shuffle 反而更慢（0.54s→1.03s）。
**结论**：不是 bug。小数据量下额外 stage 开销盖过收益，全量（热点 key max/median=1198×）才显性。
**教训**：基准要有"样本量 → 结论"的边界说明，别把小样本结论当全量结论（简历/面试尤其重要）。

---

## 🟢 D. 工程/工具类

### D1. 目标目录 root 所有，写不了
**现象**：`PermissionError: mkdir ... Permission denied`。
**根因**：`/home/lst/my-spark/my-second-project-add` 属主是 root。
**修复**：`sudo chown -R lst:lst <目录>`（需在真实终端输密码，`!` 前缀会话里 sudo 无法交互输密码）。
**教训**：sudo 需要交互式密码时，提示用户在终端执行，别在会话里硬跑。

### D2. 新老项目目录名混淆
**现象**：用户找不到 `view_results.py`。
**根因**：`my-second-project-add`（连字符）vs `my_second_project`（下划线）极易看混。
**教训**：给绝对路径 + 目录对照表，别只给相对名。

### D3. AppTest 里 vega-lite 图表显示为 `UnknownElement`
**现象**：Streamlit `AppTest` 统计不到 `altair_chart`，误以为图表没渲染。
**根因**：AppTest 对 vega-lite 图没有专用访问器，显示为 `UnknownElement`；但 `proto.spec` 里是**合法 Vega-Lite spec**。
**修复**：检查 `UnknownElement` 的 `proto.spec` 是否为合法 spec，或直接看渲染服务日志。
**教训**：测试框架的"看不见" ≠ 应用没渲染，先确认元素类型映射再下结论。

### D4. Streamlit 看板"不刷新"——数据缓存在服务端
**现象**：数仓加载了新一天（11-03）后，看板页面还是旧的（只有 11-01/11-02），F5 刷新也没用。
**根因**：`@st.cache_data` 把加载结果缓存在**服务端进程内存**里，key 是空参数，
**不会感知 HDFS 新增分区**。服务器若在加载新数据之前启动，缓存就一直停在那。
**修复**：
```python
# 看板侧边栏加刷新按钮，清缓存 + 重跑
if st.sidebar.button("🔄 刷新数据"):
    _load.clear()        # 清掉 @st.cache_data 的缓存
    st.rerun()           # 重新执行脚本 → 重新读 HDFS
```
**教训**：Streamlit 的缓存是"服务端 + 按参数 key"，读外部数据源（HDFS/DB）时
**必须提供手动刷新手段**（按钮/TTL），否则用户会以为"没同步"。
面试话术：缓存一致性 —— 外部数据变了，缓存的失效策略要有明确入口。

---

## 自查清单（做增量数仓时先过一遍）

- [ ] 分区写入用 `partitionOverwriteMode=dynamic`，重跑只覆盖目标天
- [ ] 源时间戳带时区 → `session.timeZone` 固定为源时区
- [ ] 改完幂等步骤后重跑，先确认它会真的重新执行（必要时先删分区）
- [ ] 倾斜诊断：先关 AQE 合并，看 reducer 侧 shuffle read
- [ ] 基准结论标注样本量与适用边界
- [ ] 新 DAG 文件放进 Airflow 后，等 `refresh_interval`（本项目 300s）再 `dags list` 确认
- [ ] 目录/文件命名避免新旧项目混淆，文档给绝对路径
