-- =============================================================================
-- dwd_event_fact_dqc.sql —— DWD 层质量闸门（纯 SQL 版）
-- =============================================================================
-- 用法
--   ./run.sh sql sql/dqc/dwd_event_fact_dqc.sql --dt 2019-10-01
--     ↑ 自动补上 ods_path 与「会话配置」两样东西，这是推荐用法
--
--   直接调 spark-sql 时要自己带全，并且**要用 $SPARK_HOME/bin/spark-sql**：
--   export SPARK_HOME=/home/lst/apps/spark-3.5.9-bin-hadoop3
--   "$SPARK_HOME/bin/spark-sql" \
--       --hivevar dt=2019-10-01 \
--       --hivevar ods_path=hdfs://localhost:8020/home/lst/hadoop-data/warehouse/ods_event_log \
--       --conf spark.sql.session.timeZone=UTC \
--       -f sql/dqc/dwd_event_fact_dqc.sql
--   ★ 别用裸 `spark-sql`：非交互 shell 里 SPARK_HOME 会丢，它会顺着 PATH
--     落到 ~/.local/bin/spark-sql（pip 装 pyspark 时带进来的 3.5.8），
--     那份没有 conf/spark-defaults.conf → 没有 Hive metastore → 报
--     TABLE_OR_VIEW_NOT_FOUND（看着像表没建，其实是找错了 Spark）。
--     ./run.sh sql 用的是 $SPARK_HOME/bin/spark-sql，不会踩到。
--
-- 退出码
--   0 = 7 项全部 PASS
--   1 = 存在 BLOCK —— Airflow 据此阻断下游，并归类为
--       「数据质量不达标，已阻断下游」（见 DAG 的 QUALITY_TASKS）
--
-- 与 etl/dwd/dwd_event_fact_dqc.py 的关系
--   两者口径完全相同（同一套 7 项），只是承载形式不同：一个是 Python 拼 SQL，
--   一个是纯 SQL 文件。可以并行保留、互相校验 —— 同一 dt 跑出来应逐项一致。
--
-- 参数（两个都必填，缺失或非法会立刻失败，见下方「参数守卫」）
--   dt        --hivevar dt=YYYY-MM-DD
--             日期。缺失会让查询静默落到 event_date='' 上，再误报「分区非空」。
--   ods_path  --hivevar ods_path=<ODS 分区根的路径>
--             ODS 分区根（不含 event_date= 那一段）。
--             ★ 为什么不写死在文件里：.sql 没法 import config/config.py。
--             所以路径改由调用方注入，./run.sh sql 会自动从 config.py 读取，
--             保证与 etl/dwd/dwd_event_fact_dqc.py 用的是同一个值 ——
--             否则改了 HDFS 路径只改一处，两边就会各读各的。
-- =============================================================================
--
-- ★ 三条不可改的实现约束（与 .py 版一致，改动前先读）★
--
--   ① ODS 行数走临时视图 t_ods（按 HDFS 路径读），**不要**改成
--      `FROM ods.ods_event_log`。表读依赖 MSCK REPAIR 成功，而 ODS 导入里的
--      MSCK 是 try/except 只警告（etl/ods/ods_event_log.py:55）；一旦静默失败
--      就会读到 0 行 → 误报「分区非空」为 BLOCK，而且看起来像数据问题，极难查。
--
--   ② 所有 SUM 都包 COALESCE(...,0)。空分区时 SUM 返回 NULL，而
--      `NULL = 0` 既不真也不假 → CASE WHEN 落到 ELSE，把空分区误判成 BLOCK。
--
--   ③ 闸门用 raise_error()，并且必须放在「只有存在 BLOCK 行时才有结果行」的
--      子查询里。raise_error 是**惰性求值**：没有结果行就不会被求值、不会抛；
--      放在恒被求值的位置（如无条件 SELECT 的投影里）会每次都失败。
--      Spark 3.5 不支持 ASSERT 语句（实测 PARSE_SYNTAX_ERROR），别改用它。
--
-- ★ 会话配置：调用方必须带上，spark-defaults.conf 里没有 ★
--
--   spark.sql.session.timeZone=UTC
--     不设就退回系统时区（本机实测 Asia/Shanghai），TO_DATE(event_time) 会把
--     每天 16:00-23:59 UTC 的事件算到次日 → 第 7 项会抛 33 万余行「错位」。
--     ./run.sh sql 已自动带上；直接调 spark-sql 时必须自己加。
--
--   spark.sql.sources.partitionOverwriteMode=dynamic
--     不设就是 STATIC，增量 INSERT OVERWRITE 会覆盖整张表而不是单日分区。
--     本文件只读不写，用不到；但同一会话里若还跑落表语句就必须带上。
-- =============================================================================


-- ---------------------------------------------------------------------------
-- 0. 参数守卫：dt 或 ods_path 缺失/非法 → 立即失败，不进入校验
--    写在最前面，避免用空值去读不存在的分区路径，把「路径不存在」这类
--    基础设施问题和「数据质量不达标」混为一谈。
-- ---------------------------------------------------------------------------
SELECT raise_error(CONCAT('启动参数不完整或非法：dt="', '${dt}',
                          '"，ods_path="', '${ods_path}',
                          '"。请用 --hivevar dt=YYYY-MM-DD ',
                          '--hivevar ods_path=<ODS 分区根路径> 传入。')) AS param_guard
FROM (SELECT 1) t
WHERE '${dt}' NOT RLIKE '^[0-9]{4}-[0-9]{2}-[0-9]{2}$'
   OR '${ods_path}' = '';


-- ---------------------------------------------------------------------------
-- 1. ODS 按 HDFS 路径读成临时视图（约束 ①）
--    CREATE OR REPLACE TEMPORARY VIEW 是 createOrReplaceTempView 的纯 SQL 等价物。
--    路径由 ods_path 注入（见文件头「参数」）。
-- ---------------------------------------------------------------------------
CREATE OR REPLACE TEMPORARY VIEW t_ods
USING parquet
OPTIONS (path '${ods_path}/event_date=${dt}');


-- ---------------------------------------------------------------------------
-- 2. 判定：7 项检查各输出一行 (seq, code, item, status, detail)
--    加检查项 = 加一个 UNION ALL 分支，不用碰任何其它文件。
-- ---------------------------------------------------------------------------
CREATE OR REPLACE TEMPORARY VIEW v_dqc AS
WITH m AS (
    -- 事实表只扫一遍，算出全部行级指标
    SELECT COUNT(*)                                                          AS fact_cnt,
           SUM(CASE WHEN event_date <> TO_DATE(event_time) THEN 1 ELSE 0 END) AS tz_bad,
           SUM(CASE WHEN price < 0 THEN 1 ELSE 0 END)                        AS neg_price,
           SUM(CASE WHEN event_type NOT IN ('view', 'cart', 'remove_from_cart', 'purchase')
                    THEN 1 ELSE 0 END)                                       AS bad_enum,
           SUM(CASE WHEN event_time IS NULL OR user_id IS NULL
                     OR product_id IS NULL OR event_type IS NULL
                    THEN 1 ELSE 0 END)                                       AS null_key,
           DATE_FORMAT(MIN(event_time), 'yyyy-MM-dd HH:mm:ss')                AS min_t,
           DATE_FORMAT(MAX(event_time), 'yyyy-MM-dd HH:mm:ss')                AS max_t
    FROM dwd.dwd_event_fact
    WHERE event_date = '${dt}'
),
dup AS (
    -- 去重键出现重复的分组数
    SELECT COUNT(*) AS dup_cnt FROM (
        SELECT user_id, event_time, product_id, event_type
        FROM dwd.dwd_event_fact
        WHERE event_date = '${dt}'
        GROUP BY user_id, event_time, product_id, event_type
        HAVING COUNT(*) > 1
    ) t
),
base AS (
    -- 汇总口径。SUM 一律 COALESCE 兜底空分区 —— 见约束 ②
    SELECT (SELECT COUNT(*) FROM t_ods)                                      AS ods_cnt,
           (SELECT COUNT(*) FROM dwd.dwd_event_dirty
             WHERE event_date = '${dt}')                                     AS dirty_cnt,
           COALESCE(m.fact_cnt,  0)  AS fact_cnt,
           COALESCE(m.tz_bad,    0)  AS tz_bad,
           COALESCE(m.neg_price, 0)  AS neg_price,
           COALESCE(m.bad_enum,  0)  AS bad_enum,
           COALESCE(m.null_key,  0)  AS null_key,
           m.min_t,
           m.max_t,
           dup.dup_cnt
    FROM m CROSS JOIN dup
)
SELECT seq, code, item, status, detail FROM (
    SELECT 1 AS seq, 'count_overflow' AS code, '账对平' AS item,
           CASE WHEN b.ods_cnt >= b.fact_cnt + b.dirty_cnt THEN 'PASS' ELSE 'BLOCK' END AS status,
           CONCAT('ODS=', FORMAT_NUMBER(b.ods_cnt, 0),
                  ' = DWD=', FORMAT_NUMBER(b.fact_cnt, 0),
                  ' + 脏=', FORMAT_NUMBER(b.dirty_cnt, 0),
                  ' + 去重=', FORMAT_NUMBER(b.ods_cnt - b.fact_cnt - b.dirty_cnt, 0)) AS detail
    FROM base b
    UNION ALL
    SELECT 2, 'empty_partition', '分区非空',
           CASE WHEN b.fact_cnt > 0 THEN 'PASS' ELSE 'BLOCK' END,
           CONCAT('DWD 行数=', FORMAT_NUMBER(b.fact_cnt, 0))
    FROM base b
    UNION ALL
    SELECT 3, 'dup_key', '去重键唯一',
           CASE WHEN b.dup_cnt = 0 THEN 'PASS' ELSE 'BLOCK' END,
           CONCAT('重复组=', FORMAT_NUMBER(b.dup_cnt, 0))
    FROM base b
    UNION ALL
    SELECT 4, 'null_core_field', '核心字段空值',
           CASE WHEN b.null_key = 0 THEN 'PASS' ELSE 'BLOCK' END,
           CONCAT('空值行=', FORMAT_NUMBER(b.null_key, 0))
    FROM base b
    UNION ALL
    SELECT 5, 'bad_event_type', '枚举越界',
           CASE WHEN b.bad_enum = 0 THEN 'PASS' ELSE 'BLOCK' END,
           CONCAT('越界行=', FORMAT_NUMBER(b.bad_enum, 0))
    FROM base b
    UNION ALL
    SELECT 6, 'negative_price', '价格非负',
           CASE WHEN b.neg_price = 0 THEN 'PASS' ELSE 'BLOCK' END,
           CONCAT('负值行=', FORMAT_NUMBER(b.neg_price, 0))
    FROM base b
    UNION ALL
    SELECT 7, 'timezone_offset', '时区一致',
           CASE WHEN b.tz_bad = 0 THEN 'PASS' ELSE 'BLOCK' END,
           -- 空分区时 min_t/max_t 为 NULL，CONCAT 整体返回 NULL → 用 COALESCE
           -- 兜成可读文案，否则打印出来是 "NULL"，看着像出了问题（判定其实是对的）
           COALESCE(CONCAT('错位行=', FORMAT_NUMBER(b.tz_bad, 0),
                           ' | 事件时间 ', b.min_t, ' ~ ', b.max_t),
                    '无数据（分区为空）') AS detail
    FROM base b
) c;


-- ---------------------------------------------------------------------------
-- 3. 报告：每项一行，带 ✅/❌ 便于肉眼扫
-- ---------------------------------------------------------------------------
SELECT CONCAT(CASE WHEN status = 'PASS' THEN '✅' ELSE '❌' END,
              ' ', item, '：', detail) AS result
FROM v_dqc
ORDER BY seq;


-- ---------------------------------------------------------------------------
-- 4. 闸门（约束 ③）：存在 BLOCK 才产生结果行 → 才抛错 → 退出码 1
--    没有 BLOCK 时子查询 0 行（HAVING COUNT(*) > 0 保证），不抛任何异常。
-- ---------------------------------------------------------------------------
SELECT raise_error(msg) AS gate
FROM (
    SELECT CONCAT('❌ BLOCK [', '${dt}', ']：',
                  CONCAT_WS(', ', COLLECT_LIST(code)),
                  ' → 阻断下游') AS msg
    FROM v_dqc
    WHERE status = 'BLOCK'
    HAVING COUNT(*) > 0
) x;


-- ---------------------------------------------------------------------------
-- 5. 走到这里说明闸门没拦住 → 全部通过
-- ---------------------------------------------------------------------------
SELECT CONCAT('✅ PASS [', '${dt}', ']') AS result FROM (SELECT 1) t;
