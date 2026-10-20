-- =============================================================================
-- bitmap_uv_dedup.sql —— Bitmap 精确去重（UV 预聚合）
-- =============================================================================
-- 把 dwd_event_fact 的按天 UV 从运行时 COUNT(DISTINCT) 改为写入时 Bitmap 预聚合。
-- 测试环境：StarRocks 4.1.4，单 BE
--   全量 UV : COUNT(DISTINCT) 730ms → bitmap_union_count 70ms（10.4×）
--   按天 UV : COUNT(DISTINCT) 660ms → bitmap 分组 30ms（22.0×）
-- 结果逐值一致：全量 5,316,649；月度 10 月 3,022,290 / 11 月 3,696,117
-- 规模：109,806,507 行 / 5.12 GB → 61 行 / 29.6 MB（数据区间共 61 天）
-- AGGREGATE KEY 在此为等价变换（去重合并仍为去重）；其余 17 张复刻表用 DUPLICATE KEY。
-- to_bitmap 只接受整型，BIGINT 的 user_id 可直接使用，不涉及全局字典。
-- =============================================================================


-- 1) 建表（实际库 BUCKETS = 4，compression 为 LZ4）
CREATE TABLE IF NOT EXISTS `dwd`.`dwd_user_uv_bitmap` (
  `event_date`  DATE,
  `user_bitmap` BITMAP BITMAP_UNION     -- 声明聚合函数：同 key 写入时做位图 OR
)
ENGINE=OLAP
AGGREGATE KEY(`event_date`)
DISTRIBUTED BY HASH(`event_date`) BUCKETS 4
PROPERTIES ("replication_num" = "1", "compression" = "LZ4");


-- 2) 灌数（一次性预聚合）
-- to_bitmap 生成单值位图，bitmap_union 将同一天所有位图 OR 合并
INSERT INTO dwd.dwd_user_uv_bitmap
SELECT event_date,
       bitmap_union(to_bitmap(user_id)) AS user_bitmap
FROM dwd.dwd_event_fact
GROUP BY event_date;


-- 3) 幂等重跑（按天重建，不整表清）
-- 按天先删再插，重跑同一天结果一致。BITMAP_UNION 本身幂等（OR 自己仍为自己），
-- 但 INSERT INTO 追加会重复占用存储。
--
-- DELETE FROM dwd.dwd_user_uv_bitmap WHERE event_date = '${dt}';
-- INSERT INTO dwd.dwd_user_uv_bitmap
-- SELECT event_date, bitmap_union(to_bitmap(user_id))
-- FROM dwd.dwd_event_fact WHERE event_date = '${dt}' GROUP BY event_date;


-- 4) 查询：UV 去重

-- 4.1 全量 UV（70ms，对照 COUNT(DISTINCT) 730ms）
SELECT bitmap_union_count(user_bitmap) AS uv
FROM dwd.dwd_user_uv_bitmap;

-- 4.2 按天 UV（30ms，对照 COUNT(DISTINCT) 660ms）
SELECT event_date,
       bitmap_union_count(user_bitmap) AS day_uv
FROM dwd.dwd_user_uv_bitmap
GROUP BY event_date
ORDER BY event_date;

-- 4.3 任意区间 UV（位图可直接 OR 合并，所以按天存就能算周/月）
SELECT bitmap_union_count(user_bitmap) AS week_uv
FROM dwd.dwd_user_uv_bitmap
WHERE event_date BETWEEN '2019-10-01' AND '2019-10-07';   -- 实测 957,543

-- 4.4 月度 UV（与 COUNT(DISTINCT user_id) 逐值一致）
SELECT DATE_FORMAT(event_date, '%Y-%m') AS ym,
       bitmap_union_count(user_bitmap)  AS m_uv
FROM dwd.dwd_user_uv_bitmap
GROUP BY ym
ORDER BY ym;
-- 2019-10  3,022,290
-- 2019-11  3,696,117


-- 5) 进阶：位图集合运算
-- bitmap 保留用户集合本身，可做交/并/差。函数为 bitmap_and / bitmap_andnot
-- （不是 bitmap_and_count），返回位图，需再套 bitmap_count 求基数。

-- 5.1 D+1 留存用户数（10-01 与 10-02 都活跃）：32,862
SELECT bitmap_count(bitmap_and(a.user_bitmap, b.user_bitmap)) AS both_days
FROM dwd.dwd_user_uv_bitmap a
JOIN dwd.dwd_user_uv_bitmap b ON b.event_date = '2019-10-02'
WHERE a.event_date = '2019-10-01';

-- 5.2 流失用户数（10-01 活跃但 10-02 不活跃）：157,326
SELECT bitmap_count(bitmap_andnot(a.user_bitmap, b.user_bitmap)) AS churned
FROM dwd.dwd_user_uv_bitmap a
JOIN dwd.dwd_user_uv_bitmap b ON b.event_date = '2019-10-02'
WHERE a.event_date = '2019-10-01';

-- 5.3 正确性自校验：留存 + 流失 = D0 总数（32,862 + 157,326 = 190,188）
SELECT bitmap_count(a.user_bitmap)                                AS d0,
       bitmap_count(bitmap_and(a.user_bitmap, b.user_bitmap))     AS retained,
       bitmap_count(bitmap_andnot(a.user_bitmap, b.user_bitmap))  AS churned
FROM dwd.dwd_user_uv_bitmap a
JOIN dwd.dwd_user_uv_bitmap b ON b.event_date = '2019-10-02'
WHERE a.event_date = '2019-10-01';


-- 6) 语义陷阱：逐日 UV 不能相加
-- SUM(bitmap_count(user_bitmap)) = 15,095,144，bitmap_union_count = 5,316,649，
-- 相差 2.84 倍（同一用户多天活跃被重复计数）；区间 UV 须先 OR 再计数。
-- 15,095,144 恰好等于 dws_user_behavior_daily 的行数，两者含义不同。
