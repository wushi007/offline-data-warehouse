-- MODE: daily
-- =============================================================================
-- ads_user_retention_daily —— 用户留存日报
-- =============================================================================
-- 来源：dws.dws_user_behavior_daily（日 × 用户），跨日自连接；粒度：每个基准日 D0 一行
-- 口径：D0 活跃用户中 D+1 / D+3 / D+7 仍活跃的比例
-- 右删失必须输出 NULL 而非 0：D+N 日期 > 区间末日（${end}）时无观测数据，
-- 写 0 会被读成留存归零。
-- 变量：${DB} 目标库、${dt} 基准日、${d1} ${d3} ${d7} 对应日期、${end} 区间末日
-- 幂等：先 DELETE 目标日再 INSERT
-- =============================================================================

DELETE FROM ${DB}.ads_user_retention_daily WHERE event_date = '${dt}';

INSERT INTO ${DB}.ads_user_retention_daily
    (event_date, d0_uv, d1_uv, d1_rate, d3_uv, d3_rate, d7_uv, d7_rate)
WITH base AS (
    SELECT user_id FROM dws.dws_user_behavior_daily WHERE event_date = '${dt}'
),
d1 AS (SELECT user_id FROM dws.dws_user_behavior_daily WHERE event_date = '${d1}'),
d3 AS (SELECT user_id FROM dws.dws_user_behavior_daily WHERE event_date = '${d3}'),
d7 AS (SELECT user_id FROM dws.dws_user_behavior_daily WHERE event_date = '${d7}'),
c AS (
    SELECT (SELECT COUNT(*) FROM base)                            AS d0_uv,
           (SELECT COUNT(*) FROM base b JOIN d1 x ON b.user_id = x.user_id) AS d1_uv,
           (SELECT COUNT(*) FROM base b JOIN d3 x ON b.user_id = x.user_id) AS d3_uv,
           (SELECT COUNT(*) FROM base b JOIN d7 x ON b.user_id = x.user_id) AS d7_uv
)
SELECT '${dt}' AS event_date,
       c.d0_uv,
       -- ---- D+1 ----
       CASE WHEN '${d1}' <= '${end}' THEN c.d1_uv END AS d1_uv,
       CASE WHEN c.d0_uv > 0 AND '${d1}' <= '${end}'
            THEN ROUND(c.d1_uv / c.d0_uv * 100, 4) END AS d1_rate,
       -- ---- D+3 ----
       CASE WHEN '${d3}' <= '${end}' THEN c.d3_uv END AS d3_uv,
       CASE WHEN c.d0_uv > 0 AND '${d3}' <= '${end}'
            THEN ROUND(c.d3_uv / c.d0_uv * 100, 4) END AS d3_rate,
       -- ---- D+7 ----
       CASE WHEN '${d7}' <= '${end}' THEN c.d7_uv END AS d7_uv,
       CASE WHEN c.d0_uv > 0 AND '${d7}' <= '${end}'
            THEN ROUND(c.d7_uv / c.d0_uv * 100, 4) END AS d7_rate
FROM c;
