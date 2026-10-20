-- MODE: daily
-- =============================================================================
-- ads_session_behavior_daily —— 会话行为日报
-- =============================================================================
-- 来源：dws.dws_user_session_daily（日 × 用户 × 会话）；粒度：每天 1 行
-- 指标：会话数 / 平均会话时长 / 平均行为数 / 加购转化率
-- 变量：${DB} 目标库、${dt} 业务日期；幂等：先 DELETE 目标日再 INSERT
-- =============================================================================

DELETE FROM ${DB}.ads_session_behavior_daily WHERE event_date = '${dt}';

INSERT INTO ${DB}.ads_session_behavior_daily
    (event_date, session_cnt, avg_duration_sec, avg_action_cnt, cart_purchase_rate)
SELECT '${dt}' AS event_date,
       COUNT(*)                                              AS session_cnt,
       AVG(session_duration_sec)                             AS avg_duration_sec,
       -- 一次会话的「行为数」= 四类行为计数之和
       AVG(view_cnt + cart_cnt + purchase_cnt + remove_cart_cnt) AS avg_action_cnt,
       -- 加购 → 购买的转化；cart 为 0 时给 0（不是 NULL）
       CASE WHEN SUM(cart_cnt) > 0
            THEN SUM(purchase_cnt) / SUM(cart_cnt) ELSE 0 END AS cart_purchase_rate
FROM dws.dws_user_session_daily
WHERE event_date = '${dt}';
