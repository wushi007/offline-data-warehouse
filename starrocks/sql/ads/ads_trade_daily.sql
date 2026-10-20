-- MODE: daily
-- =============================================================================
-- ads_trade_daily —— 交易日报（运营大盘）
-- =============================================================================
-- 来源：dws.dws_user_behavior_daily（日 × 用户）；粒度：每天 1 行
-- 指标：GMV / 订单数 / 购买用户数 / 客单价 / ARPPU
-- 变量：${DB} 目标库（默认 ads_v2）、${dt} 业务日期
-- 幂等：先 DELETE 目标日再 INSERT；DELETE 对 Duplicate Key 表可用，重跑不会翻倍
-- =============================================================================

DELETE FROM ${DB}.ads_trade_daily WHERE event_date = '${dt}';

INSERT INTO ${DB}.ads_trade_daily
    (event_date, gmv, order_cnt, buyer_cnt, avg_order_value, arppu)
SELECT '${dt}' AS event_date,
       gmv,
       order_cnt,
       buyer_cnt,
       -- 分母为 0 时给 0，避免出现 NULL 让下游报表出现空值
       CASE WHEN order_cnt > 0 THEN gmv / order_cnt ELSE 0 END AS avg_order_value,
       CASE WHEN buyer_cnt > 0 THEN gmv / buyer_cnt ELSE 0 END AS arppu
FROM (
    SELECT SUM(purchase_amount)                              AS gmv,
           SUM(purchase_cnt)                                 AS order_cnt,
           -- 有购买行为的用户数（不是购买次数）
           SUM(CASE WHEN purchase_cnt > 0 THEN 1 ELSE 0 END)  AS buyer_cnt
    FROM dws.dws_user_behavior_daily
    WHERE event_date = '${dt}'
) t;
