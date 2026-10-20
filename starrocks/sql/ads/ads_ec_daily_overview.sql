-- MODE: daily
-- =============================================================================
-- ads_ec_daily_overview —— 电商每日总览（运营看板主表）
-- =============================================================================
-- 来源：dws / dwd / dim（四张表）；粒度：每天 1 行
-- 指标：流量、漏斗、交易、用户、会话五组，共 21 列
--
-- 单位约定：本表所有「率」为小数 0~1（不是百分数），分母为 0 时给 0（不是 NULL）。
-- ads_traffic_hour_daily / ads_user_retention_daily 用百分数且分母 0 给 NULL，
-- 跨表拼看板需先对齐。
--
-- 陷阱：
-- 1) 分时表的 uv 为小时 UV，SUM 到全天会重复计（逐日相加为真实值的 2.84 倍），
--    dws_product_daily.exposure_uv 同理；本表 uv 走位图去重。
-- 2) 分时表 session_cnt 为按小时去重，SUM 得 420,842，真实值 391,722（高估 7.4%）；
--    本表改取 dws_user_session_daily 的 COUNT(*)。分时表只有 pv 可安全 SUM。
-- 3) view_uv 可小于 uv（2019-11-30：255,469 < 255,497），不可把漏斗顶层当日活。
-- 4) 同日 cart_uv < purchase_uv 是常态（61 天中 22 天，最多 1.76 倍），源于跨天下单，
--    本表不做 clamp；cart_to_purchase_rate 有 22/61 天 > 1，下游需自行处理。
--
-- 变量：${DB} 目标库（默认 ads_v2）、${dt} 业务日期
-- 幂等：先 DELETE 目标日再 INSERT
-- =============================================================================

DELETE FROM ${DB}.ads_ec_daily_overview WHERE event_date = '${dt}';

INSERT INTO ${DB}.ads_ec_daily_overview
    (event_date, uv, pv, session_cnt, view_uv, cart_uv, purchase_uv,
     gmv, order_cnt, buyer_cnt, new_user_cnt, old_user_cnt,
     avg_order_value, arppu, arpu,
     view_to_cart_rate, view_to_purchase_rate, cart_to_purchase_rate,
     avg_duration_sec, avg_action_cnt, cart_purchase_rate)
WITH
-- 流量：pv 为事件总数，单个事件只落在某一小时，可安全 SUM
-- （SUM(pv) = SUM(active_action_cnt) = 1,752,452）；session_cnt / uv 不可 SUM。
traffic AS (
    SELECT SUM(pv) AS pv
    FROM dws.dws_traffic_hour_daily
    WHERE event_date = '${dt}'
),
-- 日活：位图精确去重。与 dws 的 COUNT(DISTINCT user_id) 逐值一致，但快约 20×
dau AS (
    SELECT bitmap_union_count(user_bitmap) AS uv
    FROM dwd.dwd_user_uv_bitmap
    WHERE event_date = '${dt}'
),
-- 会话行为：口径与 ads_session_behavior_daily 一致。session_cnt 取 COUNT(*)
-- （粒度为日×用户×会话，行数即会话数），避免分时表重复计跨小时会话。
sess AS (
    SELECT COUNT(*)                                                   AS session_cnt,
           AVG(session_duration_sec)                                  AS avg_duration_sec,
           AVG(view_cnt + cart_cnt + purchase_cnt + remove_cart_cnt)  AS avg_action_cnt,
           SUM(cart_cnt)                                              AS cart_total,
           SUM(purchase_cnt)                                          AS purchase_total
    FROM dws.dws_user_session_daily
    WHERE event_date = '${dt}'
),
-- 交易：口径与 ads_trade_daily 完全一致
trade AS (
    SELECT SUM(purchase_amount)                              AS gmv,
           SUM(purchase_cnt)                                 AS order_cnt,
           SUM(CASE WHEN purchase_cnt > 0 THEN 1 ELSE 0 END) AS buyer_cnt
    FROM dws.dws_user_behavior_daily
    WHERE event_date = '${dt}'
),
-- 漏斗：跨商品精确去重，必须回 DWD（不能拿商品级 UV 累加）
funnel AS (
    SELECT COUNT(DISTINCT CASE WHEN event_type = 'view'     THEN user_id END) AS view_uv,
           COUNT(DISTINCT CASE WHEN event_type = 'cart'     THEN user_id END) AS cart_uv,
           COUNT(DISTINCT CASE WHEN event_type = 'purchase' THEN user_id END) AS purchase_uv
    FROM dwd.dwd_event_fact
    WHERE event_date = '${dt}'
),
-- 新客：首次活跃日就是当天。老客 = uv - 新客（含沉默后回流的用户）
newc AS (
    SELECT COUNT(DISTINCT d.user_id) AS new_user_cnt
    FROM dws.dws_user_behavior_daily d
    JOIN dim.dim_user u ON d.user_id = u.user_id
    WHERE d.event_date = '${dt}'
      AND u.first_seen_date = '${dt}'
)
SELECT '${dt}'                        AS event_date,
       dau.uv,
       traffic.pv,
       sess.session_cnt,
       funnel.view_uv,
       funnel.cart_uv,
       funnel.purchase_uv,
       trade.gmv,
       trade.order_cnt,
       trade.buyer_cnt,
       newc.new_user_cnt,
       dau.uv - newc.new_user_cnt     AS old_user_cnt,
       -- 以下 6 个派生指标：分母为 0 一律给 0，避免下游报表出现空值
       CASE WHEN trade.order_cnt > 0
            THEN trade.gmv / trade.order_cnt ELSE 0 END    AS avg_order_value,
       CASE WHEN trade.buyer_cnt > 0
            THEN trade.gmv / trade.buyer_cnt ELSE 0 END    AS arppu,
       CASE WHEN dau.uv > 0
            THEN trade.gmv / dau.uv ELSE 0 END             AS arpu,
       CASE WHEN funnel.view_uv > 0
            THEN funnel.cart_uv     / funnel.view_uv ELSE 0 END AS view_to_cart_rate,
       CASE WHEN funnel.view_uv > 0
            THEN funnel.purchase_uv / funnel.view_uv ELSE 0 END AS view_to_purchase_rate,
       CASE WHEN funnel.cart_uv > 0
            THEN funnel.purchase_uv / funnel.cart_uv ELSE 0 END AS cart_to_purchase_rate,
       sess.avg_duration_sec,
       sess.avg_action_cnt,
       CASE WHEN sess.cart_total > 0
            THEN sess.purchase_total / sess.cart_total ELSE 0 END AS cart_purchase_rate
FROM traffic
CROSS JOIN dau
CROSS JOIN sess
CROSS JOIN trade
CROSS JOIN funnel
CROSS JOIN newc;
