-- ============================================================
-- ads_ec_daily_overview 全区间对账（2019-10-01 ~ 2019-11-30）
-- 只读脚本，不修改任何数据
-- ============================================================

SELECT '=== 1. 覆盖与完整性 ===' AS section;

SELECT COUNT(*)                 AS rows_total,
       COUNT(DISTINCT event_date) AS distinct_days,
       MIN(event_date)          AS mn,
       MAX(event_date)          AS mx
FROM ads_v2.ads_ec_daily_overview;

SELECT '=== 2. 逐日对账（对不上参照的天数，0 为全绿）===' AS section;

WITH ref_uv AS (
    SELECT event_date, bitmap_union_count(user_bitmap) AS uv
    FROM dwd.dwd_user_uv_bitmap GROUP BY event_date
), ref_traffic AS (
    SELECT event_date, SUM(pv) AS pv
    FROM dws.dws_traffic_hour_daily GROUP BY event_date
), ref_sess AS (
    SELECT event_date, COUNT(*) AS sess
    FROM dws.dws_user_session_daily GROUP BY event_date
), ref_trade AS (
    SELECT event_date,
           SUM(purchase_amount)            AS gmv,
           SUM(purchase_cnt)               AS ord,
           SUM(IF(purchase_cnt > 0, 1, 0)) AS buyer
    FROM dws.dws_user_behavior_daily GROUP BY event_date
), ref_funnel AS (
    SELECT event_date,
           COUNT(DISTINCT CASE WHEN event_type = 'view'     THEN user_id END) AS view_uv,
           COUNT(DISTINCT CASE WHEN event_type = 'cart'     THEN user_id END) AS cart_uv,
           COUNT(DISTINCT CASE WHEN event_type = 'purchase' THEN user_id END) AS purchase_uv
    FROM dwd.dwd_event_fact GROUP BY event_date
), j AS (
    SELECT o.event_date, o.uv, o.pv, o.session_cnt, o.gmv, o.order_cnt, o.buyer_cnt,
           o.view_uv, o.cart_uv, o.purchase_uv,
           ru.uv    AS r_uv,
           rt.pv    AS r_pv,
           rs.sess  AS r_sess,
           rd.gmv   AS r_gmv,
           rd.ord   AS r_ord,
           rd.buyer AS r_buyer,
           rf.view_uv AS r_view, rf.cart_uv AS r_cart, rf.purchase_uv AS r_pur
    FROM ads_v2.ads_ec_daily_overview o
    LEFT JOIN ref_uv      ru ON ru.event_date = o.event_date
    LEFT JOIN ref_traffic rt ON rt.event_date = o.event_date
    LEFT JOIN ref_sess    rs ON rs.event_date = o.event_date
    LEFT JOIN ref_trade   rd ON rd.event_date = o.event_date
    LEFT JOIN ref_funnel  rf ON rf.event_date = o.event_date
)
SELECT SUM(IF(uv          <> r_uv,    1, 0)) AS uv_bad,
       SUM(IF(pv          <> r_pv,    1, 0)) AS pv_bad,
       SUM(IF(session_cnt <> r_sess,  1, 0)) AS sess_bad,
       SUM(IF(ABS(gmv - r_gmv) > 0.01, 1, 0)) AS gmv_bad,
       SUM(IF(order_cnt   <> r_ord,   1, 0)) AS ord_bad,
       SUM(IF(buyer_cnt   <> r_buyer, 1, 0)) AS buyer_bad,
       SUM(IF(view_uv     <> r_view,  1, 0)) AS view_bad,
       SUM(IF(cart_uv     <> r_cart,  1, 0)) AS cart_bad,
       SUM(IF(purchase_uv <> r_pur,   1, 0)) AS pur_bad
FROM j;

-- funnel_order_bad 与 rate_range_bad 恒为 22 天，属上游源数据固有特征（加购与下单可跨天），
-- 非本表缺陷，详见 ads_ec_daily_overview.sql 文件头陷阱 4。
-- arppu 的检查方向为 arppu < arpu：buyer_cnt <= uv 恒成立，arppu >= arpu 是数学必然。
SELECT '=== 3. 不变量（违规天数，0 为全绿）===' AS section;

SELECT SUM(IF(NOT (view_uv >= cart_uv AND cart_uv >= purchase_uv), 1, 0)) AS funnel_order_bad,
       SUM(IF(uv < view_uv, 1, 0))                    AS uv_lt_view_bad,
       SUM(IF(new_user_cnt + old_user_cnt <> uv, 1, 0)) AS newold_sum_bad,
       SUM(IF(new_user_cnt > uv, 1, 0))               AS new_gt_uv_bad,
       SUM(IF(old_user_cnt < 0, 1, 0))                AS old_negative_bad,
       SUM(IF(view_to_cart_rate < 0 OR view_to_cart_rate > 1
              OR view_to_purchase_rate < 0 OR view_to_purchase_rate > 1
              OR cart_to_purchase_rate < 0 OR cart_to_purchase_rate > 1
              OR cart_purchase_rate < 0 OR cart_purchase_rate > 1, 1, 0)) AS rate_range_bad,
       SUM(IF(arppu < arpu - 0.001, 1, 0))            AS arppu_lt_arpu_bad
FROM ads_v2.ads_ec_daily_overview;

SELECT '=== 4. 空值检查（核心列）===' AS section;

SELECT SUM(IF(uv IS NULL OR pv IS NULL OR session_cnt IS NULL OR gmv IS NULL
              OR order_cnt IS NULL OR buyer_cnt IS NULL OR new_user_cnt IS NULL
              OR old_user_cnt IS NULL, 1, 0))         AS core_null_bad,
       SUM(IF(avg_order_value IS NULL OR arppu IS NULL OR arpu IS NULL
              OR view_to_cart_rate IS NULL OR view_to_purchase_rate IS NULL
              OR cart_to_purchase_rate IS NULL OR avg_duration_sec IS NULL
              OR avg_action_cnt IS NULL OR cart_purchase_rate IS NULL, 1, 0)) AS derived_null_bad
FROM ads_v2.ads_ec_daily_overview;

-- gmv 须用容差比较：两侧 double 末位不同，最大绝对差 4.47e-06，精确等值会误报 60/61 天；
-- 整型计数列可精确比较。
SELECT '=== 5. 与 ads 库（Hive 复刻）全区间交叉对账 ===' AS section;

SELECT COUNT(*) AS compared_days,
       SUM(IF(ABS(o.gmv - t.gmv) > 0.01, 1, 0))                 AS gmv_bad,
       SUM(IF(o.order_cnt <> t.order_cnt, 1, 0))                AS ord_bad,
       SUM(IF(o.buyer_cnt <> t.buyer_cnt, 1, 0))                AS buyer_bad,
       SUM(IF(o.view_uv <> f.view_uv, 1, 0))                    AS view_bad,
       SUM(IF(o.cart_uv <> f.cart_uv, 1, 0))                    AS cart_bad,
       SUM(IF(o.purchase_uv <> f.purchase_uv, 1, 0))            AS pur_bad,
       SUM(IF(o.session_cnt <> s.session_cnt, 1, 0))            AS sess_bad
FROM ads_v2.ads_ec_daily_overview o
JOIN ads.ads_trade_daily             t ON t.event_date = o.event_date
JOIN ads.ads_conversion_funnel_daily f ON f.event_date = o.event_date
JOIN ads_v2.ads_session_behavior_daily s ON s.event_date = o.event_date;

SELECT '=== 6. 汇总形态（头尾各 3 天 + 区间合计）===' AS section;

SELECT event_date, uv, pv, session_cnt, view_uv, cart_uv, purchase_uv,
       gmv, order_cnt, buyer_cnt, new_user_cnt, old_user_cnt, arpu, arppu
FROM ads_v2.ads_ec_daily_overview
WHERE event_date IN ('2019-10-01','2019-10-02','2019-10-03','2019-11-28','2019-11-29','2019-11-30')
ORDER BY event_date;

SELECT MIN(gmv) AS gmv_min, MAX(gmv) AS gmv_max,
       ROUND(AVG(uv), 0) AS uv_avg,  MIN(uv) AS uv_min, MAX(uv) AS uv_max,
       SUM(gmv) AS gmv_total, SUM(order_cnt) AS ord_total
FROM ads_v2.ads_ec_daily_overview;
