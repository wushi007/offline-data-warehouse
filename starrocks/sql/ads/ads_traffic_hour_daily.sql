-- MODE: daily
-- =============================================================================
-- ads_traffic_hour_daily —— 分时大盘（运营时段表）
-- =============================================================================
-- 来源：dws.dws_traffic_hour_daily（日 × 小时）；粒度：event_date × event_hour（每天 24 行）
-- 在 DWS 分时表上增加报表语义：时段分桶、PV/订单占全天比例（SUM OVER）、
-- 峰值小时标记（pv = MAX(pv) OVER）、分时转化率（purchase_uv / uv）。
-- 变量：${DB} 目标库、${dt} 业务日期；幂等：先 DELETE 目标日再 INSERT
-- =============================================================================

DELETE FROM ${DB}.ads_traffic_hour_daily WHERE event_date = '${dt}';

INSERT INTO ${DB}.ads_traffic_hour_daily
    (event_date, event_hour, time_bucket, pv, uv, purchase_cnt, purchase_uv, gmv,
     pv_share, order_share, purchase_rate, is_peak_hour)
SELECT '${dt}' AS event_date,
       event_hour,
       CASE WHEN event_hour <= 6  THEN '凌晨(0-6)'
            WHEN event_hour <= 12 THEN '上午(7-12)'
            WHEN event_hour <= 18 THEN '下午(13-18)'
            ELSE                       '晚间(19-23)' END      AS time_bucket,
       pv,
       uv,
       purchase_cnt,
       purchase_uv,
       gmv,
       -- 占全天比例（百分比，保留 4 位）
       ROUND(pv           / SUM(pv)           OVER () * 100, 4) AS pv_share,
       ROUND(purchase_cnt / SUM(purchase_cnt) OVER () * 100, 4) AS order_share,
       -- uv 为 0 时保持 NULL（不做 0 除）
       CASE WHEN uv > 0 THEN ROUND(purchase_uv / uv * 100, 4) END AS purchase_rate,
       CASE WHEN pv = MAX(pv) OVER () THEN 1 ELSE 0 END        AS is_peak_hour
FROM dws.dws_traffic_hour_daily
WHERE event_date = '${dt}';
