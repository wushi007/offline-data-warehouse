-- MODE: snapshot
-- =============================================================================
-- ads_user_rfm_snapshot —— 用户 RFM 八分群（全量快照）
-- =============================================================================
-- 来源：dws.dws_user_behavior_daily 区间聚合；粒度：全量购买用户（非按天，as_of_dt = 快照日）
-- 口径：F/M 按全量均值切分，R 以 30 天为界；m/f/r 布尔组合为 8 类
-- DWS 已在日 × 用户粒度聚合：f_cnt=SUM(purchase_cnt)、m_amount=SUM(purchase_amount)、
-- last_buy_date=MAX(有购买日期)，与 DWD 明细口径等价（与 Hive 版逐位一致）。
-- HAVING SUM(purchase_cnt) > 0 不可省：否则非购买用户也会进入，用户数翻倍。
-- 变量：${DB} 目标库、${start} 区间起、${end} 区间止（兼快照日）
-- 幂等：先 DELETE 该 as_of_dt 再 INSERT
-- =============================================================================

DELETE FROM ${DB}.ads_user_rfm_snapshot WHERE as_of_dt = '${end}';

INSERT INTO ${DB}.ads_user_rfm_snapshot
    (user_id, r_days, f_cnt, m_amount, rfm_seg, as_of_dt)
WITH p AS (
    SELECT user_id,
           SUM(purchase_cnt)                                    AS f_cnt,
           SUM(purchase_amount)                                 AS m_amount,
           -- 只在「有购买」的那些天里取最大日期，才是真正的末次购买日
           MAX(CASE WHEN purchase_cnt > 0 THEN event_date END)   AS last_buy_date
    FROM dws.dws_user_behavior_daily
    WHERE event_date BETWEEN '${start}' AND '${end}'
    GROUP BY user_id
    HAVING SUM(purchase_cnt) > 0        -- 排除非购买用户，见文件头
),
t AS (
    SELECT AVG(f_cnt) AS avg_f, AVG(m_amount) AS avg_m FROM p
),
scored AS (
    SELECT p.user_id,
           p.f_cnt,
           p.m_amount,
           DATEDIFF('${end}', p.last_buy_date)                        AS r_days,
           CASE WHEN DATEDIFF('${end}', p.last_buy_date) <= 30 THEN 1 ELSE 0 END AS r_good,
           CASE WHEN p.f_cnt    >= t.avg_f THEN 1 ELSE 0 END          AS f_good,
           CASE WHEN p.m_amount >= t.avg_m THEN 1 ELSE 0 END          AS m_good
    FROM p CROSS JOIN t
)
SELECT user_id,
       r_days,
       f_cnt,
       m_amount,
       CASE
           WHEN m_good = 1 AND f_good = 1 AND r_good = 1 THEN '重要价值客户'
           WHEN m_good = 1 AND f_good = 1 AND r_good = 0 THEN '重要保持客户'
           WHEN m_good = 1 AND f_good = 0 AND r_good = 1 THEN '重要发展客户'
           WHEN m_good = 1 AND f_good = 0 AND r_good = 0 THEN '重要挽留客户'
           WHEN m_good = 0 AND f_good = 1 AND r_good = 1 THEN '一般价值客户'
           WHEN m_good = 0 AND f_good = 1 AND r_good = 0 THEN '一般保持客户'
           WHEN m_good = 0 AND f_good = 0 AND r_good = 1 THEN '一般发展客户'
           ELSE                                               '一般挽留客户'
       END AS rfm_seg,
       '${end}' AS as_of_dt
FROM scored;
