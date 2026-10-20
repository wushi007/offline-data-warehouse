-- MODE: daily
-- =============================================================================
-- ads_product_hot_rank_daily —— 商品热榜 Top100（逐日）
-- =============================================================================
-- 来源：dws.dws_product_daily（日 × 商品）；粒度：每天 Top100
-- 排名口径：销售额降序，同额按销量降序，最后以 product_id 保证可重现。
-- 仅按 sales_amount + sales_cnt 排序时同值行次序不确定（2019-10-05 两商品名次互换），
-- 加 product_id 后与 Hive 版仅在该两行编号不同，榜内商品与度量一致。
-- `rank` 为保留字，必须加反引号。
-- 变量：${DB} 目标库、${dt} 业务日期；幂等：先 DELETE 目标日再 INSERT
-- =============================================================================

DELETE FROM ${DB}.ads_product_hot_rank_daily WHERE event_date = '${dt}';

INSERT INTO ${DB}.ads_product_hot_rank_daily
    (event_date, `rank`, product_id, category_id, brand, sales_cnt, sales_amount, exposure_uv)
SELECT '${dt}' AS event_date,
       `rank`,
       product_id,
       category_id,
       brand,
       sales_cnt,
       sales_amount,
       exposure_uv
FROM (
    SELECT ROW_NUMBER() OVER (ORDER BY sales_amount DESC, sales_cnt DESC, product_id) AS `rank`,
           product_id,
           category_id,
           brand,
           sales_cnt,
           sales_amount,
           exposure_uv
    FROM dws.dws_product_daily
    WHERE event_date = '${dt}'
) t
WHERE `rank` <= 100;
