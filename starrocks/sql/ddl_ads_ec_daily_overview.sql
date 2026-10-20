-- =============================================================================
-- ddl_ads_ec_daily_overview.sql —— StarRocks 侧建表 DDL（手工执行一次）
-- =============================================================================
-- 其余 ADS 表由 run_ads_sql.py 以 `CREATE TABLE ... LIKE ads.t` 复制；本表无 Hive
-- 复刻参照物，且 IF NOT EXISTS 不会短路，LIKE 不存在的表报 1064，故手工建。
-- 本文件不能放入 starrocks/sql/ads/（会被当作表执行，且缺 -- MODE: 会退出）。
-- 用法：mysql -h127.0.0.1 -P9030 -uroot < starrocks/sql/ddl_ads_ec_daily_overview.sql
-- 重复执行安全。
-- =============================================================================

CREATE TABLE IF NOT EXISTS `ads_v2`.`ads_ec_daily_overview` (
  `event_date`            date   NULL COMMENT "业务日期",
  `uv`                    bigint NULL COMMENT "日活UV（位图精确去重，非小时UV相加）",
  `pv`                    bigint NULL COMMENT "事件总数（浏览/加购/下单全量）",
  `session_cnt`           bigint NULL COMMENT "会话数",
  `view_uv`               bigint NULL COMMENT "浏览UV（跨商品去重）",
  `cart_uv`               bigint NULL COMMENT "加购UV（跨商品去重）",
  `purchase_uv`           bigint NULL COMMENT "购买UV（跨商品去重）",
  `gmv`                   double NULL COMMENT "成交金额",
  `order_cnt`             bigint NULL COMMENT "订单数",
  `buyer_cnt`             bigint NULL COMMENT "购买用户数",
  `new_user_cnt`          bigint NULL COMMENT "新客数（首活日=当天）",
  `old_user_cnt`          bigint NULL COMMENT "老客数=uv-新客数（含回流）",
  `avg_order_value`       double NULL COMMENT "客单价=gmv/order_cnt",
  `arppu`                 double NULL COMMENT "付费用户人均=gmv/buyer_cnt",
  `arpu`                  double NULL COMMENT "活跃用户人均=gmv/uv",
  `view_to_cart_rate`     double NULL COMMENT "浏览→加购（小数0~1）",
  `view_to_purchase_rate` double NULL COMMENT "浏览→购买（小数0~1）",
  `cart_to_purchase_rate` double NULL COMMENT "加购→购买（小数0~1，可>1）",
  `avg_duration_sec`      double NULL COMMENT "平均会话时长(秒)",
  `avg_action_cnt`        double NULL COMMENT "平均会话行为数",
  `cart_purchase_rate`    double NULL COMMENT "会话级加购→购买（小数0~1）"
)
ENGINE=OLAP
-- 每天 1 行，无明细保留需求，用 DUPLICATE KEY
DUPLICATE KEY(`event_date`)
-- 数据量小，单桶足够；单 BE 环境须 replication_num = 1
DISTRIBUTED BY HASH(`event_date`) BUCKETS 1
PROPERTIES (
"compression" = "LZ4",
"fast_schema_evolution" = "true",
"replicated_storage" = "true",
"replication_num" = "1"
);
