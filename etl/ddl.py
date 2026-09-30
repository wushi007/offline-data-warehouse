#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
ddl.py — 数仓全链路建表（ODS / DWD / DIM / DWS / ADS）
========================================================
只做两件事：**建库表** + **注册已有分区**，不写数据（数据由各 build_*.py 落）。

设计约定（与仓库既有口径一致）：
  · 分区列统一 `event_date`（DATE），无 hour 层——ODS 按天落盘，重跑只覆盖当天
  · event_type 四类枚举 view/cart/remove_from_cart/purchase（不丢 remove_from_cart）
  · 全部为 **Hive 外部表 + LOCATION 指到 HDFS**：DROP 表不删文件，便于重建
  · 存储格式 PARQUET + SNAPPY，与 ODS / StarRocks 灌入链路一致

用法：
  python etl/ddl.py             # 建全部库表 + MSCK 注册 ODS 分区
  python etl/ddl.py --drop      # 先删表再建（外部表，HDFS 文件保留）
"""

import argparse
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from config.config import (  # noqa: E402
    ADS, DBS, DIM_CATEGORY_PATH, DIM_DATE_PATH, DIM_PRODUCT_SCD2_PATH,
    DIM_SESSION_PATH, DIM_USER_PATH, DWD_DIRTY_PATH, DWD_FACT_PATH, DWS,
    ODS_PATH,
)
from utils import get_spark  # noqa: E402


def ddl_statements():
    """返回 [(库名, 表名, DDL)]，顺序即依赖顺序。"""
    stmts = []

    # ---------------- ODS ----------------
    stmts.append(("ods", "ods_event_log", f"""
        CREATE EXTERNAL TABLE IF NOT EXISTS {DBS['ods']}.ods_event_log (
            event_time    TIMESTAMP COMMENT '事件发生时间(UTC)',
            event_type    STRING    COMMENT '事件类型:view/cart/remove_from_cart/purchase',
            product_id    BIGINT    COMMENT '商品ID',
            category_id   BIGINT    COMMENT '品类ID',
            category_code STRING    COMMENT '品类编码',
            brand         STRING    COMMENT '品牌',
            price         DOUBLE    COMMENT '商品单价',
            user_id       BIGINT    COMMENT '用户ID',
            user_session  STRING    COMMENT '会话ID'
        )
        COMMENT 'ODS层-用户行为原始埋点日志（零修改，仅按天分区）'
        PARTITIONED BY (event_date DATE COMMENT '事件UTC日期')
        STORED AS PARQUET
        LOCATION '{ODS_PATH}'
    """))

    # ---------------- DWD ----------------
    # 事实表 13 列：9 源字段 + behavior_type/is_purchase/event_hour + event_date 分区列
    stmts.append(("dwd", "dwd_event_fact", f"""
        CREATE EXTERNAL TABLE IF NOT EXISTS {DBS['dwd']}.dwd_event_fact (
            event_time    TIMESTAMP COMMENT '事件发生时间(UTC)',
            event_type    STRING    COMMENT '事件类型:view/cart/remove_from_cart/purchase',
            behavior_type STRING    COMMENT '行为中文:浏览/加购/移出购物车/购买（退化维度）',
            product_id    BIGINT    COMMENT '商品ID',
            category_id   BIGINT    COMMENT '行为发生时品类ID（快照，退化维度）',
            category_code STRING    COMMENT '行为发生时品类编码（快照，退化维度）',
            brand         STRING    COMMENT '行为发生时品牌（快照，退化维度）',
            price         DOUBLE    COMMENT '行为发生时单价（快照）',
            user_id       BIGINT    COMMENT '用户ID',
            user_session  STRING    COMMENT '会话ID（退化维度）',
            is_purchase   INT       COMMENT '是否购买 0/1（派生标记）',
            event_hour    INT       COMMENT '事件小时 0~23（派生维度，支撑分时分析）'
        )
        COMMENT 'DWD层-用户行为清洗事实表（星型事实表，冗余退化维度，13列）'
        PARTITIONED BY (event_date DATE COMMENT '事件UTC日期')
        STORED AS PARQUET
        LOCATION '{DWD_FACT_PATH}'
    """))

    stmts.append(("dwd", "dwd_event_dirty", f"""
        CREATE EXTERNAL TABLE IF NOT EXISTS {DBS['dwd']}.dwd_event_dirty (
            event_time    TIMESTAMP COMMENT '事件发生时间(UTC)',
            event_type    STRING    COMMENT '事件类型（可能非法）',
            product_id    BIGINT    COMMENT '商品ID',
            category_id   BIGINT    COMMENT '品类ID',
            category_code STRING    COMMENT '品类编码',
            brand         STRING    COMMENT '品牌',
            price         DOUBLE    COMMENT '商品单价',
            user_id       BIGINT    COMMENT '用户ID',
            user_session  STRING    COMMENT '会话ID',
            dirty_reason  STRING    COMMENT '脏数据原因:null_key/invalid_event_type/negative_price'
        )
        COMMENT 'DWD层-清洗被拦下的脏数据隔离表（可追溯，不污染事实表）'
        PARTITIONED BY (event_date DATE COMMENT '事件UTC日期')
        STORED AS PARQUET
        LOCATION '{DWD_DIRTY_PATH}'
    """))

    # ---------------- DIM ----------------
    stmts.append(("dim", "dim_product_scd2", f"""
        CREATE EXTERNAL TABLE IF NOT EXISTS {DBS['dim']}.dim_product_scd2 (
            dim_product_sk BIGINT COMMENT '代理键（版本粒度，非业务键）',
            product_id     BIGINT COMMENT '商品业务主键',
            category_id    BIGINT COMMENT '品类ID（该版本口径）',
            category_code  STRING COMMENT '品类编码（该版本口径）',
            brand          STRING COMMENT '品牌（该版本口径）',
            dw_start_date  DATE   COMMENT '该版本生效开始日期（闭区间）',
            dw_end_date    DATE   COMMENT '该版本生效结束日期（开区间；当前版本=9999-12-31）',
            dw_is_current  INT    COMMENT '是否当前版本 1/0'
        )
        COMMENT 'DIM层-商品维度类型2拉链表（SCD2，保留属性历史版本）'
        STORED AS PARQUET
        LOCATION '{DIM_PRODUCT_SCD2_PATH}'
    """))

    stmts.append(("dim", "dim_date", f"""
        CREATE EXTERNAL TABLE IF NOT EXISTS {DBS['dim']}.dim_date (
            date_id      DATE COMMENT '日期（主键）',
            `year`       INT  COMMENT '年',
            `month`      INT  COMMENT '月',
            `day`        INT  COMMENT '日',
            week_of_year INT  COMMENT '当年第几周',
            day_of_week  INT  COMMENT '周几（1=周一）',
            is_workday   INT  COMMENT '是否工作日 1/0'
        )
        COMMENT 'DIM层-日期维度表'
        STORED AS PARQUET
        LOCATION '{DIM_DATE_PATH}'
    """))

    stmts.append(("dim", "dim_user", f"""
        CREATE EXTERNAL TABLE IF NOT EXISTS {DBS['dim']}.dim_user (
            user_id            BIGINT COMMENT '用户ID（主键）',
            first_seen_date    DATE   COMMENT '首次活跃日',
            last_seen_date     DATE   COMMENT '末次活跃日',
            active_days        BIGINT COMMENT '活跃天数（有行为的不同日期数）',
            first_purchase_date DATE  COMMENT '首次购买日（从未购买为 NULL）',
            purchase_days      BIGINT COMMENT '购买天数'
        )
        COMMENT 'DIM层-用户基础维度（含时间属性：支撑新老客/生命周期；活跃度口径见 active_days）'
        STORED AS PARQUET
        LOCATION '{DIM_USER_PATH}'
    """))

    stmts.append(("dim", "dim_category", f"""
        CREATE EXTERNAL TABLE IF NOT EXISTS {DBS['dim']}.dim_category (
            category_id   BIGINT COMMENT '品类ID（主键）',
            category_code STRING COMMENT '品类编码:electronics.smartphone',
            category_l1   STRING COMMENT '一级类目',
            category_l2   STRING COMMENT '二级类目',
            category_l3   STRING COMMENT '三级类目'
        )
        COMMENT 'DIM层-品类基础维度（自 category_code 拆层级，不做雪花下挂）'
        STORED AS PARQUET
        LOCATION '{DIM_CATEGORY_PATH}'
    """))

    stmts.append(("dim", "dim_session", f"""
        CREATE EXTERNAL TABLE IF NOT EXISTS {DBS['dim']}.dim_session (
            user_session       STRING    COMMENT '会话ID（主键）',
            user_id            BIGINT    COMMENT '会话归属用户ID',
            session_start_time TIMESTAMP COMMENT '会话首次行为时间',
            session_end_time   TIMESTAMP COMMENT '会话末次行为时间'
        )
        COMMENT 'DIM层-会话基础维度（存会话→用户归属）'
        STORED AS PARQUET
        LOCATION '{DIM_SESSION_PATH}'
    """))

    # ---------------- DWS ----------------
    stmts.append(("dws", "dws_user_session_daily", f"""
        CREATE EXTERNAL TABLE IF NOT EXISTS {DBS['dws']}.dws_user_session_daily (
            user_id              BIGINT COMMENT '用户ID',
            user_session         STRING COMMENT '会话ID',
            view_cnt             BIGINT COMMENT '浏览次数',
            cart_cnt             BIGINT COMMENT '加购次数',
            purchase_cnt         BIGINT COMMENT '购买次数',
            remove_cart_cnt      BIGINT COMMENT '移出购物车次数',
            session_duration_sec BIGINT COMMENT '会话时长(秒)=GREATEST(MAX-MIN,0)'
        )
        COMMENT 'DWS层-日×用户×会话 行为汇总'
        PARTITIONED BY (event_date DATE COMMENT '事件UTC日期')
        STORED AS PARQUET
        LOCATION '{DWS['user_session']}'
    """))

    stmts.append(("dws", "dws_product_daily", f"""
        CREATE EXTERNAL TABLE IF NOT EXISTS {DBS['dws']}.dws_product_daily (
            product_id    BIGINT COMMENT '商品ID',
            category_id   BIGINT COMMENT '品类ID（快照口径，冗余自事实表）',
            brand         STRING COMMENT '品牌（快照口径，冗余自事实表）',
            exposure_uv   BIGINT COMMENT '曝光UV=浏览去重用户数',
            cart_uv       BIGINT COMMENT '加购用户数',
            purchase_uv   BIGINT COMMENT '购买用户数',
            sales_cnt     BIGINT COMMENT '销量=购买行为数',
            sales_amount  DOUBLE COMMENT '销售额=采购行为price合计'
        )
        COMMENT 'DWS层-日×商品 行为汇总（星型宽表，聚合不 JOIN 维度）'
        PARTITIONED BY (event_date DATE COMMENT '事件UTC日期')
        STORED AS PARQUET
        LOCATION '{DWS['product']}'
    """))

    stmts.append(("dws", "dws_user_behavior_daily", f"""
        CREATE EXTERNAL TABLE IF NOT EXISTS {DBS['dws']}.dws_user_behavior_daily (
            user_id              BIGINT COMMENT '用户ID',
            active_action_cnt    BIGINT COMMENT '当日行为总数',
            purchase_amount      DOUBLE COMMENT '购买金额',
            purchase_product_cnt BIGINT COMMENT '购买商品数(去重)',
            purchase_cnt         BIGINT COMMENT '购买次数'
        )
        COMMENT 'DWS层-日×用户 行为汇总（DAU/RFM 基础）'
        PARTITIONED BY (event_date DATE COMMENT '事件UTC日期')
        STORED AS PARQUET
        LOCATION '{DWS['user_behavior']}'
    """))

    stmts.append(("dws", "dws_traffic_hour_daily", f"""
        CREATE EXTERNAL TABLE IF NOT EXISTS {DBS['dws']}.dws_traffic_hour_daily (
            event_hour         INT    COMMENT '事件小时 0~23',
            pv                 BIGINT COMMENT '事件数（浏览/加购/下单全量）',
            uv                 BIGINT COMMENT '去重用户数',
            view_pv            BIGINT COMMENT '浏览PV',
            cart_cnt           BIGINT COMMENT '加购次数',
            purchase_cnt       BIGINT COMMENT '购买次数',
            purchase_uv        BIGINT COMMENT '购买用户数',
            gmv                DOUBLE COMMENT '成交金额',
            session_cnt        BIGINT COMMENT '会话数（去重）'
        )
        COMMENT 'DWS层-日×小时 分时流量汇总（分时分析唯一入口；明细1.37M行/天压到24行/天）'
        PARTITIONED BY (event_date DATE COMMENT '事件UTC日期')
        STORED AS PARQUET
        LOCATION '{DWS['traffic_hour']}'
    """))

    # ---------------- ADS ----------------
    stmts.append(("ads", "ads_trade_daily", f"""
        CREATE EXTERNAL TABLE IF NOT EXISTS {DBS['ads']}.ads_trade_daily (
            gmv             DOUBLE COMMENT '销售额=SUM(purchase_amount)',
            order_cnt       BIGINT COMMENT '订单数=SUM(purchase_cnt)',
            buyer_cnt       BIGINT COMMENT '购买用户数',
            avg_order_value DOUBLE COMMENT '客单价=gmv/order_cnt',
            arppu           DOUBLE COMMENT '付费用户人均=gmv/buyer_cnt'
        )
        COMMENT 'ADS层-交易日报（运营大盘：GMV/订单/客单价/ARPPU）'
        PARTITIONED BY (event_date DATE COMMENT '事件UTC日期')
        STORED AS PARQUET
        LOCATION '{ADS['trade']}'
    """))

    stmts.append(("ads", "ads_conversion_funnel_daily", f"""
        CREATE EXTERNAL TABLE IF NOT EXISTS {DBS['ads']}.ads_conversion_funnel_daily (
            view_uv                 BIGINT COMMENT '浏览UV（跨商品去重）',
            cart_uv                 BIGINT COMMENT '加购UV（跨商品去重）',
            purchase_uv             BIGINT COMMENT '购买UV（跨商品去重）',
            view_to_cart_rate       DOUBLE COMMENT '浏览→加购转化率',
            view_to_purchase_rate   DOUBLE COMMENT '浏览→购买转化率',
            cart_to_purchase_rate   DOUBLE COMMENT '加购→购买转化率（可>100%，用户可绕过加购直购）'
        )
        COMMENT 'ADS层-转化漏斗日报'
        PARTITIONED BY (event_date DATE COMMENT '事件UTC日期')
        STORED AS PARQUET
        LOCATION '{ADS['funnel']}'
    """))

    stmts.append(("ads", "ads_product_hot_rank_daily", f"""
        CREATE EXTERNAL TABLE IF NOT EXISTS {DBS['ads']}.ads_product_hot_rank_daily (
            `rank`        INT    COMMENT '当日排名（按销售额降序）',
            product_id    BIGINT COMMENT '商品ID',
            category_id   BIGINT COMMENT '品类ID',
            brand         STRING COMMENT '品牌',
            sales_cnt     BIGINT COMMENT '销量',
            sales_amount  DOUBLE COMMENT '销售额',
            exposure_uv   BIGINT COMMENT '曝光UV'
        )
        COMMENT 'ADS层-商品热榜 Top100（逐日）'
        PARTITIONED BY (event_date DATE COMMENT '事件UTC日期')
        STORED AS PARQUET
        LOCATION '{ADS['hot_rank']}'
    """))

    stmts.append(("ads", "ads_session_behavior_daily", f"""
        CREATE EXTERNAL TABLE IF NOT EXISTS {DBS['ads']}.ads_session_behavior_daily (
            session_cnt        BIGINT COMMENT '会话数',
            avg_duration_sec   DOUBLE COMMENT '平均会话时长(秒)',
            avg_action_cnt     DOUBLE COMMENT '平均行为数',
            cart_purchase_rate DOUBLE COMMENT '加购转化率=SUM(purchase_cnt)/SUM(cart_cnt)'
        )
        COMMENT 'ADS层-会话行为日报'
        PARTITIONED BY (event_date DATE COMMENT '事件UTC日期')
        STORED AS PARQUET
        LOCATION '{ADS['session']}'
    """))

    stmts.append(("ads", "ads_traffic_hour_daily", f"""
        CREATE EXTERNAL TABLE IF NOT EXISTS {DBS['ads']}.ads_traffic_hour_daily (
            event_hour     INT    COMMENT '事件小时 0~23',
            time_bucket    STRING COMMENT '时段分桶：凌晨(0-6)/上午(7-12)/下午(13-18)/晚间(19-23)',
            pv             BIGINT COMMENT '事件数',
            uv             BIGINT COMMENT '去重用户数',
            purchase_cnt   BIGINT COMMENT '购买次数',
            purchase_uv    BIGINT COMMENT '购买用户数',
            gmv            DOUBLE COMMENT '成交金额',
            pv_share       DOUBLE COMMENT '该小时PV占全天比（×100，%）',
            order_share    DOUBLE COMMENT '该小时订单占全天比（%）',
            purchase_rate  DOUBLE COMMENT '该小时下单转化率=购买UV/UV',
            is_peak_hour   INT    COMMENT '是否当日流量峰值小时 1/0（支持多峰并列）'
        )
        COMMENT 'ADS层-分时大盘（运营时段表：投放/排班/扩容）'
        PARTITIONED BY (event_date DATE COMMENT '事件UTC日期')
        STORED AS PARQUET
        LOCATION '{ADS['hour']}'
    """))

    stmts.append(("ads", "ads_user_retention_daily", f"""
        CREATE EXTERNAL TABLE IF NOT EXISTS {DBS['ads']}.ads_user_retention_daily (
            d0_uv     BIGINT COMMENT '基准日活跃用户数',
            d1_uv     BIGINT COMMENT '次日仍活跃用户数',
            d1_rate   DOUBLE COMMENT '次日留存率（×100，%）',
            d3_uv     BIGINT COMMENT '3 日后仍活跃用户数',
            d3_rate   DOUBLE COMMENT '3 日留存率（%）',
            d7_uv     BIGINT COMMENT '7 日后仍活跃用户数',
            d7_rate   DOUBLE COMMENT '7 日留存率（%）'
        )
        COMMENT 'ADS层-用户留存日报（基准日=event_date 分区；超出数据区间的窗口为 NULL，非 0）'
        PARTITIONED BY (event_date DATE COMMENT '留存基准日 D0')
        STORED AS PARQUET
        LOCATION '{ADS['retention']}'
    """))

    # RFM 为全量快照，分区列用 as_of_dt（快照日）
    stmts.append(("ads", "ads_user_rfm_snapshot", f"""
        CREATE EXTERNAL TABLE IF NOT EXISTS {DBS['ads']}.ads_user_rfm_snapshot (
            user_id   BIGINT COMMENT '用户ID（主键）',
            r_days    BIGINT COMMENT 'Recency=快照日-末次购买日（天）',
            f_cnt     BIGINT COMMENT 'Frequency=累计购买次数',
            m_amount  DOUBLE COMMENT 'Monetary=累计购买金额',
            rfm_seg   STRING COMMENT 'RFM分群标签（8类）'
        )
        COMMENT 'ADS层-用户RFM分群快照（全量，非按天追加）'
        PARTITIONED BY (as_of_dt STRING COMMENT '快照日期 yyyy-MM-dd')
        STORED AS PARQUET
        LOCATION '{ADS['rfm']}'
    """))

    return stmts


def create_databases(spark):
    for db in DBS.values():
        spark.sql(f"CREATE DATABASE IF NOT EXISTS {db}")
    print(f"✅ 建库完成：{', '.join(DBS.values())}")


def create_all(spark, drop=False, drop_tables=None):
    """drop=False 只建缺失的表；drop=True 全部重建；drop_tables 只重建指定表（改 schema 时用）。"""
    create_databases(spark)
    targets = set(drop_tables or [])
    for db, tbl, ddl in ddl_statements():
        if drop or f"{db}.{tbl}" in targets:
            spark.sql(f"DROP TABLE IF EXISTS {db}.{tbl}")
        spark.sql(ddl)
        print(f"  建表 {db}.{tbl}")
    print(f"✅ 建表完成")


def repair_partitions(spark):
    """MSCK REPAIR：把 HDFS 上已存在的分区目录登记进 metastore（外部表重建后必需）。"""
    spark.sql(f"MSCK REPAIR TABLE {DBS['ods']}.ods_event_log")
    print("✅ ods_event_log 分区已注册")


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--drop", action="store_true", help="先 DROP TABLE 再建（HDFS 文件保留）")
    ap.add_argument("--drop-tables", default="", help="只重建指定表，逗号分隔，如 dim.dim_user")
    args = ap.parse_args()

    spark = get_spark("DDL", memory="2g")
    spark.sparkContext.setLogLevel("ERROR")
    targets = [t.strip() for t in args.drop_tables.split(",") if t.strip()]
    create_all(spark, drop=args.drop, drop_tables=targets)
    repair_partitions(spark)
    # 分区表重建后旧分区目录要重新登记（外部表 DROP 不删文件，新表只认得本次 INSERT 的分区）
    for db, tbl, _ in ddl_statements():
        if f"{db}.{tbl}" in targets and tbl != "ods_event_log":
            try:
                spark.sql(f"MSCK REPAIR TABLE {db}.{tbl}")
                print(f"  MSCK REPAIR {db}.{tbl}")
            except Exception:  # 非分区表会报错，忽略
                pass

    for db in DBS.values():
        try:
            tbls = [t.tableName for t in spark.sql(f"SHOW TABLES IN {db}").collect()]
            print(f"  [{db}] {len(tbls)} 张: {tbls}")
        except Exception as e:  # noqa: BLE001
            print(f"  [{db}] ERR {str(e)[:60]}")
    spark.stop()


if __name__ == "__main__":
    main()
