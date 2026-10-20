# -*- coding: utf-8 -*-
"""
数仓统一配置

集中管理 HDFS 路径、源数据 Schema 与业务口径常量。

新表口径与 my_second_project 旧表对齐，可拼接使用（旧=2019-10，新=2019-11）：
分区列统一 event_date；event_type 四类枚举 view/cart/remove_from_cart/purchase；
去重键为 (user_id, event_time, product_id, event_type)。
"""

import os

from pyspark.sql.types import (
    StructType, StructField, TimestampType, StringType,
    LongType, DoubleType,
)

# ---------------- HDFS 路径 ----------------
HDFS_ROOT = "hdfs://localhost:8020"
WAREHOUSE = f"{HDFS_ROOT}/home/lst/hadoop-data/warehouse"

# 源数据（已上传 HDFS 的 2019-11 行为日志）
SRC_CSV_2019NOV = f"{HDFS_ROOT}/home/lst/hadoop-data/eCommerce_behavior/2019-Nov.csv"

# ODS / DWD（分区: event_date）
ODS_PATH = f"{WAREHOUSE}/ods_event_log"
DWD_FACT_PATH = f"{WAREHOUSE}/dwd_event_fact"
DWD_DIRTY_PATH = f"{WAREHOUSE}/dwd_event_dirty"
DWD_FACT_INC_PATH = f"{WAREHOUSE}/dwd_ec_user_behavior_inc"  # 星型事实表
DWD_FACT_INC_PATH_ec= f"{WAREHOUSE}/dwd_ec_user_behavior_inc_ec"

# 维度表路径（外部表 LOCATION）
DIM_PRODUCT_SCD2_PATH = f"{WAREHOUSE}/dim/dim_product_scd2"
DIM_CATEGORY_PATH     = f"{WAREHOUSE}/dim/dim_category"
DIM_USER_PATH         = f"{WAREHOUSE}/dim/dim_user"
DIM_SESSION_PATH      = f"{WAREHOUSE}/dim/dim_session"
DIM_DATE_PATH         = f"{WAREHOUSE}/dim/dim_date"

# DWS（分区: event_date）
DWS = {
    "user_session": f"{WAREHOUSE}/dws_user_session_daily",
    "product":      f"{WAREHOUSE}/dws_product_daily",
    "user_behavior": f"{WAREHOUSE}/dws_user_behavior_daily",
    "traffic_hour": f"{WAREHOUSE}/dws_traffic_hour_daily",
}

# ADS（分区: event_date；RFM 为全量快照 as_of_dt）
ADS = {
    "funnel":   f"{WAREHOUSE}/ads/ads_conversion_funnel_daily",
    "trade":    f"{WAREHOUSE}/ads/ads_trade_daily",
    "hot_rank": f"{WAREHOUSE}/ads/ads_product_hot_rank_daily",
    "rfm":      f"{WAREHOUSE}/ads/ads_user_rfm_snapshot",
    "session":  f"{WAREHOUSE}/ads/ads_session_behavior_daily",
    "hour":     f"{WAREHOUSE}/ads/ads_traffic_hour_daily",
    "retention": f"{WAREHOUSE}/ads/ads_user_retention_daily",
}

DBS = {
    "ods": "ods",
    "dwd": "dwd",
    "dim": "dim",
    "dws": "dws",
    "ads": "ads",
}

PARTITION_COL = "event_date"       # 全链路统一分区列（dim_* 为全量表、RFM 用 as_of_dt）

DATA_START = "2019-10-01"
DATA_END = "2019-10-31"

FACT_COLUMNS = [
    "event_time", "event_type", "behavior_type", "product_id",
    "category_id", "category_code", "brand", "price",
    "user_id", "user_session", "is_purchase", "event_hour",
]
DIRTY_REASON_COL = "dirty_reason"

# ---------------- MySQL（ADS 落地，供报表/看板读取） ----------------
# 连接信息从环境变量读取（见 .env.example），不要提交真实口令。
MYSQL = {
    "host": os.environ.get("DW_MYSQL_HOST", "127.0.0.1"),
    "port": int(os.environ.get("DW_MYSQL_PORT", "3306")),
    "user": os.environ.get("DW_MYSQL_USER", "warehouse"),
    "password": os.environ.get("DW_MYSQL_PASSWORD", ""),
    "database": os.environ.get("DW_MYSQL_DATABASE", "dw_ads"),
    "charset": os.environ.get("DW_MYSQL_CHARSET", "utf8mb4"),
}

SRC_SCHEMA = StructType([
    StructField("event_time", TimestampType(), True),
    StructField("event_type", StringType(), True),
    StructField("product_id", LongType(), True),
    StructField("category_id", LongType(), True),
    StructField("category_code", StringType(), True),
    StructField("brand", StringType(), True),
    StructField("price", DoubleType(), True),
    StructField("user_id", LongType(), True),
    StructField("user_session", StringType(), True),
])

VALID_EVENT_TYPES = ["view", "cart", "remove_from_cart", "purchase"]

BEHAVIOR_CN = {
    "view": "浏览",
    "cart": "加购",
    "remove_from_cart": "移出购物车",
    "purchase": "购买",
}

DEDUP_KEYS = ["user_id", "event_time", "product_id", "event_type"]

# DQC 阈值
DQC_DIRTY_RATIO_LIMIT = 0.02     # 脏数据比例阻断阈值 2%
DQC_GMV_DAYS = 7                 # GMV 波动对比窗口
DQC_GMV_WARN = 0.30              # GMV 偏离近 7 日中位数 30% 告警（上涨、下跌都告警）
DQC_GMV_DROP_BLOCK = 0.50        # 低于基线 50% 阻断（仅下跌）
# 涨跌不对称：下跌多为漏数需阻断，上涨多为真实高峰，仅告警。
DQC_ROWS_7D_DAYS = 7
DQC_ROWS_WARN = 0.50             # 行数偏离近 7 日中位数 50% 告警（上涨、下跌都告警）
DQC_ROWS_DROP_BLOCK = 0.50       # 低于基线 50% 阻断（仅下跌）
# 同样只拦下跌：上涨疑似重复导入，由 DWD 去重键兜底。
DQC_NULL_MAX_RATE = 0.001        # 单字段空值率上限 0.1%（超出即阻断）
# 留 0.1% 容差，避免个别空值拦掉整天，整列缺失仍可拦截。
