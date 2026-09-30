# -*- coding: utf-8 -*-
"""
数仓统一配置
============
集中管理 HDFS 路径、源数据 Schema 与业务口径常量。

口径对齐说明（重要）：
    新表与 my_second_project 旧表口径对齐，可拼接使用（旧=2019-10，新=2019-11）。
    - 分区/日期列统一为 event_date（Design Doc 中的 dt 在本实现中映射为 event_date）
    - event_type 四类枚举：view / cart / remove_from_cart / purchase
    - 衍生字段 behavior_type(中文) / is_purchase(0/1) / event_hour 与旧表完全一致
    - 去重键沿用旧表 (user_id, event_time, product_id, event_type)
    新 dwd_event_fact 为旧 dwd_ecom_behavior 的超集（额外保留 price/session/brand/category_code）。
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

# ODS / DWD
ODS_PATH = f"{WAREHOUSE}/ods_event_log"            # 分区: event_date
DWD_FACT_PATH = f"{WAREHOUSE}/dwd_event_fact"      # 分区: event_date
DWD_DIRTY_PATH = f"{WAREHOUSE}/dwd_event_dirty"    # 分区: event_date
DWD_FACT_INC_PATH = f"{WAREHOUSE}/dwd_ec_user_behavior_inc"  # 星型事实表（分区: event_date）
DWD_FACT_INC_PATH_ec= f"{WAREHOUSE}/dwd_ec_user_behavior_inc_ec"

# 维度表路径（外部表 LOCATION；与统一口径维度定义一致）
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

# ---------------- Hive 库名（metastore 注册用） ----------------
DBS = {
    "ods": "ods",
    "dwd": "dwd",
    "dim": "dim",
    "dws": "dws",
    "ads": "ads",
}

PARTITION_COL = "event_date"       # 全链路统一分区列（除 dim_product_scd2 / dim_* 为全量表、RFM 用 as_of_dt）

# ---------------- 数据区间（本次落地：2019-10 整月） ----------------
DATA_START = "2019-10-01"
DATA_END = "2019-10-31"

# ---------------- DWD 落表口径 ----------------
# dwd_event_fact 13 列 = 9 源字段 + behavior_type / is_purchase / event_hour + event_date 分区列
FACT_COLUMNS = [
    "event_time", "event_type", "behavior_type", "product_id",
    "category_id", "category_code", "brand", "price",
    "user_id", "user_session", "is_purchase", "event_hour",
]
DIRTY_REASON_COL = "dirty_reason"

# ---------------- MySQL（ADS 落地，供报表/看板读取） ----------------
# 连接信息一律从环境变量读取（参考项目根目录 .env.example），
# 不要把真实口令写进代码后提交。
MYSQL = {
    "host": os.environ.get("DW_MYSQL_HOST", "127.0.0.1"),
    "port": int(os.environ.get("DW_MYSQL_PORT", "3306")),
    "user": os.environ.get("DW_MYSQL_USER", "warehouse"),
    "password": os.environ.get("DW_MYSQL_PASSWORD", ""),
    "database": os.environ.get("DW_MYSQL_DATABASE", "dw_ads"),
    "charset": os.environ.get("DW_MYSQL_CHARSET", "utf8mb4"),
}

# ---------------- 源数据 Schema（与旧 ODS 完全一致） ----------------
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

# ---------------- 业务口径常量 ----------------
VALID_EVENT_TYPES = ["view", "cart", "remove_from_cart", "purchase"]

# 行为中文映射（与旧表 behavior_type 一致）
BEHAVIOR_CN = {
    "view": "浏览",
    "cart": "加购",
    "remove_from_cart": "移出购物车",
    "purchase": "购买",
}

# DWD 去重键（沿用旧表口径）
DEDUP_KEYS = ["user_id", "event_time", "product_id", "event_type"]

# DQC 阈值
DQC_DIRTY_RATIO_LIMIT = 0.02     # 脏数据比例阻断阈值 2%
DQC_GMV_DAYS = 7                 # GMV 波动对比窗口
DQC_GMV_VOLATILITY = 0.30        # GMV 相对 7 日均波动 >30% 阻断
DQC_ROWS_7D_DAYS = 7
DQC_ROWS_WARN = 0.50             # 行数波动 50% 告警
DQC_ROWS_BLOCK = 0.80            # 行数波动 80% 阻断
