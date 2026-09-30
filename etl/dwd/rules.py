# -*- coding: utf-8 -*-
"""
DWD 层清洗规则（本层两张表共用）
================================
dwd_event_fact（干净事实表）和 dwd_event_dirty（脏数据隔离表）必须用**同一套判定规则**，
否则"干净 + 脏 + 去重 = ODS"这条对账账目会对不平。规则原文从 dwd_clean.py 搬来，未改动。
"""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent))

from config.config import BEHAVIOR_CN, VALID_EVENT_TYPES  # noqa: E402

# 脏数据打标：优先级 null_key > invalid_event_type > negative_price（复合脏数据只记首要原因）
DIRTY_CLASSIFY_SQL = """
    CASE
        WHEN event_time IS NULL OR user_id IS NULL OR product_id IS NULL THEN 'null_key'
        WHEN event_type IS NULL OR event_type NOT IN ({enums}) THEN 'invalid_event_type'
        WHEN price < 0 THEN 'negative_price'
    END
""".format(enums=",".join(f"'{e}'" for e in VALID_EVENT_TYPES))

# 派生列：behavior_type 中文映射（CASE WHEN 展开，避免 UDF 序列化开销）
BEHAVIOR_CASE_SQL = "CASE event_type " + " ".join(
    f"WHEN '{k}' THEN '{v}'" for k, v in BEHAVIOR_CN.items()
) + " END"
