# -*- coding: utf-8 -*-
"""
DWD 层清洗规则，dwd_event_fact 与 dwd_event_dirty 共用。

两表必须使用同一套判定，保证"干净 + 脏 + 去重 = ODS"可对账。
"""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent))

from config.config import BEHAVIOR_CN, VALID_EVENT_TYPES  # noqa: E402

# 复合脏数据只记首要原因，优先级 null_key > invalid_event_type > negative_price
DIRTY_CLASSIFY_SQL = """
    CASE
        WHEN event_time IS NULL OR user_id IS NULL OR product_id IS NULL THEN 'null_key'
        WHEN event_type IS NULL OR event_type NOT IN ({enums}) THEN 'invalid_event_type'
        WHEN price < 0 THEN 'negative_price'
    END
""".format(enums=",".join(f"'{e}'" for e in VALID_EVENT_TYPES))

# behavior_type 中文映射；用 CASE WHEN 展开，避免 UDF 序列化开销
BEHAVIOR_CASE_SQL = "CASE event_type " + " ".join(
    f"WHEN '{k}' THEN '{v}'" for k, v in BEHAVIOR_CN.items()
) + " END"
