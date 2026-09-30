#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
export_ads_powerbi.py — 导出 StarRocks ADS 结果到 CSV，供 Power BI / Excel 导入
==============================================================================================
- 导出表: 5 张 ADS + dim_category（供 Power BI 关联类目）
- 输出: UTF-8 BOM CSV（中文正常），默认写到 Windows 可访问的 Documents/starrocks_ads/
- 用法: <项目根目录>/.venv/bin/python etl/export_ads_powerbi.py [输出目录]
"""

import csv
import sys
import os
from pathlib import Path

import pymysql

SR_FE = (os.environ.get("SR_HOST", "127.0.0.1"), int(os.environ.get("SR_PORT", "9030")))
SR_USER, SR_PASS = os.environ.get("SR_USER", "root"), os.environ.get("SR_PASSWORD", "")
DB = "dwd"
# 默认输出目录：优先环境变量；WSL 下默认写到 Windows 的 Documents（Power BI 可直接打开），
# 非 WSL 环境则退回当前目录下的 starrocks_ads/
_WIN_DOCS = Path("/mnt/c/Users/{}".format(os.environ.get("USER", "user"))) / "Documents"
DEFAULT_OUT = Path(
    os.environ.get(
        "DW_EXPORT_DIR",
        str(_WIN_DOCS / "starrocks_ads") if _WIN_DOCS.parent.exists() else "starrocks_ads",
    )
)

# (表名, 中文说明)
TABLES = [
    ("ads_trade_daily", "交易日报：GMV / 订单数 / 购买用户 / 客单价 / ARPU"),
    ("ads_conversion_funnel_daily", "转化漏斗：浏览/加购/购买 UV + 转化率"),
    ("ads_product_hot_rank_daily", "商品热榜 Top100（逐日，按销售额）"),
    ("ads_session_behavior_daily", "会话行为：会话数 / 平均时长 / 加购转化率"),
    ("ads_user_rfm_snapshot", "用户 RFM 分群（全量快照，含分群标签）"),
    ("dim_category", "类目维度（供关联商品热榜的类目名）"),
]


def main():
    out_dir = Path(sys.argv[1]) if len(sys.argv) > 1 else DEFAULT_OUT
    out_dir.mkdir(parents=True, exist_ok=True)

    conn = pymysql.connect(host=SR_FE[0], port=SR_FE[1], user=SR_USER,
                           password=SR_PASS, charset="utf8")
    cur = conn.cursor()
    print(f"📦 导出到: {out_dir}\n")
    for name, desc in TABLES:
        cur.execute(f"SELECT * FROM {DB}.{name}")
        cols = [d[0] for d in cur.description]
        rows = cur.fetchall()
        path = out_dir / f"{name}.csv"
        with open(path, "w", newline="", encoding="utf-8-sig") as f:
            w = csv.writer(f)
            w.writerow(cols)
            for r in rows:
                w.writerow(["" if v is None else v for v in r])
        print(f"✅ {name:<34} {len(rows):>9,} 行  {path.name}")

    # README 说明
    readme = out_dir / "README.txt"
    lines = [
        "StarRocks ADS 导出（供 Power BI 导入）",
        "=" * 40,
        "",
        "Power BI 导入方式：",
        "  获取数据 → 文件夹 → 选择本目录 starrocks_ads → 确定（5+1 张 CSV）",
        "",
        "Power BI 直连 StarRocks（可选，更实时）：",
        "  获取数据 → MySQL 数据库 → 服务器 localhost:9030，数据库 dwd，用户名 root，密码留空",
        "  （需安装 MySQL 连接器；StarRocks FE 9030 为 MySQL 协议）",
        "",
        "表说明：",
    ]
    for name, desc in TABLES:
        lines.append(f"  - {name}.csv  {desc}")
    readme.write_text("\n".join(lines), encoding="utf-8-sig")
    print(f"\n📄 已写 {readme.name}")
    conn.close()


if __name__ == "__main__":
    main()
