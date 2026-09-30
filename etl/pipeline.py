#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
pipeline.py — 全链路编排（一个进程、一个 SparkSession 跑完一段区间）
====================================================================
把各层的表脚本按依赖顺序串起来，**复用同一个 SparkSession**（省掉每天十几次会话启动开销）。
每张表也都能单独跑（`python etl/<层>/<表>.py`），单表跑失败时便于重试与定位。

依赖顺序：
    ODS(已有分区) → DWD 事实/脏数据 → DWD DQC → DWS 四张 → DIM 四张 → ADS 五张日报
    ADS 的 retention / rfm 是**区间型**（RFM 是快照），单独跑：
        python etl/ads/ads_user_retention_daily.py --start D --end D
        python etl/ads/ads_user_rfm_snapshot.py --start D --end D

用法：
  python etl/pipeline.py --start 2019-10-01 --end 2019-10-31
  python etl/pipeline.py --dt 2019-11-01              # 单日增量（含 DIM 增量）
  python etl/pipeline.py --dt 2019-11-01 --only dwd,dws
  python etl/pipeline.py --start 2019-10-01 --end 2019-10-31 --check-only
"""

import argparse
import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from etl.ads.ads_conversion_funnel_daily import build as ads_funnel  # noqa: E402
from etl.ads.ads_product_hot_rank_daily import build as ads_rank  # noqa: E402
from etl.ads.ads_session_behavior_daily import build as ads_session  # noqa: E402
from etl.ads.ads_trade_daily import build as ads_trade  # noqa: E402
from etl.ads.ads_traffic_hour_daily import build as ads_hour  # noqa: E402
from etl.dim import dim_category, dim_session, dim_user  # noqa: E402
from etl.dim.dim_product_scd2 import build_inc as dim_scd2_inc  # noqa: E402
from etl.dwd import dwd_event_dirty, dwd_event_fact, dwd_event_fact_dqc  # noqa: E402
from etl.dws import (  # noqa: E402
    dws_product_daily, dws_traffic_hour_daily, dws_user_behavior_daily,
    dws_user_session_daily,
)
from etl.utils import date_range, get_spark  # noqa: E402

# (步骤名, 层级, 是否需要 dt) —— 顺序即依赖顺序
STEPS = [
    ("dwd_event_fact",       "dwd", dwd_event_fact.build),
    ("dwd_event_dirty",      "dwd", dwd_event_dirty.build),
    ("dws_user_session",     "dws", dws_user_session_daily.build),
    ("dws_product",          "dws", dws_product_daily.build),
    ("dws_user_behavior",    "dws", dws_user_behavior_daily.build),
    ("dws_traffic_hour",     "dws", dws_traffic_hour_daily.build),
    ("dim_product_scd2",     "dim", dim_scd2_inc),
    ("dim_user",             "dim", dim_user.build_inc),
    ("dim_session",          "dim", dim_session.build_inc),
    ("dim_category",         "dim", dim_category.build_inc),
    ("ads_trade",            "ads", ads_trade),
    ("ads_conversion_funnel", "ads", ads_funnel),
    ("ads_product_hot_rank", "ads", ads_rank),
    ("ads_session_behavior", "ads", ads_session),
    ("ads_traffic_hour",     "ads", ads_hour),
]
LAYERS = ["dwd", "dws", "dim", "ads"]


def run_checks(spark, start, end):
    """跨层对账：DWD 明细 ↔ 各 DWS ↔ ADS 的守恒不变量。"""
    from config.config import DBS, PARTITION_COL as P
    print("\n🔎 跨层对账")
    q = lambda s: spark.sql(s).collect()[0]  # noqa: E731

    dwd = q(f"""SELECT COUNT(*) rows_cnt, SUM(is_purchase) buy,
                       SUM(CASE WHEN is_purchase=1 THEN price ELSE 0 END) gmv
                FROM {DBS['dwd']}.dwd_event_fact
                WHERE {P} BETWEEN '{start}' AND '{end}'""")
    dws = q(f"""SELECT SUM(purchase_cnt) buy, SUM(purchase_amount) gmv
                FROM {DBS['dws']}.dws_user_behavior_daily
                WHERE {P} BETWEEN '{start}' AND '{end}'""")
    ads = q(f"""SELECT SUM(order_cnt) buy, SUM(gmv) gmv FROM {DBS['ads']}.ads_trade_daily
                WHERE {P} BETWEEN '{start}' AND '{end}'""")
    prod = q(f"""SELECT SUM(sales_cnt) buy, SUM(sales_amount) gmv
                 FROM {DBS['dws']}.dws_product_daily
                 WHERE {P} BETWEEN '{start}' AND '{end}'""")
    ses = q(f"""SELECT SUM(purchase_cnt) buy FROM {DBS['dws']}.dws_user_session_daily
                WHERE {P} BETWEEN '{start}' AND '{end}'""")

    print(f"  DWD 明细 {dwd['rows_cnt']:,} 行 / 购买 {dwd['buy']:,} / GMV {dwd['gmv']:,.2f}")
    for name, base, others in [
        ("购买次数守恒 DWD=DWS用户=DWS商品=DWS会话=ADS",
         dwd["buy"], [dws["buy"], prod["buy"], ses["buy"], ads["buy"]]),
        ("GMV 守恒 DWD=DWS用户=DWS商品=ADS",
         round(dwd["gmv"], 2), [round(dws["gmv"], 2), round(prod["gmv"], 2), round(ads["gmv"], 2)]),
    ]:
        ok = all(o == base for o in others)
        print(f"  {'✅' if ok else '❌'} {name}: {base:,} vs {[f'{o:,}' for o in others]}")

    bad = q(f"""SELECT COUNT(*) c FROM {DBS['ads']}.ads_conversion_funnel_daily
                WHERE {P} BETWEEN '{start}' AND '{end}'
                  AND NOT (view_uv >= cart_uv AND view_uv >= purchase_uv)""")["c"]
    print(f"  {'✅' if bad == 0 else '❌'} 漏斗不变量（每日 view_uv 为上游最大值）: {bad} 天违规")

    bad2 = q(f"""SELECT COUNT(*) c FROM (
                    SELECT product_id FROM {DBS['dim']}.dim_product_scd2
                    GROUP BY product_id HAVING SUM(dw_is_current) <> 1) t""")["c"]
    print(f"  {'✅' if bad2 == 0 else '❌'} SCD2 每商品单一当前版本: {bad2} 个违规")

    hour = q(f"""SELECT SUM(pv) pv, SUM(purchase_cnt) buy FROM {DBS['dws']}.dws_traffic_hour_daily
                 WHERE {P} BETWEEN '{start}' AND '{end}'""")
    hour_ads = q(f"""SELECT SUM(pv) pv FROM {DBS['ads']}.ads_traffic_hour_daily
                     WHERE {P} BETWEEN '{start}' AND '{end}'""")
    ok_hour = hour["pv"] == dwd["rows_cnt"] and hour_ads["pv"] == dwd["rows_cnt"]
    print(f"  {'✅' if ok_hour else '❌'} 分时守恒 DWS=ADS=DWD明细: "
          f"PV {hour['pv']:,}={hour_ads['pv']:,}={dwd['rows_cnt']:,} / 购买 {hour['buy']:,}")

    ret = q(f"""SELECT COUNT(*) c,
                       SUM(CASE WHEN d7_rate IS NULL THEN 1 ELSE 0 END) censored
                FROM {DBS['ads']}.ads_user_retention_daily
                WHERE {P} BETWEEN '{start}' AND '{end}'""")
    bad_ret = q(f"""SELECT COUNT(*) c FROM {DBS['ads']}.ads_user_retention_daily
                    WHERE {P} BETWEEN '{start}' AND '{end}'
                      AND ((d1_uv IS NOT NULL AND d1_uv > d0_uv)
                        OR (d3_uv IS NOT NULL AND d3_uv > d0_uv)
                        OR (d7_uv IS NOT NULL AND d7_uv > d0_uv))""")["c"]
    print(f"  {'✅' if bad_ret == 0 else '❌'} 留存不变量（D+n 人数 ≤ D0）: {bad_ret} 天违规；"
          f"其中 {ret['censored']} 天右删失（NULL）")


def main():
    import config.config as cfg
    ap = argparse.ArgumentParser()
    ap.add_argument("--dt", help="单日：只跑这一天（DIM 走增量）")
    ap.add_argument("--start", default=cfg.DATA_START)
    ap.add_argument("--end", default=cfg.DATA_END)
    ap.add_argument("--only", default=",".join(LAYERS), help="只跑部分层，逗号分隔")
    ap.add_argument("--steps", default="", help="只跑指定步骤（逗号分隔，见 STEPS 名）")
    ap.add_argument("--check-only", action="store_true", help="只做对账，不构建")
    args = ap.parse_args()

    days = [args.dt] if args.dt else date_range(args.start, args.end)
    start, end = (args.dt, args.dt) if args.dt else (args.start, args.end)
    layers = [x.strip() for x in args.only.split(",") if x.strip()]
    only_steps = [x.strip() for x in args.steps.split(",") if x.strip()]
    wanted = [s for s in STEPS
              if s[1] in layers and (not only_steps or s[0] in only_steps)]

    t0 = time.time()
    print(f"🚀 数仓全链路 {start} ~ {end}（{len(days)} 天）层={layers} "
          f"步骤={[s[0] for s in wanted]}", flush=True)

    spark = get_spark("Pipeline")
    spark.sparkContext.setLogLevel("ERROR")

    if not args.check_only:
        for i, (name, layer, fn) in enumerate(wanted, 1):
            print(f"\n=== [{i}/{len(wanted)}] {layer}.{name} ===", flush=True)
            for d in days:
                fn(spark, d)

    run_checks(spark, start, end)
    print(f"\n🎉 完成，耗时 {(time.time() - t0) / 60:.1f} 分钟")
    spark.stop()


if __name__ == "__main__":
    main()
