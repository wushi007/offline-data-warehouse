#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
pipeline.py — 全链路编排（一个进程、一个 SparkSession 跑完一段区间）

按依赖顺序串联各层表脚本，单表也可单独运行。用法：
  python etl/pipeline.py --start 2019-10-01 --end 2019-10-31
  python etl/pipeline.py --dt 2019-11-01 [--only dwd,dws] [--check-only]

区间模式（--start/--end）：
  - DWD / DWS / ADS 日报逐日构建；
  - DIM 走全量重算：SCD2 按 [start,end] 窗口，其余维表扫全部 DWD 分区；
  - 留存 / RFM 按 [start,end] 构建一次。
单日模式（--dt）：
  - DWD / DWS / ADS 日报构建当天；
  - DIM 走增量；
  - 留存 / RFM 按 [DATA_START, dt] 构建一次（与 DAG 的 ads_range 层一致）。
"""

import argparse
import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from etl.ads import ads_user_retention_daily, ads_user_rfm_snapshot  # noqa: E402
from etl.ads.ads_conversion_funnel_daily import build as ads_funnel  # noqa: E402
from etl.ads.ads_product_hot_rank_daily import build as ads_rank  # noqa: E402
from etl.ads.ads_session_behavior_daily import build as ads_session  # noqa: E402
from etl.ads.ads_trade_daily import build as ads_trade_build  # noqa: E402
from etl.ads.ads_trade_daily import check_gmv_volatility  # noqa: E402
from etl.ads.ads_traffic_hour_daily import build as ads_hour  # noqa: E402
from etl.dim import dim_category, dim_date, dim_session, dim_user  # noqa: E402
from etl.dim.dim_product_scd2 import build_inc as dim_scd2_inc  # noqa: E402
from etl.dim.dim_product_scd2 import rebuild as dim_scd2_rebuild  # noqa: E402
from etl.dwd import dwd_event_dirty, dwd_event_fact  # noqa: E402
from etl.dwd.dwd_event_fact_dqc import check_day as dwd_dqc_check  # noqa: E402
from etl.dws import (  # noqa: E402
    dws_product_daily, dws_traffic_hour_daily, dws_user_behavior_daily,
    dws_user_session_daily,
)
from etl.utils import date_range, get_spark  # noqa: E402


def _run_dwd_dqc(spark, dt):
    """DWD 质量闸门；有阻断项时抛错，避免下游继续跑。"""
    blocked = dwd_dqc_check(spark, dt)
    if blocked:
        raise SystemExit(f"FAIL: {dt} DWD 质量校验未通过：{', '.join(blocked)}")


def _run_ads_trade(spark, dt):
    """ADS 交易日报 + GMV 波动闸门（与脚本 __main__ 的口径一致）。"""
    ads_trade_build(spark, dt)
    is_blocked, msg = check_gmv_volatility(spark, dt)
    print(f"  {'FAIL' if is_blocked else 'OK'} [{dt}] {msg}")
    if is_blocked:
        raise SystemExit(f"FAIL: {dt} GMV 下跌超阈值，疑似购买事件漏传")


# 步骤描述：
#   daily     → fn(spark, dt)，每个日期执行一次
#   dim       → --dt: inc(spark, dt)；区间: rebuild(spark)
#   dim_scd2  → --dt: inc(spark, dt)；区间: rebuild(spark, start, end)
#   range     → --dt: fn(spark, DATA_START, dt)；区间: fn(spark, start, end)
#   ensure    → 静态表为空才生成（dim_date）
STEPS = [
    {"name": "dwd_event_fact", "layer": "dwd", "mode": "daily", "fn": dwd_event_fact.build},
    {"name": "dwd_event_dirty", "layer": "dwd", "mode": "daily", "fn": dwd_event_dirty.build},
    {"name": "dwd_event_fact_dqc", "layer": "dwd", "mode": "daily", "fn": _run_dwd_dqc},
    {"name": "dws_user_session", "layer": "dws", "mode": "daily", "fn": dws_user_session_daily.build},
    {"name": "dws_product", "layer": "dws", "mode": "daily", "fn": dws_product_daily.build},
    {"name": "dws_user_behavior", "layer": "dws", "mode": "daily", "fn": dws_user_behavior_daily.build},
    {"name": "dws_traffic_hour", "layer": "dws", "mode": "daily", "fn": dws_traffic_hour_daily.build},
    {"name": "dim_product_scd2", "layer": "dim", "mode": "dim_scd2",
     "inc": dim_scd2_inc, "rebuild": dim_scd2_rebuild},
    {"name": "dim_user", "layer": "dim", "mode": "dim",
     "inc": dim_user.build_inc, "rebuild": dim_user.rebuild},
    {"name": "dim_session", "layer": "dim", "mode": "dim",
     "inc": dim_session.build_inc, "rebuild": dim_session.rebuild},
    {"name": "dim_category", "layer": "dim", "mode": "dim",
     "inc": dim_category.build_inc, "rebuild": dim_category.rebuild},
    {"name": "dim_date", "layer": "dim", "mode": "ensure", "fn": dim_date.ensure},
    {"name": "ads_trade", "layer": "ads", "mode": "daily", "fn": _run_ads_trade},
    {"name": "ads_conversion_funnel", "layer": "ads", "mode": "daily", "fn": ads_funnel},
    {"name": "ads_product_hot_rank", "layer": "ads", "mode": "daily", "fn": ads_rank},
    {"name": "ads_session_behavior", "layer": "ads", "mode": "daily", "fn": ads_session},
    {"name": "ads_traffic_hour", "layer": "ads", "mode": "daily", "fn": ads_hour},
    {"name": "ads_user_retention", "layer": "ads", "mode": "range",
     "fn": ads_user_retention_daily.build_range},
    {"name": "ads_user_rfm", "layer": "ads", "mode": "range",
     "fn": ads_user_rfm_snapshot.build},
]
LAYERS = ["dwd", "dws", "dim", "ads"]


def run_steps(spark, steps, days, start, end, dt_mode, range_start):
    """按步骤模式执行；层内顺序即 STEPS 顺序。"""
    for i, st in enumerate(steps, 1):
        print(f"\n=== [{i}/{len(steps)}] {st['layer']}.{st['name']} ({st['mode']}) ===",
              flush=True)
        mode = st["mode"]
        if mode == "daily":
            for d in days:
                st["fn"](spark, d)
        elif mode == "dim":
            if dt_mode:
                st["inc"](spark, days[0])
            else:
                st["rebuild"](spark)
        elif mode == "dim_scd2":
            if dt_mode:
                st["inc"](spark, days[0])
            else:
                print(f"  SCD2 重建窗口 {start} ~ {end}"
                      f"（窗口外的历史版本会被覆盖，请传完整区间）")
                st["rebuild"](spark, start, end)
        elif mode == "range":
            st["fn"](spark, range_start, end)
        elif mode == "ensure":
            st["fn"](spark)
        else:
            raise SystemExit(f"未知步骤模式：{mode}")


def run_checks(spark, start, end):
    """跨层对账：DWD 明细 ↔ 各 DWS ↔ ADS 的守恒不变量。返回是否全部通过。"""
    from config.config import DBS, PARTITION_COL as P
    print("\n跨层对账")
    q = lambda s: spark.sql(s).collect()[0]  # noqa: E731

    def n(v):
        """表/分区为空时 SUM 返回 NULL，统一按 0 展示与比较。"""
        return int(v or 0)

    def f(v):
        return float(v or 0)

    dwd = q(f"""SELECT COUNT(*) rows_cnt, SUM(is_purchase) buy,
                       SUM(CASE WHEN is_purchase=1 THEN price ELSE 0 END) gmv
                FROM {DBS['dwd']}.dwd_event_fact
                WHERE {P} BETWEEN '{start}' AND '{end}'""")
    if dwd["rows_cnt"] == 0:
        print(f"  FAIL 区间 {start} ~ {end} 没有 DWD 数据")
        return False

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

    print(f"  DWD 明细 {dwd['rows_cnt']:,} 行 / 购买 {n(dwd['buy']):,} / GMV {f(dwd['gmv']):,.2f}")
    failures = []
    for name, base, others in [
        ("购买次数守恒 DWD=DWS用户=DWS商品=DWS会话=ADS",
         n(dwd["buy"]), [n(dws["buy"]), n(prod["buy"]), n(ses["buy"]), n(ads["buy"])]),
        ("GMV 守恒 DWD=DWS用户=DWS商品=ADS",
         round(f(dwd["gmv"]), 2),
         [round(f(dws["gmv"]), 2), round(f(prod["gmv"]), 2), round(f(ads["gmv"]), 2)]),
    ]:
        ok = all(o == base for o in others)
        print(f"  {'OK' if ok else 'FAIL'} {name}: {base:,} vs {[f'{o:,}' for o in others]}")
        if not ok:
            failures.append(name)

    bad = q(f"""SELECT COUNT(*) c FROM {DBS['ads']}.ads_conversion_funnel_daily
                WHERE {P} BETWEEN '{start}' AND '{end}'
                  AND NOT (view_uv >= cart_uv AND view_uv >= purchase_uv)""")["c"]
    print(f"  {'OK' if bad == 0 else 'FAIL'} 漏斗不变量（每日 view_uv 为上游最大值）: {bad} 天违规")
    if bad:
        failures.append("漏斗不变量")

    bad2 = q(f"""SELECT COUNT(*) c FROM (
                    SELECT product_id FROM {DBS['dim']}.dim_product_scd2
                    GROUP BY product_id HAVING SUM(dw_is_current) <> 1) t""")["c"]
    print(f"  {'OK' if bad2 == 0 else 'FAIL'} SCD2 每商品单一当前版本: {bad2} 个违规")
    if bad2:
        failures.append("SCD2 当前版本")

    hour = q(f"""SELECT SUM(pv) pv, SUM(purchase_cnt) buy FROM {DBS['dws']}.dws_traffic_hour_daily
                 WHERE {P} BETWEEN '{start}' AND '{end}'""")
    hour_ads = q(f"""SELECT SUM(pv) pv FROM {DBS['ads']}.ads_traffic_hour_daily
                     WHERE {P} BETWEEN '{start}' AND '{end}'""")
    hpv, hbuy, hapv = n(hour["pv"]), n(hour["buy"]), n(hour_ads["pv"])
    ok_hour = hpv == dwd["rows_cnt"] and hapv == dwd["rows_cnt"]
    print(f"  {'OK' if ok_hour else 'FAIL'} 分时守恒 DWS=ADS=DWD明细: "
          f"PV {hpv:,}={hapv:,}={dwd['rows_cnt']:,} / 购买 {hbuy:,}")
    if not ok_hour:
        failures.append("分时守恒")

    ret = q(f"""SELECT COUNT(*) c,
                       SUM(CASE WHEN d7_rate IS NULL THEN 1 ELSE 0 END) censored
                FROM {DBS['ads']}.ads_user_retention_daily
                WHERE {P} BETWEEN '{start}' AND '{end}'""")
    bad_ret = q(f"""SELECT COUNT(*) c FROM {DBS['ads']}.ads_user_retention_daily
                    WHERE {P} BETWEEN '{start}' AND '{end}'
                      AND ((d1_uv IS NOT NULL AND d1_uv > d0_uv)
                        OR (d3_uv IS NOT NULL AND d3_uv > d0_uv)
                        OR (d7_uv IS NOT NULL AND d7_uv > d0_uv))""")["c"]
    expected_days = len(date_range(start, end))
    ret_days = n(ret["c"])
    ok_ret = bad_ret == 0 and ret_days == expected_days
    print(f"  {'OK' if ok_ret else 'FAIL'} 留存覆盖与不变量: {ret_days}/{expected_days} 天，"
          f"{bad_ret} 天违规；其中 {int(ret['censored'] or 0)} 天右删失（NULL）")
    if not ok_ret:
        failures.append("留存覆盖/不变量")

    if failures:
        print(f"\nFAIL: 跨层对账未通过：{', '.join(failures)}")
        return False
    print("\nOK: 跨层对账全部通过")
    return True


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

    dt_mode = args.dt is not None
    if dt_mode:
        days = [args.dt]
        start = end = args.dt
        range_start = cfg.DATA_START          # 留存/RFM 与 DAG 的 ads_range 一致
    else:
        start, end = args.start, args.end
        days = date_range(start, end)
        range_start = start
    if not days:
        raise SystemExit(f"FAIL: 空日期区间 {start} ~ {end}")

    layers = [x.strip() for x in args.only.split(",") if x.strip()]
    only_steps = [x.strip() for x in args.steps.split(",") if x.strip()]
    wanted = [s for s in STEPS
              if s["layer"] in layers and (not only_steps or s["name"] in only_steps)]

    t0 = time.time()
    print(f"数仓全链路 {start} ~ {end}（{len(days)} 天）层={layers} "
          f"步骤={[s['name'] for s in wanted]}", flush=True)

    spark = get_spark("Pipeline")
    spark.sparkContext.setLogLevel("ERROR")

    if not args.check_only:
        run_steps(spark, wanted, days, start, end, dt_mode, range_start)

    ok = run_checks(spark, start, end)
    spark.stop()
    if not ok:
        sys.exit(1)
    print(f"\n完成，耗时 {(time.time() - t0) / 60:.1f} 分钟")


if __name__ == "__main__":
    main()
