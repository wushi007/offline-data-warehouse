#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
ads.ads_user_rfm_snapshot — 用户 RFM 八分群（全量快照）
=======================================================
来源 dwd_event_fact 购买明细；粒度：全量购买用户（**非按天**，分区 as_of_dt = 快照日）。

口径：阈值 = 全量均值切分 F/M，R 以 30 天为界（无节假日/业务日历，口径写在此处）。
     分组：m/f/r 三档布尔组合成 8 类（重要价值/保持/发展/挽留 + 一般价值/保持/发展/挽留）。

用法：
  python etl/ads/ads_user_rfm_snapshot.py --start 2019-10-01 --end 2019-10-31
"""

import argparse
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent))

from config.config import DBS, PARTITION_COL  # noqa: E402
from etl.utils import get_spark  # noqa: E402

TABLE = f"{DBS['ads']}.ads_user_rfm_snapshot"
FACT = f"{DBS['dwd']}.dwd_event_fact"
P = PARTITION_COL

# RFM：全量快照（非按天），阈值 = 全量均值；R 以 30 天为界
SQL = f"""
    INSERT OVERWRITE TABLE {TABLE} PARTITION (as_of_dt = '{{as_of}}')
    WITH p AS (
        SELECT user_id,
               COUNT(*)          AS f_cnt,
               SUM(price)        AS m_amount,
               MAX({P})          AS last_buy_date
        FROM {FACT}
        WHERE is_purchase = 1 AND {P} BETWEEN '{{start}}' AND '{{end}}'
        GROUP BY user_id
    ),
    t AS (SELECT AVG(f_cnt) AS avg_f, AVG(m_amount) AS avg_m FROM p),
    scored AS (
        SELECT p.*,
               CAST(DATEDIFF(DATE '{{end}}', last_buy_date) AS BIGINT)               AS r_days,
               CAST(CASE WHEN DATEDIFF(DATE '{{end}}', last_buy_date) <= 30 THEN 1 ELSE 0 END AS INT) AS r_good,
               CAST(CASE WHEN p.f_cnt    >= t.avg_f    THEN 1 ELSE 0 END AS INT)     AS f_good,
               CAST(CASE WHEN p.m_amount >= t.avg_m    THEN 1 ELSE 0 END AS INT)     AS m_good
        FROM p CROSS JOIN t
    )
    SELECT
        user_id, r_days, f_cnt, m_amount,
        CASE
            WHEN m_good = 1 AND f_good = 1 AND r_good = 1 THEN '重要价值客户'
            WHEN m_good = 1 AND f_good = 1 AND r_good = 0 THEN '重要保持客户'
            WHEN m_good = 1 AND f_good = 0 AND r_good = 1 THEN '重要发展客户'
            WHEN m_good = 1 AND f_good = 0 AND r_good = 0 THEN '重要挽留客户'
            WHEN m_good = 0 AND f_good = 1 AND r_good = 1 THEN '一般价值客户'
            WHEN m_good = 0 AND f_good = 1 AND r_good = 0 THEN '一般保持客户'
            WHEN m_good = 0 AND f_good = 0 AND r_good = 1 THEN '一般发展客户'
            ELSE '一般挽留客户'
        END AS rfm_seg
    FROM scored
"""


def drop_all_partitions(spark):
    """清掉本表**所有已注册分区**（含幽灵分区）。

    为什么不能用 utils.drop_path 删目录：
        这是**分区外部表**。外部表的元数据在 metastore 里，删 HDFS 目录**不会**删掉分区注册，
        于是留下"幽灵分区"——metastore 说有、目录却不存在，查这张表不带分区条件时会报
        `[PATH_NOT_FOUND]`。反过来 DROP TABLE 不删文件是同一个问题的另一面：
        **目录和注册必须一起动**。
        正确做法是让 metastore 自己删分区：ALTER TABLE ... DROP PARTITION。
        （外部表 DROP PARTITION 只清元数据、不删数据文件；我们紧接着就重写它。）
    """
    for row in spark.sql(f"SHOW PARTITIONS {TABLE}").collect():
        spec = row[0]                     # 形如 "as_of_dt=2019-10-01"
        k, _, v = spec.partition("=")
        spark.sql(f"ALTER TABLE {TABLE} DROP PARTITION ({k}='{v}')")
        print(f"  🗑  注销分区 {spec}")


def build(spark, start, end):
    drop_all_partitions(spark)            # 快照表只保留最新一份：先注销旧分区（含幽灵）
    spark.sql(SQL.format(as_of=end, start=start, end=end))
    stats = spark.sql(f"""
        SELECT COUNT(*) AS users, SUM(m_amount) AS gmv,
               SUM(CASE WHEN rfm_seg = '重要价值客户' THEN 1 ELSE 0 END) AS vip,
               SUM(CASE WHEN rfm_seg = '重要价值客户' THEN m_amount ELSE 0 END) AS vip_gmv
        FROM {TABLE} WHERE as_of_dt = '{end}'
    """).collect()[0]
    print(f"  RFM 快照({end}): {stats['users']:,} 购买用户 / GMV {stats['gmv']:,.2f}；"
          f"重要价值客户 {stats['vip']:,} 人（{stats['vip'] / max(stats['users'], 1) * 100:.1f}%）"
          f"贡献 GMV {stats['vip_gmv']:,.2f}（{stats['vip_gmv'] / max(stats['gmv'], 0.01) * 100:.1f}%）")
    return stats


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--start", required=True)
    ap.add_argument("--end", required=True)
    args = ap.parse_args()

    spark = get_spark("ADS_UserRFMSnapshot")
    spark.sparkContext.setLogLevel("ERROR")
    print(f"📈 {TABLE} 快照区间 {args.start} ~ {args.end}（as_of_dt={args.end}）")
    build(spark, args.start, args.end)
    print("✅ 完成")
    spark.stop()


if __name__ == "__main__":
    main()
