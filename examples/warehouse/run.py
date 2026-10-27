"""真实五层 ETL 的固定样本验收，不使用生产库表或路径。"""
import argparse
from datetime import date
import hashlib
import json
import os
from pathlib import Path


def snapshot(spark):
    from config.config import DBS
    from etl.ddl import ddl_statements
    result = {}
    for layer, table, _ in ddl_statements():
        rows = [r.asDict() for r in spark.table(f'{DBS[layer]}.{table}').collect()]
        # 类型统一为 JSON 字符串；仅处理这份小样本，不用于全量数据。
        canonical = sorted(json.dumps(r, sort_keys=True, default=str, ensure_ascii=False) for r in rows)
        result[f'{layer}.{table}'] = hashlib.sha256('\n'.join(canonical).encode()).hexdigest()
    return result


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument('--verify-rerun', action='store_true', help='完整重跑并逐表对比结果摘要')
    args = ap.parse_args()
    if os.environ.get('DW_LOCAL_DEMO') != '1':
        raise SystemExit('请通过 bash examples/warehouse/run.sh 执行，以隔离现有数据')
    root = Path(os.environ['DW_DEMO_ROOT']).resolve()
    if os.environ.get('DW_WAREHOUSE_URI') != (root / 'warehouse').as_uri():
        raise SystemExit('演示输出路径未隔离，拒绝执行')
    from config.config import DBS
    from etl.ddl import create_all
    from etl.ods.ods_event_log_bulk import import_csv
    from etl.pipeline import STEPS, run_steps, run_checks
    from etl.utils import date_range
    from pyspark.sql import SparkSession

    start, end = '2019-10-01', '2019-10-08'
    # 只在样例内配置本地 Hive；正式 Spark 工厂无需感知样例。
    spark = (SparkSession.builder.master('local[2]').appName('Warehouse_Sample')
             .config('spark.hadoop.fs.defaultFS', 'file:///')
             .config('spark.sql.warehouse.dir', (root/'managed').as_uri())
             .config('spark.hadoop.javax.jdo.option.ConnectionURL',
                     f"jdbc:derby:;databaseName={root/'metastore'};create=true")
             .config('spark.hadoop.hive.metastore.uris', '')
             .config('spark.eventLog.enabled', 'false')
             .config('spark.sql.session.timeZone', 'UTC')
             .config('spark.sql.sources.partitionOverwriteMode', 'dynamic')
             .config('spark.sql.shuffle.partitions', '2')
             .enableHiveSupport().getOrCreate())
    spark.sparkContext.setLogLevel('ERROR')
    checks = {}
    try:
        create_all(spark)
        source = Path(__file__).with_name('events.csv')
        import_csv(spark, source.as_uri())
        run_steps(spark, STEPS, date_range(start,end),start,end,False,start)
        checks['cross_layer'] = run_checks(spark,start,end)
        count = lambda table: spark.table(f'{DBS[table.split(".")[0]]}.{table.split(".")[1]}').count()
        checks['ods_37'] = count('ods.ods_event_log') == 37
        checks['dwd_32'] = count('dwd.dwd_event_fact') == 32
        checks['dirty_3'] = count('dwd.dwd_event_dirty') == 3
        facts = f"{DBS['dwd']}.dwd_event_fact"
        metrics = spark.sql(f'SELECT SUM(is_purchase) purchases, SUM(CASE WHEN is_purchase=1 THEN price ELSE 0 END) gmv FROM {facts}').first()
        checks['purchases_12_gmv_120'] = metrics.purchases == 12 and metrics.gmv == 120
        scd = spark.table(f"{DBS['dim']}.dim_product_scd2").where('product_id=1001').orderBy('dw_start_date').collect()
        checks['scd2_change'] = len(scd)==2 and scd[0].brand=='alpha' and scd[1].brand=='beta' and scd[0].dw_end_date==date(2019,10,5)
        retention=spark.table(f"{DBS['ads']}.ads_user_retention_daily")
        checks['retention_right_censoring'] = retention.where('d7_uv IS NULL').count()==7
        first=retention.where("event_date='2019-10-01'").first()
        checks['retention_day1'] = first.d0_uv==3 and first.d1_uv==2 and first.d7_uv==2
        rfm=spark.table(f"{DBS['ads']}.ads_user_rfm_snapshot")
        checks['rfm_snapshot'] = rfm.count()==2 and {r.as_of_dt for r in rfm.collect()}=={'2019-10-08'}
        # 不完整的早期窗口必须保持 NULL，不能用不存在的观测推断 0。
        from etl.ads.ads_user_retention_daily import build as retention_build
        retention_build(spark,start,'2019-10-03')
        checks['short_window_null'] = retention.where("event_date='2019-10-01'").first().d7_uv is None
        retention_build(spark,start,end)
        if args.verify_rerun:
            before=snapshot(spark)
            run_steps(spark,STEPS,date_range(start,end),start,end,False,start)
            checks['all_tables_rerun_identical'] = before==snapshot(spark)
        # 验证真实增量接口：同日重复执行不额外开版本，历史回补被拒绝。
        from etl.dim.dim_product_scd2 import build_inc
        before_scd=snapshot(spark)['dim.dim_product_scd2']
        build_inc(spark,end)
        checks['scd2_same_day_idempotent']=before_scd==snapshot(spark)['dim.dim_product_scd2']
        try:
            build_inc(spark,start)
        except (SystemExit, ValueError, RuntimeError):
            checks['scd2_backfill_rejected']=True
        else:
            checks['scd2_backfill_rejected']=False
        from etl.pipeline import _run_dwd_dqc
        from pyspark.errors import AnalysisException
        checks['missing_input_rejected']=False
        try:
            _run_dwd_dqc(spark,'2019-10-09')
        except AnalysisException as exc:
            checks['missing_input_rejected']=exc.getErrorClass() == 'PATH_NOT_FOUND'
        report={'sample':'events.csv','range':[start,end],'spark':spark.version,
                'tables':len(snapshot(spark)),'checks':checks,'passed':all(checks.values()),'output':str(root)}
        (root/'verification.json').write_text(json.dumps(report,ensure_ascii=False,indent=2))
        print('\n'+json.dumps(report,ensure_ascii=False,indent=2),flush=True)
        if not report['passed']:
            raise SystemExit('样本验收失败')
    finally:
        spark.stop()


if __name__ == '__main__':
    main()
