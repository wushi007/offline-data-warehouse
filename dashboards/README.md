# 交易与转化日报看板

[下载 HTML](https://github.com/wushi007/offline-data-warehouse/raw/refs/heads/main/dashboards/trade_funnel_dashboard.html)，保存后用浏览器打开即可。
GitHub 文件页展示的是源代码，不会直接运行 HTML。

这是单文件离线看板，ECharts 与数据均内嵌，不需要联网、Python 服务或 StarRocks 连接。
支持日期区间筛选、GMV/客单价趋势、转化率趋势和单日漏斗联动。

数据来自 `ads.ads_trade_daily` 与 `ads.ads_conversion_funnel_daily` 的导出快照，覆盖 2019-10-01～2019-11-30，共 61 天。
GMV 合计 511,810,289.17，purchase 事件合计 1,680,272，与 README 中已有全量结果一致。页面不会自动获取后续数据。

指标口径：

- `order_cnt` 在本项目表示 purchase 事件次数，源数据没有真实订单主键。
- 区间买家数累加每天去重人数，是人次，不是整个区间去重人数；区间 ARPPU 也按此口径展示。
- 浏览、加购、购买分别按日去重，不是同一批用户依次经过各步骤的严格路径漏斗。购买/加购人数比可以超过 100%，需要结合跨日行为和源数据质量分析。

图表库的许可证说明保留在 HTML 内。本次接入未改变内嵌数据，未重新执行完整数仓。
