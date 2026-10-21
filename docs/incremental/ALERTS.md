# 任务告警机制

> 代码：`scheduler/alerts.py`（通知模块）+ `scheduler/incremental_warehouse_dag.py`（回调接线）
> 已在 2026-09-29 的真实失败上验证（样例见文末）

## 一、设计原则

1. 不抛异常。所有通道逐个 best-effort，失败即静默降级。告警自身故障不能改变任务状态，否则正常任务会被标成失败。
2. 分级推送。只有质量阻断、失败、超时推送给人工；重试中只写入台账，避免网络抖动引起的重试淹没真实问题。
3. DQC 失败单独标注。`ods_layer` / `dwd_layer` 内部的 ODS/DWD DQC 失败语义是「数据质量不达标，已阻断下游」，与「脚本异常」分开报警，收到后可直接判断是数据问题还是代码问题。
4. 每条告警带行动信息。包含重跑命令和日志尾部，收到后不需要再翻 UI。

## 二、通道（前两个默认开启，其余由环境变量启用）

| 通道 | 默认 | 依赖 | 位置 / 开关 |
|---|---|---|---|
| 1 本地台账 | 开启 | 无 | `scheduler/alert_records/alerts-YYYY-MM-DD.jsonl`（结构化，可用 `jq` 查询），按天一个文件，放在项目内 |
| 2 Windows 告警文件 | 开启 | 无 | `/mnt/c/Users/<用户名>/Documents/数仓告警.txt`；目录不存在时自动跳过，可用 `DW_ALERT_WIN_FILE` 覆盖 |
| 3 桌面弹窗 | best-effort | 安装 BurntToast 后可用 | 未安装则静默跳过，不使用阻塞式 MessageBox |
| 4 HTTP Webhook | 由环境变量控制 | `DW_ALERT_WEBHOOK` | 企业微信 / 钉钉 / 通用 Webhook，手机推送 |
| 5 邮件 SMTP | 由环境变量控制 | `DW_ALERT_SMTP_HOST` / `_PORT` / `_USER` / `_PASS` / `_TO`（全部设置后启用） | — |
| 6 飞书机器人 | 由环境变量控制 | `DW_ALERT_FEISHU_WEBHOOK` / `_SECRET` / `_STYLE` | 卡片（默认）或文本；业务失败也返回 HTTP 200，`alerts.py` 会检查响应 body |

启用手机推送（拿到群机器人 URL 后，加入 `airflow/airflow-env.sh`）：
```bash
export DW_ALERT_WEBHOOK='https://qyapi.weixin.qq.com/cgi-bin/webhook/send?key=xxx'
```

## 三、事件分级与触发点

| 事件 | 级别 | 推送人工 | 触发来源 |
|---|---|---|---|
| `quality_block` | 数据质量阻断（已拦住下游） | 是 | `ods_layer`、`dwd_layer` 失败（层内 ODS/DWD DQC 返回非 0） |
| `quality_warn` | 数据质量异常（值超阈值，未阻断下游） | 是 | `ads_daily_layer`：GMV 相对近 7 日中位数波动 >30% |
| `failure` | 任务失败（重试已耗尽） | 是 | 其余所有任务（`default_args` 中也兜了一层） |
| `retry` | 任务重试中 | 否，只进台账 | 所有任务（`on_retry_callback`） |
| `timeout` | 任务超时（疑似卡死） | 是 | 保留字；Airflow 超时会走 `failure` |

超时：每个**层任务**设置 `execution_timeout`（ods 90 分 / dwd 75 分 / dws 110 分 / dim 120 分 / ads_daily 130 分 / ads_range 120 分，可用 `warehouse_env` 的 `<layer>_timeout_min` 覆盖）。由于 `max_active_tasks=1`，一个卡死的层会阻塞当天后续所有层，因此需要这个兜底。

## 四、台账字段

```
ts / event / level / dag_id / task_id / ds / run_id / try_number
reason / exception / log_url / rerun_cmd / log_tail
```

一条真实记录（2026-09-29 首次失败，代理配置问题）：

```json
{
  "ts": "2026-09-29 19:41:47",
  "event": "failure",
  "level": "任务失败（重试已耗尽）",
  "task_id": "check_env",
  "ds": "2019-11-02",
  "exception": null,
  "log_url": "http://localhost:8090/dags/incremental_warehouse_dag/runs/manual__.../tasks/check_env",
  "rerun_cmd": "airflow tasks run incremental_warehouse_dag check_env 2019-11-02\n# 或直接跑（绕过 Airflow）：..."
}
```

`log_tail` 为空不是 bug：该次任务被 executor 在执行前判为失败（SOCKS 代理问题），没有生成任务日志，因此取不到日志尾部。日志尾部按 `logs/dag_id=/run_id=/task_id=/attempt=N.log` 这一约定路径读取，取不到时降级为只提供 UI 链接，不影响送达。

## 五、使用方法

```bash
# 今天有没有告警（没有输出 = 一切正常）
cat <项目根目录>/scheduler/alert_records/alerts-$(date +%F).jsonl 2>/dev/null | \
  python3 -c "import sys,json; [print(json.loads(l)['ts'], json.loads(l)['level'], json.loads(l)['task_id']) for l in sys.stdin]"

# 只看失败原因
grep -o '"exception":"[^"]*"' <项目根目录>/scheduler/alert_records/*.jsonl

# 重跑：台账里的 rerun_cmd 直接可用
airflow tasks run incremental_warehouse_dag <task_id> <ds>
```

## 六、保留策略

台账按天累积（一条失败记录可能几 KB，因为带 `log_tail` 日志尾部），不清理会持续增长。规则：

| 对象 | 策略 | 环境变量 |
|---|---|---|
| 台账 `scheduler/alert_records/alerts-*.jsonl` | 只留最近 30 天 | `DW_ALERT_KEEP_DAYS` |
| Windows 通知 `数仓告警.txt` | 超过 512 KB 时截断，只保留最新一半 | `DW_ALERT_TXT_MAX_KB` |

执行时机：在 `notify()` 中顺带执行，每个进程每天最多清理一次；目录只有几十个文件，开销可忽略，不需要额外配置 cron。也可手动执行：

```bash
python scheduler/alerts.py --status    # 看现状（只读，不产生记录）
python scheduler/alerts.py --prune     # 立即执行保留策略
```

安全边界：
- 只删除文件名严格匹配 `alerts-YYYY-MM-DD.jsonl` 的文件，`.gitkeep` 及手动放置的其他文件不受影响
- 按文件名中的日期判断过期，不读取 mtime，规则确定、可审计
- 全程 `try/except`，清理失败不影响告警写入和任务状态

## 七、指标异常校验（GMV 波动）

除质量闸门外，还接入了指标异常检查（`etl/ads/ads_trade_daily.py::check_gmv_volatility`）：

- 口径：今日 GMV 对比最近 `DQC_GMV_DAYS`(7) 天中位数（不含今日）；下跌超过 `DQC_GMV_DROP_BLOCK`(50%) 时阻断，上涨或小幅下跌超过 `DQC_GMV_WARN`(30%) 只告警
- 使用 `quality_warn` 而非 `quality_block`：`ads_daily_layer` 是叶子层，没有下游可拦，报「已拦住下游」与事实不符
- 历史不足 7 天时跳过校验（月初第 1~7 天、补历史的头几天），避免误报
- 数据照常写出，不撤回。该检查用于暴露异常，不作为正确性闸门

实测的三条分支：

```
① 正常：2019-11-04  今日 8,033,899.65 vs 近 7 日均 6,586,973.18 → 偏差 22.0% < 30%  OK 通过
② 跳过：2019-10-03  历史仅 2 天（不足 7 天）→ 跳过校验                        OK 不误报
③ 阈值确实生效：同一天，阈值给 1% → FAIL 拦住；给 30% → OK 通过
```

## 八、已知局限

1. 桌面弹窗基本不会触发。依赖 BurntToast 模块，本机大概率未安装；实际生效的推送通道是 webhook。
2. `try_number` 曾为 None。回调 context 中该键常为空，已改为从 `ti.try_number` 兜底；修复后未再触发失败，现有 5 条历史记录中仍为 None。
3. 告警按任务粒度触发。一天 21 个任务失败几个就报几条，没有按天合并成摘要。
4. 指标异常目前只接入 GMV 一项。行数波动只在 ODS DQC 中校验，DWD 行数、漏斗转化率的波动尚未校验。
5. GMV 校验失败会重试一次（DAG 的 `retries=1`）。对确定性的数据检查而言这次重试没有意义，影响只是多花一分钟，暂未单独关闭。
