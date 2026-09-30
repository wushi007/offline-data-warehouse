# 任务告警机制

> 代码：`scheduler/alerts.py`（通知模块）+ `scheduler/incremental_warehouse_dag.py`（回调接线）
> 已在 **2026-09-29 的真实失败上验证生效**（见文末样例）

## 一、四条设计原则

1. **绝不抛异常** —— 所有通道逐个 best-effort、失败即静默降级。
   告警自己挂掉不能把任务状态搞乱（否则会把正常的任务标成失败）。
2. **分级推送** —— 只有「质量阻断 / 失败 / 超时」推给人；**「重试中」只进台账**。
   否则一次网络抖动重试就把真问题淹了。
3. **DQC 失败单独标注** —— `ods_dqc` / `dwd_event_fact_dqc` 的失败语义是
   **"数据质量不达标，已阻断下游"**，与"脚本崩了"分开报警。收到就知道是数据问题还是代码问题。
4. **每条告警自带行动信息** —— 重跑命令 + 日志尾部。收到就能直接动手，不用再去翻 UI。

## 二、通道（前两个默认开，其余靠环境变量启用）

| 通道 | 默认 | 依赖 | 位置 / 开关 |
|---|---|---|---|
| ① 本地台账 | ✅ 开 | 无 | `scheduler/alert_records/alerts-YYYY-MM-DD.jsonl`（结构化，可 `jq`）—— **按天一个文件，放在项目内** |
| ② Windows 告警文件 | ✅ 开 | 无 | `/mnt/c/Users/<用户名>/Documents/数仓告警.txt`（打开就能看；目录不存在时自动跳过，可用 `DW_ALERT_WIN_FILE` 覆盖） |
| ③ 桌面弹窗 | best-effort | 装了 BurntToast 才有 | 没装则**静默跳过**（刻意不用阻塞式 MessageBox） |
| ④ HTTP Webhook | 靠环境变量 | `DW_ALERT_WEBHOOK` | 企业微信/钉钉/飞书群机器人 → **手机推送，唯一真正"响"的通道** |
| ⑤ 邮件 SMTP | 靠环境变量 | `DW_ALERT_SMTP_HOST` / `_PORT` / `_USER` / `_PASS` / `_TO`（都设了才启用） | — |

**启手机推送**（拿到群机器人 URL 后，加进 `airflow/airflow-env.sh`）：
```bash
export DW_ALERT_WEBHOOK='https://qyapi.weixin.qq.com/cgi-bin/webhook/send?key=xxx'
```

## 三、事件分级与触发点

| 事件 | 级别 | 会推给人吗 | 谁触发 |
|---|---|---|---|
| `quality_block` | 🔴 数据质量阻断（已拦住下游） | ✅ | `ods_dqc`、`dwd_event_fact_dqc` 失败 |
| `quality_warn` | 🟠 数据质量异常（值超阈值，未阻断下游） | ✅ | `ads_trade`：GMV 相对近 7 日均值波动 >30% |
| `failure` | 🔴 任务失败（重试已耗尽） | ✅ | 其余所有任务（`default_args` 里也兜了一层） |
| `retry` | 🟡 任务重试中 | ❌ 只进台账 | 所有任务（`on_retry_callback`） |
| `timeout` | 🟠 任务超时（疑似卡死） | ✅ | 保留字；Airflow 超时会走 `failure` |

**超时**：每个任务设了 `execution_timeout`（`ods_import` 45 分 / `dim_session` 30 分 / `ads_user_rfm` 45 分 / 其余 20 分）。
因为 `max_active_tasks=1`，**一个卡死的任务会堵死整天**，所以这个兜底必须有。

## 四、台账里有什么字段

```
ts / event / level / dag_id / task_id / ds / run_id / try_number
reason / exception / log_url / rerun_cmd / log_tail
```

一条真实记录（2026-09-29 首次实战，就是代理那个坑）：

```json
{
  "ts": "2026-09-29 19:41:47",
  "event": "failure",
  "level": "🔴 任务失败（重试已耗尽）",
  "task_id": "check_env",
  "ds": "2019-11-02",
  "exception": null,
  "log_url": "http://localhost:8090/dags/incremental_warehouse_dag/runs/manual__.../tasks/check_env",
  "rerun_cmd": "airflow tasks run incremental_warehouse_dag check_env 2019-11-02\n# 或直接跑（绕过 Airflow）：..."
}
```

> `log_tail` 为空 **不是 bug**：那次任务是被 executor 在"执行前"判失败的（SOCKS 代理坑），
> **根本没生成任务日志** —— 所以取不到尾部。日志尾部靠 `logs/dag_id=/run_id=/task_id=/attempt=N.log`
> 这个约定路径读取，取不到就降级为只给 UI 链接（不影响送达）。

## 五、怎么用

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

台账**按天累积**（一条失败记录可能几 KB，因为带 `log_tail` 日志尾部），不清理会无限增长。规则：

| 对象 | 策略 | 环境变量 |
|---|---|---|
| 台账 `scheduler/alert_records/alerts-*.jsonl` | 只留最近 **30 天** | `DW_ALERT_KEEP_DAYS` |
| Windows 通知 `数仓告警.txt` | 超过 **512 KB** 时截断，只保留最新一半 | `DW_ALERT_TXT_MAX_KB` |

**执行时机**：`notify()` 里顺手做（**每个进程每天最多清一次**，目录就几十个文件，开销可忽略）——
不需要额外配 cron。也可手动执行：

```bash
python scheduler/alerts.py --status    # 看现状（只读，不产生记录）
python scheduler/alerts.py --prune     # 立即执行保留策略
```

**安全边界**（这块是刻意收紧的）：
- **只删**文件名严格匹配 `alerts-YYYY-MM-DD.jsonl` 的 → `.gitkeep`、你手放的其他文件一概不动
- 按**文件名里的日期**判断过期，不看 mtime → 规则确定、可审计
- 全程 `try/except` → 清理失败绝不影响写告警和任务状态

## 七、指标异常校验（GMV 波动）

除"闸门"之外，还接了一个**指标异常**检查（`etl/ads/ads_trade_daily.py::check_gmv_volatility`）：

- **口径**：今日 GMV 对比**最近 `DQC_GMV_DAYS`(7) 天均值**（不含今日），偏差超 **`DQC_GMV_VOLATILITY`(30%)** 就非 0 退出
- **为什么用 `quality_warn` 而不是 `quality_block`**：`ads_trade` 是叶子节点，**没有下游可拦** ——
  报"已拦住下游"会与事实不符，收多了就没人信告警了
- **历史不足 7 天就跳过**（月初第 1~7 天、补历史的头几天）：判不了就别误报
- 数据照写、不撤回 —— 它是把异常**暴露**出来，不是正确性闸门

实测三条分支：

```
① 正常：2019-11-04  今日 8,033,899.65 vs 近 7 日均 6,586,973.18 → 偏差 22.0% < 30%  ✅ 通过
② 跳过：2019-10-03  历史仅 2 天（不足 7 天）→ 跳过校验                        ✅ 不误报
③ 阈值确实生效：同一天，阈值给 1% → ❌ 拦住；给 30% → ✅ 通过
```

## 八、已知局限

1. **桌面弹窗基本不会响** —— 依赖 BurntToast 模块，本机大概率没装；真正"响"的是 webhook。
2. **`try_number` 曾为 None** —— 回调的 context 里该键常为空，已改成从 `ti.try_number` 兜底；
   但修复之后还没再触发过失败，所以现有 5 条历史记录里仍是 None。
3. **告警按任务粒度** —— 一天 21 个任务失败几个就报几条，没有做"同一天合并成一条摘要"。
4. **指标异常只接了 GMV 一个** —— 行数波动只在 ODS DQC 里有；DWD 行数、漏斗转化率的波动还没有校验。
5. **GMV 校验失败会重试一次**（DAG 的 `retries=1`）—— 对确定性的数据检查来说这次重试是浪费，
   但影响只是多花一分钟，暂未单独关掉。
