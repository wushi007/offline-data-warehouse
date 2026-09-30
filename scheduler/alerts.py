# -*- coding: utf-8 -*-
"""
alerts.py — 任务告警通知（被 DAG 的 on_failure_callback / on_retry_callback 调用）
==================================================================================
设计原则
--------
1. **绝不抛异常**：告警本身出问题不能把任务状态搞乱，所有通道逐个 best-effort、失败即静默降级。
2. **分级推送**：只有「质量阻断 / 失败 / 超时」推给人；「重试中」只进台账（避免重试噪音淹没真问题）。
3. **每条告警都自带行动信息**：重跑命令 + 日志尾部 —— 收到就能直接动手，不用再去翻 UI。

通道（前两个默认开，后两个靠环境变量启用）
------------------------------------------
① 本地台账       {AIRFLOW_HOME}/alerts/alerts-YYYY-MM-DD.jsonl   （结构化，可 grep/jq）
② Windows 告警文件 /mnt/c/Users/lst/Documents/数仓告警.txt         （WSL 里能写到 Windows，打开就能看）
③ 桌面弹窗       best-effort（装了 BurntToast 才有 toast；否则跳过。刻意不用阻塞式 MessageBox）
④ HTTP Webhook   DW_ALERT_WEBHOOK=<企业微信/钉钉/飞书 机器人 URL>
⑤ 邮件 SMTP      DW_ALERT_SMTP_HOST / _PORT / _USER / _PASS / _TO（都设了才启用）
"""

import argparse
import json
import os
import re
import subprocess
import urllib.request
from datetime import datetime, timedelta
from pathlib import Path

AIRFLOW_HOME = Path(os.environ.get("AIRFLOW_HOME", "/home/lst/my-spark/airflow"))
# 告警记录放在**项目内**（scheduler/alert_records/），不放 AIRFLOW_HOME：
#   ① 跟代码同仓，一起备份/迁移，不随 Airflow 重装丢失；
#   ② 目录名特意避开 `alerts`——本模块就叫 alerts.py，同名目录会和它撞（Python 导入歧义）。
ALERT_DIR = Path(__file__).resolve().parent / "alert_records"
LOG_DIR = AIRFLOW_HOME / "logs"
WIN_ALERT_FILE = Path("/mnt/c/Users/lst/Documents/数仓告警.txt")
PROJECT_ROOT = Path(__file__).resolve().parent.parent
LOCK_WRAPPER = str(PROJECT_ROOT / "etl" / "run_locked.sh")

LOG_TAIL_LINES = 15

# ---------------- 保留策略 ----------------
# 台账按天累积（一条失败记录可能几 KB，因为带日志尾部），不清理会无限长。
# 规则：台账只留最近 KEEP_DAYS 天；Windows 通知文件超过 WIN_MAX_KB 就截断保留最新部分。
# 都可用环境变量覆盖：DW_ALERT_KEEP_DAYS / DW_ALERT_TXT_MAX_KB
KEEP_DAYS = int(os.environ.get("DW_ALERT_KEEP_DAYS", "30"))
WIN_MAX_KB = int(os.environ.get("DW_ALERT_TXT_MAX_KB", "512"))
_LEDGER_RE = re.compile(r"alerts-(\d{4}-\d{2}-\d{2})\.jsonl$")
_pruned_on = None          # 本进程内已执行过清理的日期，避免每条告警都扫目录

# 事件分级：只有这几个会"推给人"
EVENTS = {
    "quality_block": "🔴 数据质量阻断（已拦住下游）",
    "quality_warn":  "🟠 数据质量异常（值超阈值，未阻断下游）",
    "failure":       "🔴 任务失败（重试已耗尽）",
    "timeout":       "🟠 任务超时（疑似卡死）",
    "retry":         "🟡 任务重试中",
}
# quality_warn 与 quality_block 的区别：前者是叶子节点上的**指标异常**（如 GMV 波动超阈值），
# 数据已经写出来了、也没有下游被拦；后者是闸门任务不通过、下游确实没跑。
# 分开是为了让收到告警的人能判断"要不要立刻动手"。
PUSH_EVENTS = {"quality_block", "quality_warn", "failure", "timeout"}


# ---------------- 上下文提取 ----------------
def _log_tail(context, keep=LOG_TAIL_LINES):
    """按 Airflow 日志布局取任务日志尾部：logs/dag_id=/run_id=/task_id=/attempt=N.log。

    布局是约定（随版本可能变），所以取不到就返回 None —— 告警降级为只给 UI 链接，不影响送达。
    """
    try:
        ti = context.get("ti") or context.get("task_instance")
        dag_id = context["dag"].dag_id
        run_id = context["run_id"]
        task_id = context["task"].task_id
        attempt = context.get("try_number") or getattr(ti, "try_number", 1)
        p = LOG_DIR / f"dag_id={dag_id}" / f"run_id={run_id}" / f"task_id={task_id}" / f"attempt={attempt}.log"
        if not p.exists():                      # 退一步：扫该 task 目录下最新的 attempt
            cands = sorted((LOG_DIR / f"dag_id={dag_id}" / f"run_id={run_id}"
                            / f"task_id={task_id}").glob("attempt=*.log"))
            if not cands:
                return None
            p = cands[-1]
        lines = p.read_text(errors="replace").splitlines()
        return "\n".join(lines[-keep:])
    except Exception:
        return None


def _rerun_cmd(context):
    """给出两条重跑路径：Airflow 单任务重跑 / 直接跑底层脚本。"""
    try:
        dag_id = context["dag"].dag_id
        task_id = context["task"].task_id
        ds = context.get("ds") or "YYYY-MM-DD"
        raw = (context["task"].bash_command or "").replace(LOCK_WRAPPER + " ", "").strip()
        return (f"airflow tasks run {dag_id} {task_id} {ds}\n"
                f"# 或直接跑（绕过 Airflow）：\n{raw}")
    except Exception:
        return "(取重跑命令失败，去 UI 重跑该 task)"


def _build_record(context, event, extra):
    try:
        ti = context.get("ti") or context.get("task_instance")
        exc = context.get("exception")
        rec = {
            "ts": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "event": event,
            "level": EVENTS.get(event, event),
            "dag_id": context["dag"].dag_id,
            "task_id": context["task"].task_id,
            "ds": context.get("ds"),
            "run_id": context.get("run_id"),
            # 回调的 context 里 try_number 常为 None（键存在但没填），从 ti 兜底取
            "try_number": context.get("try_number") or getattr(ti, "try_number", None),
            "reason": context.get("reason"),
            "exception": f"{type(exc).__name__}: {exc}" if exc else None,
            "log_url": getattr(ti, "log_url", None),
            "rerun_cmd": _rerun_cmd(context),
        }
    except Exception as e:
        rec = {"ts": datetime.now().strftime("%Y-%m-%d %H:%M:%S"), "event": event,
               "error": f"构建告警记录失败: {e}"}
    if extra:
        rec.update(extra)
    rec["log_tail"] = _log_tail(context)
    return rec


# ---------------- 通道 ----------------
def _write_ledger(rec):
    ALERT_DIR.mkdir(parents=True, exist_ok=True)
    f = ALERT_DIR / f"alerts-{datetime.now().strftime('%Y-%m-%d')}.jsonl"
    with f.open("a", encoding="utf-8") as fh:
        fh.write(json.dumps(rec, ensure_ascii=False) + "\n")


def _human(rec):
    """给人看的单条文本（Windows 文件 / webhook / 邮件共用）。"""
    lines = [
        f"[{rec.get('ts')}] {rec.get('level')}",
        f"  任务   : {rec.get('dag_id')} / {rec.get('task_id')}",
        f"  数据日 : {rec.get('ds')}    重试次数: {rec.get('try_number')}",
    ]
    if rec.get("exception"):
        lines.append(f"  异常   : {rec['exception']}")
    if rec.get("reason"):
        lines.append(f"  原因   : {rec['reason']}")
    lines.append(f"  重跑   : {rec.get('rerun_cmd', '').splitlines()[0]}")
    if rec.get("log_tail"):
        tail = "\n".join("    " + ln for ln in rec["log_tail"].splitlines()[-8:])
        lines.append("  日志尾部:\n" + tail)
    return "\n".join(lines)


def _windows_file(rec):
    if not WIN_ALERT_FILE.parent.exists():
        return
    with WIN_ALERT_FILE.open("a", encoding="utf-8") as fh:
        fh.write(_human(rec) + "\n" + "-" * 70 + "\n")


def _windows_toast(rec):
    """best-effort 桌面弹窗：装了 BurntToast 才有；没有就跳过（绝不用阻塞式 MessageBox）。"""
    title = f"{rec.get('level')} | {rec.get('task_id')}"
    body = f"{rec.get('ds')}  {rec.get('exception') or rec.get('reason') or ''}"
    ps = (f"if (Get-Module -ListAvailable -Name BurntToast) {{"
          f" Import-Module BurntToast;"
          f" New-BurntToastNotification -Text '{title}','{body}' }}")
    try:
        subprocess.run(["powershell.exe", "-NoProfile", "-Command", ps],
                       capture_output=True, timeout=15)
    except Exception:
        pass


def _webhook(rec):
    url = os.environ.get("DW_ALERT_WEBHOOK")
    if not url:
        return
    # 企业微信/钉钉/飞书机器人都是 POST JSON；这里用最通用的 text 结构（三家都认 content/text）
    payload = {"msgtype": "text", "text": {"content": _human(rec)}}
    try:
        req = urllib.request.Request(
            url, data=json.dumps(payload).encode("utf-8"),
            headers={"Content-Type": "application/json"})
        urllib.request.urlopen(req, timeout=10).read()
    except Exception:
        pass


def _email(rec):
    host = os.environ.get("DW_ALERT_SMTP_HOST")
    to = os.environ.get("DW_ALERT_SMTP_TO")
    if not (host and to):
        return
    try:
        import smtplib
        from email.mime.text import MIMEText
        from email.header import Header
        port = int(os.environ.get("DW_ALERT_SMTP_PORT", "465"))
        user = os.environ.get("DW_ALERT_SMTP_USER", "")
        pwd = os.environ.get("DW_ALERT_SMTP_PASS", "")
        msg = MIMEText(_human(rec), "plain", "utf-8")
        msg["Subject"] = Header(f"[数仓告警] {rec.get('level')} {rec.get('task_id')} @{rec.get('ds')}", "utf-8")
        msg["From"], msg["To"] = user, to
        s = smtplib.SMTP_SSL(host, port, timeout=15)
        if user:
            s.login(user, pwd)
        s.sendmail(user, [a.strip() for a in to.split(",")], msg.as_string())
        s.quit()
    except Exception:
        pass


# ---------------- 保留策略 ----------------
def prune(verbose=True):
    """按保留策略清理：台账只留最近 KEEP_DAYS 天，Windows 文件超限则截断。

    安全边界：
      · **只删**文件名严格匹配 `alerts-YYYY-MM-DD.jsonl` 的（`.gitkeep`、别的文件一概不动）；
      · 按**文件名里的日期**判断过期，不看 mtime —— 规则确定、可审计；
      · 全程 try/except，清理失败绝不影响写告警和任务状态。
    """
    removed = []
    cutoff = datetime.now().date() - timedelta(days=KEEP_DAYS)

    try:
        for p in ALERT_DIR.glob("alerts-*.jsonl"):
            m = _LEDGER_RE.search(p.name)
            if not m:
                continue
            try:
                d = datetime.strptime(m.group(1), "%Y-%m-%d").date()
            except ValueError:
                continue
            if d < cutoff:
                p.unlink()
                removed.append(p.name)
    except Exception:
        pass

    try:
        if WIN_ALERT_FILE.exists() and WIN_ALERT_FILE.stat().st_size > WIN_MAX_KB * 1024:
            sep = "-" * 70
            blocks = WIN_ALERT_FILE.read_text(errors="replace").split(sep)
            keep = blocks[-(max(1, len(blocks) // 2)):]      # 只留最新的一半
            WIN_ALERT_FILE.write_text(sep.join(keep), encoding="utf-8")
            removed.append(f"{WIN_ALERT_FILE.name}(超 {WIN_MAX_KB}KB 截断保留最新部分)")
    except Exception:
        pass

    if verbose:
        if removed:
            print(f"🧹 告警保留策略（保留 {KEEP_DAYS} 天）：清理 {len(removed)} 项")
            for r in removed:
                print(f"   - {r}")
        else:
            print(f"🧹 告警保留策略（保留 {KEEP_DAYS} 天）：无需清理")
    return removed


def _prune_once_a_day():
    """在 notify 里顺手触发：每个进程每天最多清一次（目录就几十个文件，开销可忽略）。"""
    global _pruned_on
    today = datetime.now().date()
    if _pruned_on != today:
        _pruned_on = today
        prune(verbose=False)


def status():
    """看台账现状（只读，不产生记录）。"""
    files = sorted(ALERT_DIR.glob("alerts-*.jsonl")) if ALERT_DIR.exists() else []
    print(f"  台账目录 : {ALERT_DIR}")
    print(f"  存在     : {ALERT_DIR.exists()}")
    print(f"  保留策略 : 最近 {KEEP_DAYS} 天（DW_ALERT_KEEP_DAYS 可覆盖）")
    print(f"  文件数   : {len(files)}")
    if files:
        print(f"  范围     : {files[0].name} ~ {files[-1].name}")
        total = sum(f.stat().st_size for f in files)
        print(f"  总体积   : {total / 1024:.1f} KB")
    if WIN_ALERT_FILE.exists():
        print(f"  Windows  : {WIN_ALERT_FILE}  {WIN_ALERT_FILE.stat().st_size / 1024:.1f} KB"
              f"（超 {WIN_MAX_KB}KB 自动截断）")
    else:
        print(f"  Windows  : {WIN_ALERT_FILE}  （还没生成）")


# ---------------- 对外入口 ----------------
def notify(context, event="failure", extra=None):
    """DAG 回调入口。任何通道失败都不影响主流程。"""
    _prune_once_a_day()
    try:
        rec = _build_record(context, event, extra)
    except Exception:
        return
    for ch in (_write_ledger, _windows_file):
        try:
            ch(rec)
        except Exception:
            pass
    if event not in PUSH_EVENTS:
        return
    for ch in (_windows_toast, _webhook, _email):
        try:
            ch(rec)
        except Exception:
            pass


def make_callback(event="failure", extra=None):
    """给 DAG 用：把事件类型绑成一个 on_xxx_callback。"""
    def _cb(context):
        notify(context, event, extra)
    _cb.__name__ = f"alert_{event}"
    return _cb


if __name__ == "__main__":
    ap = argparse.ArgumentParser(description="告警模块工具（默认只读，不会产生记录）")
    ap.add_argument("--status", action="store_true", help="看台账现状（路径/文件数/体积/保留策略）")
    ap.add_argument("--prune", action="store_true", help="立即执行保留策略（清理过期台账）")
    ap.add_argument("--selftest", action="store_true",
                    help="⚠️ 发一条假告警验证各通道 —— **会往台账里写一条测试记录**，事后需手动删")
    args = ap.parse_args()

    if args.prune:
        prune(verbose=True)
    elif args.selftest:
        class _T:
            task_id = "dwd_event_fact"
            bash_command = (f"{LOCK_WRAPPER} $PROJECT_ROOT/.venv/bin/python "
                            "etl/dwd/dwd_event_fact.py --dt 2019-11-01")
        class _D:
            dag_id = "incremental_warehouse_dag"
        notify({"dag": _D(), "task": _T(), "ds": "2019-11-01", "run_id": "manual__selftest",
                "try_number": 1, "exception": RuntimeError("selftest 假异常"),
                "reason": None}, "failure")
        print(f"✅ 自测完成，测试记录写在 {ALERT_DIR}/alerts-*.jsonl（记得删）")
        print(f"   推送通道：webhook={'已配置' if os.environ.get('DW_ALERT_WEBHOOK') else '未配置（跳过）'}"
              f"  邮件={'已配置' if os.environ.get('DW_ALERT_SMTP_HOST') else '未配置（跳过）'}")
    else:
        status()
