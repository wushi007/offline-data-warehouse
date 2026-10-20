# -*- coding: utf-8 -*-
"""任务告警通知，供 DAG 的 on_failure_callback / on_retry_callback 调用。

通道：本地 jsonl 台账、Windows 告警文件、桌面 toast、HTTP webhook、
飞书机器人（卡片/文本）、SMTP 邮件；除前两项外均由环境变量启用。
各通道失败静默降级，告警异常不影响任务状态；retry 事件只进台账不推送。
"""

import argparse
import json
import os
import re
import subprocess
import sys
import urllib.request
from datetime import datetime, timedelta
from pathlib import Path

# 加载项目根目录 .env（独立运行时也能读到告警配置）
_ROOT = Path(__file__).resolve().parent.parent
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))
try:
    from config.env import load_env

    load_env()
except ImportError:
    pass

AIRFLOW_HOME = Path(os.path.expanduser(os.environ.get("AIRFLOW_HOME", "~/airflow")))
# 台账放项目内，随代码一起迁移；目录名避开 alerts，防止与 alerts.py 的导入冲突。
ALERT_DIR = Path(__file__).resolve().parent / "alert_records"
LOG_DIR = AIRFLOW_HOME / "logs"
# WSL 专用通道，目录不存在时跳过；可用 DW_ALERT_WIN_FILE 覆盖路径。
_DEFAULT_WIN_FILE = "/mnt/c/Users/{}/Documents/数仓告警.txt".format(os.environ.get("USER", "user"))
WIN_ALERT_FILE = Path(os.environ.get("DW_ALERT_WIN_FILE", _DEFAULT_WIN_FILE))
PROJECT_ROOT = Path(__file__).resolve().parent.parent
LOCK_WRAPPER = str(PROJECT_ROOT / "etl" / "run_locked.sh")

LOG_TAIL_LINES = 15

# ---------------- 保留策略 ----------------
# 台账保留 KEEP_DAYS 天，Windows 文件超过 WIN_MAX_KB 截断；环境变量可覆盖。
KEEP_DAYS = int(os.environ.get("DW_ALERT_KEEP_DAYS", "30"))
WIN_MAX_KB = int(os.environ.get("DW_ALERT_TXT_MAX_KB", "512"))
_LEDGER_RE = re.compile(r"alerts-(\d{4}-\d{2}-\d{2})\.jsonl$")
_pruned_on = None          # 本进程已清理的日期，避免每条告警都扫目录

# 事件分级：只有 PUSH_EVENTS 中的事件推送给人
EVENTS = {
    "quality_block": "数据质量阻断（已拦住下游）",
    "quality_warn":  "数据质量异常（值超阈值，未阻断下游）",
    "failure":       "任务失败（重试已耗尽）",
    "timeout":       "任务超时（疑似卡死）",
    "retry":         "任务重试中",
}
# quality_warn 为叶子节点指标异常（数据已写出、无下游被拦）；quality_block 为闸门失败、下游未跑。
PUSH_EVENTS = {"quality_block", "quality_warn", "failure", "timeout"}

def _log_tail(context, keep=LOG_TAIL_LINES):
    """取任务日志尾部（logs/dag_id=/run_id=/task_id=/attempt=N.log），取不到返回 None。"""
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
            # 回调 context 的 try_number 常为 None，从 ti 兜底取
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
    """桌面弹窗，best-effort：未安装 BurntToast 时跳过，不使用阻塞式 MessageBox。"""
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
    # 企业微信 / 钉钉格式；飞书格式不同，其地址应填 DW_ALERT_FEISHU_WEBHOOK。
    payload = {"msgtype": "text", "text": {"content": _human(rec)}}
    try:
        req = urllib.request.Request(
            url, data=json.dumps(payload).encode("utf-8"),
            headers={"Content-Type": "application/json"})
        urllib.request.urlopen(req, timeout=10).read()
    except Exception:
        pass


# ---------------- 飞书（Feishu / Lark）自定义机器人 ----------------
# 格式为 {"msg_type":"text","content":{"text":...}}；业务失败仍返回 HTTP 200，须检查 body。
# 配置项写在 airflow_env.sh 的 gen_env_file；STYLE 取 card（默认）或 text。
import base64
import hashlib
import hmac
import time

# 卡片头部配色：红=失败/阻断，橙=异常/超时，灰=信息
FEISHU_TEMPLATE = {
    "quality_block": "red",
    "failure":       "red",
    "quality_warn":  "orange",
    "timeout":       "orange",
    "retry":         "grey",
}


def _feishu_conf():
    return (
        os.environ.get("DW_ALERT_FEISHU_WEBHOOK", "").strip(),
        os.environ.get("DW_ALERT_FEISHU_SECRET", "").strip(),
        (os.environ.get("DW_ALERT_FEISHU_STYLE", "card") or "card").strip().lower(),
    )


def gen_sign(timestamp, secret):
    """飞书加签：以 f"{timestamp}\\n{secret}" 为 HMAC key，对空串求 HmacSHA256 后 Base64。"""
    string_to_sign = f"{timestamp}\n{secret}"
    hmac_code = hmac.new(string_to_sign.encode("utf-8"), digestmod=hashlib.sha256).digest()
    return base64.b64encode(hmac_code).decode("utf-8")


def _feishu_post(url, payload, timeout=10):
    """POST 并检查响应 body 的 code：飞书业务失败时同样返回 HTTP 200。"""
    data = json.dumps(payload, ensure_ascii=False).encode("utf-8")
    req = urllib.request.Request(
        url, data=data, headers={"Content-Type": "application/json; charset=utf-8"})
    with urllib.request.urlopen(req, timeout=timeout) as resp:
        body = json.loads(resp.read().decode("utf-8") or "{}")
    code = body.get("code", body.get("StatusCode", -1))
    return code == 0, f"code={code} msg={body.get('msg') or body.get('StatusMessage') or ''}"


def _feishu_text(rec):
    """纯文本消息（卡片失败时的兜底）。首行含「数仓告警」以匹配机器人关键词。"""
    lines = [f"【数仓告警】{rec.get('level', rec.get('event', '?'))}",
             f"任务   : {rec.get('dag_id', '-')} / {rec.get('task_id', '-')}",
             f"数据日 : {rec.get('ds') or '-'}    重试次数: {rec.get('try_number')}"]
    if rec.get("exception"):
        lines.append(f"异常   : {rec['exception']}")
    if rec.get("reason"):
        lines.append(f"原因   : {rec['reason']}")
    cmd = (rec.get("rerun_cmd") or "").splitlines()
    if cmd:
        lines.append(f"重跑   : {cmd[0]}")
    if rec.get("log_url"):
        lines.append(f"日志   : {rec['log_url']}")
    if rec.get("log_tail"):
        tail = "\n".join("    " + ln for ln in rec["log_tail"].splitlines()[-6:])
        lines.append("日志尾部:\n" + tail)
    return {"msg_type": "text", "content": {"text": "\n".join(lines)}}


def _feishu_card(rec):
    """交互式卡片：头部按事件着色，附「打开 Airflow 日志」按钮。"""
    title = f"数仓告警 | {rec.get('level', rec.get('event', ''))} {rec.get('task_id', '')}"
    md = [f"**任务**：{rec.get('dag_id', '-')} / {rec.get('task_id', '-')}",
          f"**数据日**：{rec.get('ds') or '-'}　**重试**：{rec.get('try_number')}"]
    if rec.get("exception"):
        md.append(f"**异常**：{rec['exception']}")
    if rec.get("reason"):
        md.append(f"**原因**：{rec['reason']}")
    cmd = (rec.get("rerun_cmd") or "").splitlines()
    if cmd:
        md.append(f"**重跑**：`{cmd[0][:180]}`")

    elements = [{"tag": "div", "text": {"tag": "lark_md", "content": "\n".join(md)}}]
    if rec.get("log_tail"):
        tail = "\n".join(rec["log_tail"].splitlines()[-8:])
        elements += [{"tag": "hr"},
                     {"tag": "div",
                      "text": {"tag": "plain_text", "content": ("日志尾部：\n" + tail)[:1500]}}]
    if rec.get("log_url"):
        elements.append({"tag": "action", "actions": [
            {"tag": "button", "type": "primary",
             "text": {"tag": "plain_text", "content": "打开 Airflow 日志"},
             "url": rec["log_url"]}]})

    return {"msg_type": "interactive",
            "card": {"config": {"wide_screen_mode": True},
                     "header": {"title": {"tag": "plain_text", "content": title},
                                "template": FEISHU_TEMPLATE.get(rec.get("event"), "orange")},
                     "elements": elements}}


def _feishu(rec):
    """飞书通道，返回 (ok, detail)，不抛异常；未配置 webhook 时跳过。"""
    url, secret, style = _feishu_conf()
    if not url:
        return False, "未配置 DW_ALERT_FEISHU_WEBHOOK（跳过）"

    payloads = [_feishu_text(rec)] if style == "text" else [_feishu_card(rec), _feishu_text(rec)]
    detail = "未发送"
    for i, p in enumerate(payloads):
        try:
            if secret:
                ts = str(int(time.time()))          # 秒级时间戳，不是毫秒
                payload = {"timestamp": ts, "sign": gen_sign(ts, secret), **p}
            else:
                payload = p
            ok, detail = _feishu_post(url, payload)
        except Exception as e:                      # 网络异常同样降级，不往外抛
            ok, detail = False, f"{type(e).__name__}: {e}"
        if ok:
            return True, f"{detail}（第 {i + 1} 次尝试：{p['msg_type']}）"
    return False, detail


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

def prune(verbose=True):
    """清理过期台账并截断超限的 Windows 文件；只处理 alerts-YYYY-MM-DD.jsonl，按文件名日期判断过期。"""
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
    """由 notify 触发，每个进程每天最多清理一次。"""
    global _pruned_on
    today = datetime.now().date()
    if _pruned_on != today:
        _pruned_on = today
        prune(verbose=False)


def status():
    """查看台账现状（只读）。"""
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
    for ch in (_windows_toast, _webhook, _feishu, _email):
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
                    help="WARN: 发一条假告警验证各通道 —— **会往台账里写一条测试记录**，事后需手动删")
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
        print(f"OK: 自测完成，测试记录写在 {ALERT_DIR}/alerts-*.jsonl（记得删）")
        print(f"   推送通道：webhook={'已配置' if os.environ.get('DW_ALERT_WEBHOOK') else '未配置（跳过）'}"
              f"  飞书={'已配置' if os.environ.get('DW_ALERT_FEISHU_WEBHOOK') else '未配置（跳过）'}"
              f"  邮件={'已配置' if os.environ.get('DW_ALERT_SMTP_HOST') else '未配置（跳过）'}")
    else:
        status()
