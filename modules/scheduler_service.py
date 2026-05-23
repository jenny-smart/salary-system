"""
modules/scheduler_service.py
期別資料夾與檔案排程服務  v2026-05

執行環境：
  - GitHub Actions（推薦）：credentials 從環境變數讀取
  - 本機測試：credentials 從 Streamlit secrets / token.json 讀取

設計原則：
  - 地區設定、排程設定統一從 config.yaml 讀取
  - 排程邏輯與手動按鈕完全一致：create_period() → record_batch()
  - email 通知用 Gmail API（OAuth），不需要 SMTP 設定

CLI 用法：
  --run-once  單次檢查，符合排程日才執行（GitHub Actions 用）
  --force     忽略排程日，立刻執行（測試用）
  --daemon    常駐執行，每 30 秒檢查一次（本機 launchd 用）
"""

from __future__ import annotations

import argparse
import os
import sys
import time
import traceback
from datetime import datetime
from pathlib import Path

try:
    from zoneinfo import ZoneInfo
except ImportError:
    ZoneInfo = None

import yaml

DEFAULT_TZ       = "Asia/Taipei"
DEFAULT_LOG_PATH = Path("logs/scheduler.log")
LOCK_PATH        = Path(".period_scheduler.lock")
CONFIG_PATH      = Path("config.yaml")


# ═══════════════════════════════════════════════════════════
# 工具
# ═══════════════════════════════════════════════════════════

def _now(tz_name: str = DEFAULT_TZ) -> datetime:
    if ZoneInfo is not None:
        return datetime.now(ZoneInfo(tz_name))
    return datetime.now()


def _write_log(path: Path, msg: str, tz_name: str = DEFAULT_TZ) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    ts = _now(tz_name).strftime("%Y-%m-%d %H:%M:%S")
    line = f"[{ts}] {msg}"
    with path.open("a", encoding="utf-8") as f:
        f.write(line + "\n")
    print(line, flush=True)   # GitHub Actions log 同步顯示


# ═══════════════════════════════════════════════════════════
# Credentials（優先從環境變數，其次走原本的 modules.auth）
# ═══════════════════════════════════════════════════════════

def _build_credentials():
    """
    GitHub Actions 執行時，credentials 來自環境變數：
      OAUTH_CLIENT_ID / OAUTH_CLIENT_SECRET / OAUTH_REFRESH_TOKEN

    本機執行時，走原本的 modules.auth.get_credentials()。
    """
    client_id     = os.environ.get("OAUTH_CLIENT_ID", "").strip()
    client_secret = os.environ.get("OAUTH_CLIENT_SECRET", "").strip()
    refresh_token = os.environ.get("OAUTH_REFRESH_TOKEN", "").strip()

    if client_id and client_secret and refresh_token:
        from google.oauth2.credentials import Credentials
        from google.auth.transport.requests import Request

        creds = Credentials(
            token         = None,
            refresh_token = refresh_token,
            client_id     = client_id,
            client_secret = client_secret,
            token_uri     = "https://oauth2.googleapis.com/token",
            scopes        = [
                "https://www.googleapis.com/auth/drive",
                "https://www.googleapis.com/auth/spreadsheets",
                "https://www.googleapis.com/auth/gmail.send",
            ],
        )
        creds.refresh(Request())
        return creds

    # 本機：走原本的 auth 模組
    from modules.auth import get_credentials
    return get_credentials()


# ═══════════════════════════════════════════════════════════
# 設定讀取
# ═══════════════════════════════════════════════════════════

def load_config(path: Path = CONFIG_PATH) -> dict:
    """
    讀取 config.yaml。
    schedule 區塊結構：
      enabled: true
      days: [10, 25]
      time: "05:30"
      timezone: "Asia/Taipei"
      all_regions: true
    notify_email: "you@example.com"
    """
    try:
        with open(path, "r", encoding="utf-8") as f:
            cfg = yaml.safe_load(f) or {}
    except FileNotFoundError:
        cfg = {}
    cfg.setdefault("regions",  [])
    cfg.setdefault("schedule", {})
    return cfg


# ═══════════════════════════════════════════════════════════
# 排程判斷
# ═══════════════════════════════════════════════════════════

def should_run_now(cfg: dict, now_dt: datetime | None = None) -> tuple[bool, str]:
    sched   = cfg.get("schedule", {})
    tz_name = sched.get("timezone", DEFAULT_TZ)
    now_dt  = now_dt or _now(tz_name)
    hhmm    = now_dt.strftime("%H:%M")
    run_key = f"{now_dt.strftime('%Y-%m-%d')} {sched.get('time', '05:30')}"

    if not sched.get("enabled", False):
        return False, run_key

    days = sched.get("days", [])
    if isinstance(days, str):
        days = [int(d.strip()) for d in days.split(",") if d.strip()]
    days = [int(d) for d in days]

    if now_dt.day not in days:
        return False, run_key

    # GitHub Actions：只比對小時（cron 不保證精確到分鐘）
    cfg_hh = str(sched.get("time", "05:30")).strip()[:2]
    if hhmm[:2] != cfg_hh:
        return False, run_key

    return True, run_key


def _acquire_lock(run_key: str) -> bool:
    if LOCK_PATH.exists():
        if LOCK_PATH.read_text(encoding="utf-8").strip() == run_key:
            return False
    LOCK_PATH.write_text(run_key, encoding="utf-8")
    return True


def _calc_period(now_dt: datetime) -> str:
    try:
        from modules.period_utils import get_auto_period
        return get_auto_period()
    except Exception:
        suffix = "上" if now_dt.day <= 15 else "下"
        return f"{now_dt.year}{now_dt.month:02d}{suffix}"


# ═══════════════════════════════════════════════════════════
# 核心執行：create_period + 打卡
# ═══════════════════════════════════════════════════════════

def _run_region(region: dict, period: str, log_fn, creds) -> bool:
    name    = region.get("name", "未知")
    root_id = region.get("root_folder_id", "")

    if not root_id:
        log_fn(f"⚠️ 【{name}】root_folder_id 未設定，略過")
        return False

    def _log(msg):
        log_fn(f"  [{name}] {msg}")

    try:
        from modules.payment_reconciliation import create_period
        result    = create_period(root_id, period, name, _log)
        copied    = result.get("copied", 0)
        file_ids  = result.get("fileIds", {})
        folder_id = result.get("folderId")
        _log(f"✅ 建立完成，複製 {copied} 個檔案")

        try:
            from modules.master_sheet import record_batch
            record_batch(name, period, [
                {"task_key": "排程期別資料夾",   "count": folder_id},
                {"task_key": "排程期別金流對帳", "count": file_ids.get("金流對帳")},
                {"task_key": "排程期別清潔承攬", "count": file_ids.get("清潔承攬")},
                {"task_key": "排程期別其他承攬", "count": file_ids.get("其他承攬")},
                {"task_key": "排程期別元大帳戶", "count": file_ids.get("元大帳戶")},
            ])
            _log("🔵 打卡完成")
        except Exception as e:
            _log(f"⚠️ 打卡失敗：{e}")

        return True

    except Exception as e:
        _log(f"❌ 失敗：{e}\n{traceback.format_exc()}")
        return False


def _execute(cfg: dict, period: str, log_path: Path) -> dict:
    sched    = cfg.get("schedule", {})
    all_flag = sched.get("all_regions", True)
    regions  = cfg.get("regions", []) if all_flag else []

    if not regions:
        raise RuntimeError("沒有可執行的地區，請確認 config.yaml regions 已設定")

    creds   = _build_credentials()
    results = {}

    _write_log(log_path, f"═══ 開始執行：period={period}，地區數={len(regions)} ═══")

    for region in regions:
        name = region.get("name", "未知")
        logs = []

        def _log(msg, _logs=logs):
            _logs.append(msg)
            _write_log(log_path, msg)

        ok = _run_region(region, period, _log, creds)
        results[name] = {"ok": ok, "logs": logs}

    return results


# ═══════════════════════════════════════════════════════════
# email 通知（Gmail API，不需要 SMTP）
# ═══════════════════════════════════════════════════════════

def _send_notify(cfg: dict, period: str, results: dict, log_path: Path):
    # 收件人：優先從環境變數（GitHub Secrets），其次 config.yaml
    notify_email = (
        os.environ.get("NOTIFY_EMAIL", "").strip()
        or cfg.get("notify_email", "").strip()
    )
    if not notify_email:
        _write_log(log_path, "notify_email 未設定，略過寄信")
        return

    ok_list   = [n for n, r in results.items() if r["ok"]]
    fail_list = [n for n, r in results.items() if not r["ok"]]
    all_logs  = [l for r in results.values() for l in r["logs"]]

    subject = (
        f"⚠️ [{period}] 排程部分失敗：{', '.join(fail_list)}"
        if fail_list else
        f"✅ [{period}] 排程完成：{', '.join(ok_list)}"
    )

    tz_name = cfg.get("schedule", {}).get("timezone", DEFAULT_TZ)
    now_str = _now(tz_name).strftime("%Y-%m-%d %H:%M:%S")
    body    = "\n".join([
        "Lemon Clean 排程通知",
        f"執行時間：{now_str}",
        f"期別：{period}",
        "",
        f"✅ 成功：{', '.join(ok_list) or '無'}",
        f"❌ 失敗：{', '.join(fail_list) or '無'}",
        "",
        "── 執行日誌 ──────────────────────────",
    ] + (all_logs or ["（無日誌）"]))

    try:
        import base64
        from email.mime.text import MIMEText
        import googleapiclient.discovery

        creds = _build_credentials()
        svc   = googleapiclient.discovery.build(
            "gmail", "v1", credentials=creds, cache_discovery=False
        )
        msg = MIMEText(body, "plain", "utf-8")
        msg["to"]      = notify_email
        msg["subject"] = subject
        raw = base64.urlsafe_b64encode(msg.as_bytes()).decode()
        svc.users().messages().send(userId="me", body={"raw": raw}).execute()
        _write_log(log_path, f"✅ 通知信已寄出 → {notify_email}")
    except Exception as e:
        _write_log(log_path, f"⚠️ 寄信失敗：{e}\n{traceback.format_exc()}")


# ═══════════════════════════════════════════════════════════
# 主流程
# ═══════════════════════════════════════════════════════════

def run_once_if_due(*, log_path: Path = DEFAULT_LOG_PATH, force: bool = False) -> dict | None:
    cfg    = load_config()
    sched  = cfg.get("schedule", {})
    tz_name = sched.get("timezone", DEFAULT_TZ)
    now_dt = _now(tz_name)

    run, run_key = should_run_now(cfg, now_dt)

    if not force and not run:
        _write_log(log_path, f"今天（{now_dt.day}日 {now_dt.strftime('%H:%M')}）不在排程條件，略過")
        return None

    if not force and not _acquire_lock(run_key):
        _write_log(log_path, f"略過重複執行：{run_key}")
        return None

    period = _calc_period(now_dt)
    try:
        results = _execute(cfg, period, log_path)
    except Exception as e:
        _write_log(log_path, f"❌ 排程失敗：{e}\n{traceback.format_exc()}")
        return None

    _send_notify(cfg, period, results, log_path)
    return results


def start_scheduler_once(*, log_path: Path = DEFAULT_LOG_PATH, interval_seconds: int = 30):
    """Streamlit app 啟動時呼叫的背景執行緒版本（本機備用）。"""
    import threading
    marker = "_PERIOD_SCHEDULER_THREAD_STARTED"
    if os.environ.get(marker) == "1":
        return
    os.environ[marker] = "1"

    def _loop():
        _write_log(log_path, "背景排程器已啟動")
        while True:
            try:
                run_once_if_due(log_path=log_path)
            except Exception as e:
                _write_log(log_path, f"錯誤：{e}\n{traceback.format_exc()}")
            time.sleep(interval_seconds)

    threading.Thread(target=_loop, daemon=True).start()


# ═══════════════════════════════════════════════════════════
# CLI
# ═══════════════════════════════════════════════════════════

def main():
    parser = argparse.ArgumentParser(description="Lemon Clean 期別排程服務")
    parser.add_argument("--log",      default=str(DEFAULT_LOG_PATH))
    parser.add_argument("--run-once", action="store_true", help="單次檢查（GitHub Actions 用）")
    parser.add_argument("--force",    action="store_true", help="立刻執行，忽略排程日（測試用）")
    parser.add_argument("--daemon",   action="store_true", help="常駐執行，每 30 秒檢查（本機備用）")
    args = parser.parse_args()

    log_path = Path(args.log)

    if args.force:
        run_once_if_due(log_path=log_path, force=True)
    elif args.run_once:
        run_once_if_due(log_path=log_path, force=False)
    elif args.daemon:
        _write_log(log_path, "daemon 啟動")
        while True:
            try:
                run_once_if_due(log_path=log_path)
            except Exception as e:
                _write_log(log_path, f"daemon 錯誤：{e}\n{traceback.format_exc()}")
            time.sleep(30)
    else:
        parser.print_help()


if __name__ == "__main__":
    main()
