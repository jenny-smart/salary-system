"""
modules/scheduler_service.py
期別資料夾與檔案排程服務  v2026-05

設計原則：
- 地區設定、排程設定統一從 config.yaml 讀取
  （config.yaml 由 Streamlit 介面存檔後自動寫入，主控 Google Sheet 同步備援）
- 排程執行邏輯與手動按鈕完全一致：
    create_period() → record_batch()（打卡）→ 寄 email 通知
- 不依賴 schedule_config.json，避免兩份設定不同步

執行方式：
  launchd daemon（推薦）：
    python -m modules.scheduler_service --daemon
  單次檢查（cron 備援）：
    python -m modules.scheduler_service --run-once
  強制立刻執行（測試）：
    python -m modules.scheduler_service --force
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
# 工具函式
# ═══════════════════════════════════════════════════════════

def _now(tz_name: str = DEFAULT_TZ) -> datetime:
    if ZoneInfo is not None:
        return datetime.now(ZoneInfo(tz_name))
    return datetime.now()


def _write_log(path: Path, msg: str, tz_name: str = DEFAULT_TZ) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    ts = _now(tz_name).strftime("%Y-%m-%d %H:%M:%S")
    with path.open("a", encoding="utf-8") as f:
        f.write(f"[{ts}] {msg}\n")


# ═══════════════════════════════════════════════════════════
# 設定讀取（純 config.yaml）
# ═══════════════════════════════════════════════════════════

def load_config(path: Path = CONFIG_PATH) -> dict:
    """
    讀取 config.yaml。
    Streamlit 介面「儲存排程設定」或「儲存地區」後會自動更新這個檔案。
    排程執行前每次重新讀取，確保拿到最新設定。

    config.yaml 結構（相關欄位）：
      regions:
        - name: 新北
          root_folder_id: "..."
          allowance_id: "..."
          salary_id: "..."
          roster_id: "..."
      schedule:
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

def _run_key(now_dt: datetime, time_hhmm: str) -> str:
    return f"{now_dt.strftime('%Y-%m-%d')} {time_hhmm}"


def should_run_now(cfg: dict, now_dt: datetime | None = None) -> tuple[bool, str]:
    """
    判斷現在是否應該執行。
    回傳 (是否執行, run_key)。
    """
    sched    = cfg.get("schedule", {})
    tz_name  = sched.get("timezone", DEFAULT_TZ)
    now_dt   = now_dt or _now(tz_name)
    hhmm     = now_dt.strftime("%H:%M")
    key      = _run_key(now_dt, sched.get("time", "05:30"))

    if not sched.get("enabled", False):
        return False, key

    days = sched.get("days", [])
    if isinstance(days, str):
        days = [int(d.strip()) for d in days.split(",") if d.strip()]
    days = [int(d) for d in days]

    if now_dt.day not in days:
        return False, key

    if hhmm != str(sched.get("time", "05:30")).strip():
        return False, key

    return True, key


def _acquire_lock(run_key: str) -> bool:
    """同一個 run_key 只執行一次，避免每分鐘 cron 重複觸發。"""
    if LOCK_PATH.exists():
        if LOCK_PATH.read_text(encoding="utf-8").strip() == run_key:
            return False
    LOCK_PATH.write_text(run_key, encoding="utf-8")
    return True


def _calc_period(now_dt: datetime) -> str:
    """
    依日期產生期別（與 get_auto_period 邏輯一致）：
      1–15 日 → YYYYMM上
      16 日後  → YYYYMM下
    若 period_utils 可用則直接呼叫，否則備援自算。
    """
    try:
        from modules.period_utils import get_auto_period
        return get_auto_period()
    except Exception:
        suffix = "上" if now_dt.day <= 15 else "下"
        return f"{now_dt.year}{now_dt.month:02d}{suffix}"


# ═══════════════════════════════════════════════════════════
# 核心執行：create_period + 打卡
# ═══════════════════════════════════════════════════════════

def _run_region(
    region: dict,
    period: str,
    log_fn,
) -> bool:
    """
    對單一地區執行 create_period 並打卡。
    與手動按鈕「① 建立期別資料夾與檔案（排程）」邏輯完全一致。
    """
    name    = region.get("name", "未知")
    root_id = region.get("root_folder_id", "")

    if not root_id:
        log_fn(f"⚠️ 【{name}】root_folder_id 未設定，略過")
        return False

    def _log(msg):
        log_fn(f"[{name}] {msg}")

    try:
        from modules.payment_reconciliation import create_period
        _log(f"🔄 呼叫 GAS 建立期別：{period}")
        result    = create_period(root_id, period, name, _log)
        copied    = result.get("copied", 0)
        file_ids  = result.get("fileIds", {})
        folder_id = result.get("folderId")
        _log(f"✅ 建立完成，複製 {copied} 個檔案")

        # 打卡（同手動邏輯）
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


def run_create_period(
    *,
    cfg: dict,
    period: str,
    log_path: Path = DEFAULT_LOG_PATH,
    extra_log_fn=None,
) -> dict:
    """
    對所有啟用地區執行建立期別。
    回傳 {地區名: {"ok": bool, "logs": [...]}} 。
    """
    sched       = cfg.get("schedule", {})
    all_flag    = sched.get("all_regions", True)
    regions     = cfg.get("regions", []) if all_flag else []

    if not regions:
        raise RuntimeError("沒有可執行的地區（請確認 config.yaml regions 已設定）")

    results = {}
    all_logs = []

    for region in regions:
        name = region.get("name", "未知")
        logs = []

        def _log(msg, _logs=logs):
            _logs.append(msg)
            _write_log(log_path, msg)
            if extra_log_fn:
                extra_log_fn(msg)

        ok = _run_region(region, period, _log)
        results[name] = {"ok": ok, "logs": logs}
        all_logs.extend(logs)

    return results


# ═══════════════════════════════════════════════════════════
# email 通知
# ═══════════════════════════════════════════════════════════

def _send_notify(cfg: dict, period: str, results: dict, log_path: Path):
    notify_email = cfg.get("notify_email", "").strip()
    if not notify_email:
        _write_log(log_path, "notify_email 未設定，略過寄信")
        return

    ok_list   = [n for n, r in results.items() if r["ok"]]
    fail_list = [n for n, r in results.items() if not r["ok"]]
    all_logs  = [l for r in results.values() for l in r["logs"]]

    try:
        import base64
        from email.mime.text import MIMEText
        import googleapiclient.discovery
        import google.auth.transport.requests
        from modules.auth import get_credentials

        creds = get_credentials()
        if not getattr(creds, "token", None) or not creds.valid:
            creds.refresh(google.auth.transport.requests.Request())
        svc = googleapiclient.discovery.build(
            "gmail", "v1", credentials=creds, cache_discovery=False
        )

        subject = (
            f"⚠️ [{period}] 排程部分失敗：{', '.join(fail_list)}"
            if fail_list else
            f"✅ [{period}] 排程完成：{', '.join(ok_list)}"
        )

        tz_name  = cfg.get("schedule", {}).get("timezone", DEFAULT_TZ)
        now_str  = _now(tz_name).strftime("%Y-%m-%d %H:%M:%S")
        body_lines = [
            "Lemon Clean 排程通知",
            f"執行時間：{now_str}",
            f"期別：{period}",
            "",
            f"✅ 成功：{', '.join(ok_list) or '無'}",
            f"❌ 失敗：{', '.join(fail_list) or '無'}",
            "",
            "── 執行日誌 ──────────────────────────",
        ] + (all_logs or ["（無日誌）"])

        msg = MIMEText("\n".join(body_lines), "plain", "utf-8")
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

def run_once_if_due(
    *,
    log_path: Path = DEFAULT_LOG_PATH,
    force: bool = False,
) -> dict | None:
    """
    每次呼叫重新讀 config.yaml，判斷是否到排程時間。
    force=True 時忽略日期時間，直接執行（用於測試）。
    """
    cfg    = load_config()
    sched  = cfg.get("schedule", {})
    tz_name = sched.get("timezone", DEFAULT_TZ)
    now_dt = _now(tz_name)

    run, run_key = should_run_now(cfg, now_dt)

    if not force and not run:
        return None

    if not force and not _acquire_lock(run_key):
        _write_log(log_path, f"略過重複執行：{run_key}")
        return None

    period = _calc_period(now_dt)
    _write_log(log_path, f"═══ 排程觸發：period={period} ═══")

    try:
        results = run_create_period(cfg=cfg, period=period, log_path=log_path)
    except Exception as e:
        _write_log(log_path, f"❌ 排程失敗：{e}\n{traceback.format_exc()}")
        return None

    _send_notify(cfg, period, results, log_path)
    return results


def start_scheduler_once(
    *,
    log_path: Path = DEFAULT_LOG_PATH,
    interval_seconds: int = 30,
) -> None:
    """
    給 Streamlit app 啟動時呼叫的背景執行緒版本。
    注意：Streamlit Cloud 可能休眠，launchd daemon 模式更可靠。
    """
    import threading

    marker = "_PERIOD_SCHEDULER_THREAD_STARTED"
    if os.environ.get(marker) == "1":
        return
    os.environ[marker] = "1"

    def _loop():
        _write_log(log_path, "背景排程器已啟動（threading）")
        while True:
            try:
                run_once_if_due(log_path=log_path)
            except Exception as e:
                _write_log(log_path, f"背景排程器錯誤：{e}\n{traceback.format_exc()}")
            time.sleep(interval_seconds)

    threading.Thread(target=_loop, daemon=True).start()


# ═══════════════════════════════════════════════════════════
# CLI 進入點
# ═══════════════════════════════════════════════════════════

def main() -> None:
    parser = argparse.ArgumentParser(description="Lemon Clean 期別排程服務")
    parser.add_argument("--log",      default=str(DEFAULT_LOG_PATH), help="log 輸出路徑")
    parser.add_argument("--run-once", action="store_true", help="單次檢查，符合排程才執行")
    parser.add_argument("--force",    action="store_true", help="忽略日期時間，立即執行一次（測試用）")
    parser.add_argument("--daemon",   action="store_true", help="常駐執行，每 30 秒檢查一次（launchd 用）")
    args = parser.parse_args()

    log_path = Path(args.log)

    if args.force:
        run_once_if_due(log_path=log_path, force=True)
        return

    if args.run_once:
        run_once_if_due(log_path=log_path, force=False)
        return

    if args.daemon:
        _write_log(log_path, "Lemon Clean Scheduler daemon 啟動")
        while True:
            try:
                run_once_if_due(log_path=log_path, force=False)
            except Exception as e:
                _write_log(log_path, f"daemon 錯誤：{e}\n{traceback.format_exc()}")
            time.sleep(30)
        return

    parser.print_help()


if __name__ == "__main__":
    main()
