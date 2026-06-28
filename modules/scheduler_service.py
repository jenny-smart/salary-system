"""
modules/scheduler_service.py
期別資料夾與檔案排程服務 v2026-06

執行環境：
  - GitHub Actions：credentials 從環境變數讀取
  - 本機測試：credentials 從 modules.auth.get_credentials() 讀取

CLI 用法：
  --run-once          單次檢查，符合排程日才執行
  --force            忽略排程日，立刻執行
  --period 202606-1  指定期別；留空則自動判斷
  --daemon           常駐執行，每 30 秒檢查一次
"""

from __future__ import annotations

import argparse
import os
import time
import traceback
from datetime import datetime
from pathlib import Path

try:
    from zoneinfo import ZoneInfo
except ImportError:
    ZoneInfo = None

import yaml


DEFAULT_TZ = "Asia/Taipei"
DEFAULT_LOG_PATH = Path("logs/scheduler.log")
LOCK_PATH = Path(".period_scheduler.lock")
CONFIG_PATH = Path("config.yaml")


# ═══════════════════════════════════════════════════════════
# 基礎工具
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

    print(line, flush=True)


# ═══════════════════════════════════════════════════════════
# Credentials
# ═══════════════════════════════════════════════════════════

def _build_credentials():
    client_id = os.environ.get("OAUTH_CLIENT_ID", "").strip()
    client_secret = os.environ.get("OAUTH_CLIENT_SECRET", "").strip()
    refresh_token = os.environ.get("OAUTH_REFRESH_TOKEN", "").strip()

    if client_id and client_secret and refresh_token:
        from google.oauth2.credentials import Credentials
        from google.auth.transport.requests import Request

        creds = Credentials(
            token=None,
            refresh_token=refresh_token,
            client_id=client_id,
            client_secret=client_secret,
            token_uri="https://oauth2.googleapis.com/token",
            scopes=[
                "https://www.googleapis.com/auth/drive",
                "https://www.googleapis.com/auth/spreadsheets",
                "https://www.googleapis.com/auth/gmail.send",
            ],
        )
        creds.refresh(Request())
        return creds

    from modules.auth import get_credentials
    return get_credentials()


# ═══════════════════════════════════════════════════════════
# 設定讀取
# ═══════════════════════════════════════════════════════════

def load_config(path: Path = CONFIG_PATH) -> dict:
    try:
        with open(path, "r", encoding="utf-8") as f:
            cfg = yaml.safe_load(f) or {}
    except FileNotFoundError:
        cfg = {}

    cfg.setdefault("regions", {})
    cfg.setdefault("schedule", {})
    return cfg


# ═══════════════════════════════════════════════════════════
# 排程判斷
# ═══════════════════════════════════════════════════════════

def should_run_now(cfg: dict, now_dt: datetime | None = None) -> tuple[bool, str]:
    sched = cfg.get("schedule", {})
    tz_name = sched.get("timezone", DEFAULT_TZ)
    now_dt = now_dt or _now(tz_name)

    hhmm = now_dt.strftime("%H:%M")
    run_key = f"{now_dt.strftime('%Y-%m-%d')} {sched.get('time', '05:30')}"

    if not sched.get("enabled", False):
        return False, run_key

    days = sched.get("days", [])

    if isinstance(days, str):
        days = [int(d.strip()) for d in days.split(",") if d.strip()]

    days = [int(d) for d in days]

    if now_dt.day not in days:
        return False, run_key

    cfg_time = str(sched.get("time", "05:30")).strip()
    cfg_hour = cfg_time[:2]

    # GitHub Actions cron 不一定準到分鐘，所以只比對小時
    if hhmm[:2] != cfg_hour:
        return False, run_key

    return True, run_key


def _acquire_lock(run_key: str) -> bool:
    if LOCK_PATH.exists():
        current = LOCK_PATH.read_text(encoding="utf-8").strip()
        if current == run_key:
            return False

    LOCK_PATH.write_text(run_key, encoding="utf-8")
    return True


def _calc_period(now_dt: datetime) -> str:
    """
    正常規則：
      1～15 日  → 當月-1
      16～月底 → 當月-2

    若 modules.period_utils.get_auto_period() 存在，優先使用原本模組。
    """
    try:
        from modules.period_utils import get_auto_period
        return get_auto_period()
    except Exception:
        suffix = "1" if now_dt.day <= 15 else "2"
        return f"{now_dt.year}{now_dt.month:02d}-{suffix}"


# ═══════════════════════════════════════════════════════════
# Action 分派
# ═══════════════════════════════════════════════════════════

def _execute_action(
    action: str,
    root_id: str,
    period: str,
    region_name: str,
    log_fn,
):
    """
    根據 config.yaml 的 schedule.action / schedule.task 執行對應功能。
    """

    normalized = str(action or "").strip()

    if normalized in {
        "create_period",
        "建立期別資料夾與檔案",
        "期別資料夾",
        "建立期別",
    }:
        from modules.payment_reconciliation import create_period
        return create_period(root_id, period, region_name, log_fn)

    raise ValueError(f"未知的排程 action：{action}")


# ═══════════════════════════════════════════════════════════
# 地區執行
# ═══════════════════════════════════════════════════════════

def _run_region(region: dict, period: str, log_fn, creds) -> bool:
    name = region.get("name", "未知")
    root_id = region.get("root_folder_id", "")
    action = region.get("action", "create_period")

    if not root_id:
        log_fn(f"⚠️ 【{name}】root_folder_id 未設定，略過")
        return False

    def _log(msg):
        log_fn(f"  [{name}] {msg}")

    try:
        result = _execute_action(
            action=action,
            root_id=root_id,
            period=period,
            region_name=name,
            log_fn=_log,
        )

        copied = result.get("copied", 0)
        file_ids = result.get("fileIds", {})
        folder_id = result.get("folderId")

        _log(f"✅ {action} 執行完成，複製 {copied} 個檔案")

        try:
            from modules.master_sheet import record_batch

            record_batch(
                name,
                period,
                [
                    {"task_key": "排程期別資料夾", "count": folder_id},
                    {"task_key": "排程期別金流對帳", "count": file_ids.get("金流對帳")},
                    {"task_key": "排程期別清潔承攬", "count": file_ids.get("清潔承攬")},
                    {"task_key": "排程期別其他承攬", "count": file_ids.get("其他承攬")},
                    {"task_key": "排程期別元大帳戶", "count": file_ids.get("元大帳戶")},
                ],
            )

            _log("🔵 打卡完成")

        except Exception as e:
            _log(f"⚠️ 打卡失敗：{e}")

        return True

    except Exception as e:
        _log(f"❌ 失敗：{e}\n{traceback.format_exc()}")
        return False


def _normalize_regions(cfg: dict, action: str) -> list[dict]:
    regions_cfg = cfg.get("regions", {})
    regions: list[dict] = []

    if isinstance(regions_cfg, dict):
        for region_name, region_data in regions_cfg.items():
            region = dict(region_data or {})
            region["name"] = region_name
            region["action"] = action
            regions.append(region)

    elif isinstance(regions_cfg, list):
        for region_data in regions_cfg:
            region = dict(region_data or {})
            region.setdefault("name", "未知")
            region["action"] = action
            regions.append(region)

    return regions


def _filter_regions_by_schedule(regions: list[dict], sched: dict) -> list[dict]:
    all_flag = sched.get("all_regions", True)

    if all_flag:
        return regions

    selected_region = (
        sched.get("region")
        or sched.get("selected_region")
        or sched.get("region_name")
    )

    if not selected_region:
        return regions

    return [
        r for r in regions
        if r.get("name") == selected_region
    ]


# ═══════════════════════════════════════════════════════════
# 核心執行
# ═══════════════════════════════════════════════════════════

def _execute(cfg: dict, period: str, log_path: Path) -> dict:
    sched = cfg.get("schedule", {})

    action = (
        sched.get("action")
        or sched.get("task")
        or "create_period"
    )

    regions = _normalize_regions(cfg, action)
    regions = _filter_regions_by_schedule(regions, sched)

    if not regions:
        raise RuntimeError("沒有可執行的地區，請確認 config.yaml regions 已設定")

    creds = _build_credentials()
    results = {}

    _write_log(
        log_path,
        f"═══ 開始執行：period={period}，action={action}，地區數={len(regions)} ═══",
    )

    for region in regions:
        name = region.get("name", "未知")
        logs = []

        def _log(msg, _logs=logs):
            _logs.append(msg)
            _write_log(log_path, msg)

        ok = _run_region(region, period, _log, creds)
        results[name] = {
            "ok": ok,
            "logs": logs,
        }

    return results


# ═══════════════════════════════════════════════════════════
# Email 通知
# ═══════════════════════════════════════════════════════════

def _send_notify(cfg: dict, period: str, results: dict, log_path: Path):
    notify_email = (
        os.environ.get("NOTIFY_EMAIL", "").strip()
        or cfg.get("notify_email", "").strip()
    )

    if not notify_email:
        _write_log(log_path, "notify_email 未設定，略過寄信")
        return

    ok_list = [n for n, r in results.items() if r["ok"]]
    fail_list = [n for n, r in results.items() if not r["ok"]]
    all_logs = [l for r in results.values() for l in r["logs"]]

    subject = (
        f"⚠️ [{period}] 排程部分失敗：{', '.join(fail_list)}"
        if fail_list
        else f"✅ [{period}] 排程完成：{', '.join(ok_list)}"
    )

    tz_name = cfg.get("schedule", {}).get("timezone", DEFAULT_TZ)
    now_str = _now(tz_name).strftime("%Y-%m-%d %H:%M:%S")

    body = "\n".join(
        [
            "Lemon Clean 排程通知",
            f"執行時間：{now_str}",
            f"期別：{period}",
            "",
            f"✅ 成功：{', '.join(ok_list) or '無'}",
            f"❌ 失敗：{', '.join(fail_list) or '無'}",
            "",
            "── 執行日誌 ──────────────────────────",
        ]
        + (all_logs or ["（無日誌）"])
    )

    try:
        import base64
        from email.mime.text import MIMEText
        import googleapiclient.discovery

        creds = _build_credentials()

        svc = googleapiclient.discovery.build(
            "gmail",
            "v1",
            credentials=creds,
            cache_discovery=False,
        )

        msg = MIMEText(body, "plain", "utf-8")
        msg["to"] = notify_email
        msg["subject"] = subject

        raw = base64.urlsafe_b64encode(msg.as_bytes()).decode()

        svc.users().messages().send(
            userId="me",
            body={"raw": raw},
        ).execute()

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
    period: str | None = None,
) -> dict | None:
    cfg = load_config()
    sched = cfg.get("schedule", {})
    tz_name = sched.get("timezone", DEFAULT_TZ)
    now_dt = _now(tz_name)

    run, run_key = should_run_now(cfg, now_dt)

    if not force and not run:
        _write_log(
            log_path,
            f"今天（{now_dt.day}日 {now_dt.strftime('%H:%M')}）不在排程條件，略過",
        )
        return None

    if not force and not _acquire_lock(run_key):
        _write_log(log_path, f"略過重複執行：{run_key}")
        return None

    if period:
        selected_period = period.strip()
        _write_log(log_path, f"使用指定期別：{selected_period}")
    else:
        selected_period = _calc_period(now_dt)
        _write_log(log_path, f"自動判斷期別：{selected_period}")

    try:
        results = _execute(cfg, selected_period, log_path)
    except Exception as e:
        _write_log(log_path, f"❌ 排程失敗：{e}\n{traceback.format_exc()}")
        return None

    _send_notify(cfg, selected_period, results, log_path)

    return results


def start_scheduler_once(
    *,
    log_path: Path = DEFAULT_LOG_PATH,
    interval_seconds: int = 30,
):
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

    parser.add_argument(
        "--log",
        default=str(DEFAULT_LOG_PATH),
        help="log 檔案路徑",
    )

    parser.add_argument(
        "--run-once",
        action="store_true",
        help="單次檢查，符合排程日才執行",
    )

    parser.add_argument(
        "--force",
        action="store_true",
        help="立刻執行，忽略排程日",
    )

    parser.add_argument(
        "--period",
        type=str,
        default="",
        help="指定期別，例如 202606-1；留空則自動判斷",
    )

    parser.add_argument(
        "--daemon",
        action="store_true",
        help="常駐執行，每 30 秒檢查",
    )

    args = parser.parse_args()
    log_path = Path(args.log)
    period = args.period.strip() or None

    if args.force:
        run_once_if_due(
            log_path=log_path,
            force=True,
            period=period,
        )

    elif args.run_once:
        run_once_if_due(
            log_path=log_path,
            force=False,
            period=period,
        )

    elif args.daemon:
        _write_log(log_path, "daemon 啟動")

        while True:
            try:
                run_once_if_due(
                    log_path=log_path,
                    period=period,
                )
            except Exception as e:
                _write_log(log_path, f"daemon 錯誤：{e}\n{traceback.format_exc()}")

            time.sleep(30)

    else:
        parser.print_help()


if __name__ == "__main__":
    main()
