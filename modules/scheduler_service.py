"""
modules/scheduler_service.py
Lemon Clean 期別排程執行服務 v2026-06

這版設計：
  - 不再由 GitHub cron 定時檢查。
  - 排程由 GAS Trigger 負責。
  - GAS 到時間後呼叫 GitHub workflow_dispatch。
  - 本程式只負責「被叫醒後執行指定 action」。

CLI 用法：
  --force                 直接執行
  --period 202606-2       指定期別；留空則自動判斷
  --region 台北            指定地區；留空則全部地區
  --action create_period  指定功能；預設 create_period
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
    return cfg


def _calc_period(now_dt: datetime) -> str:
    """
    正常規則：
      1～15 日  → 當月-1
      16～月底 → 當月-2
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
    normalized = str(action or "").strip()

    if normalized in {
        "create_period",
        "建立期別資料夾與檔案",
        "建立期別資料夾與檔案",
        "期別資料夾",
        "建立期別",
    }:
        from modules.payment_reconciliation import create_period
        return create_period(root_id, period, region_name, log_fn)

    raise ValueError(f"未知的排程 action：{action}")


# ═══════════════════════════════════════════════════════════
# 地區處理
# ═══════════════════════════════════════════════════════════

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


def _filter_regions(
    regions: list[dict],
    force_region: str | None = None,
) -> list[dict]:
    if not force_region:
        return regions

    target = force_region.strip()

    return [
        r for r in regions
        if str(r.get("name", "")).strip() == target
    ]


def _run_region(region: dict, period: str, log_fn) -> bool:
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


# ═══════════════════════════════════════════════════════════
# 核心執行
# ═══════════════════════════════════════════════════════════

def _execute(
    cfg: dict,
    period: str,
    log_path: Path,
    action: str,
    region: str | None = None,
) -> dict:
    regions = _normalize_regions(cfg, action)
    regions = _filter_regions(regions, force_region=region)

    if not regions:
        raise RuntimeError(
            "沒有可執行的地區，請確認 config.yaml regions 已設定，或確認 --region 名稱是否正確"
        )

    _build_credentials()

    results = {}
    region_names = ", ".join([r.get("name", "未知") for r in regions])

    _write_log(
        log_path,
        f"═══ 開始執行：period={period}，action={action}，地區數={len(regions)}，地區={region_names} ═══",
    )

    for region_cfg in regions:
        name = region_cfg.get("name", "未知")
        logs = []

        def _log(msg, _logs=logs):
            _logs.append(msg)
            _write_log(log_path, msg)

        ok = _run_region(region_cfg, period, _log)
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

    now_str = _now(DEFAULT_TZ).strftime("%Y-%m-%d %H:%M:%S")

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

def run_scheduler(
    *,
    log_path: Path = DEFAULT_LOG_PATH,
    period: str | None = None,
    region: str | None = None,
    action: str = "create_period",
) -> dict | None:
    cfg = load_config()
    now_dt = _now(DEFAULT_TZ)

    if period:
        selected_period = period.strip()
        _write_log(log_path, f"使用指定期別：{selected_period}")
    else:
        selected_period = _calc_period(now_dt)
        _write_log(log_path, f"自動判斷期別：{selected_period}")

    if region:
        _write_log(log_path, f"使用指定地區：{region.strip()}")

    _write_log(log_path, f"使用 action：{action}")

    try:
        results = _execute(
            cfg,
            selected_period,
            log_path,
            action=action,
            region=region,
        )
    except Exception as e:
        _write_log(log_path, f"❌ 執行失敗：{e}\n{traceback.format_exc()}")
        return None

    _send_notify(cfg, selected_period, results, log_path)

    return results


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
        "--force",
        action="store_true",
        help="直接執行；保留此參數是為了相容 GitHub workflow",
    )

    parser.add_argument(
        "--period",
        type=str,
        default="",
        help="指定期別，例如 202606-1；留空則自動判斷",
    )

    parser.add_argument(
        "--region",
        type=str,
        default="",
        help="指定地區，例如 台北；留空則全部地區",
    )

    parser.add_argument(
        "--action",
        type=str,
        default="create_period",
        help="指定功能，例如 create_period",
    )

    args = parser.parse_args()

    log_path = Path(args.log)
    period = args.period.strip() or None
    region = args.region.strip() or None
    action = args.action.strip() or "create_period"

    run_scheduler(
        log_path=log_path,
        period=period,
        region=region,
        action=action,
    )


if __name__ == "__main__":
    main()
