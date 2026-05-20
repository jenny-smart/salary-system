"""
modules/scheduler_service.py
期別資料夾與檔案排程服務

用途：
- 排程時間到時，直接呼叫手動建立期別使用的 create_period()
- 手動與排程共用同一套建立邏輯，避免兩套流程不一致
- 支援 Streamlit 背景排程，也支援 CLI / cron / systemd 執行

重要：
1. Streamlit Cloud / Render / Railway 若會休眠，背景排程可能不會在 05:30 被喚醒。
   最穩定做法是用 cron 或 systemd 定時執行：
      python -m modules.scheduler_service --run-once
2. 若部署環境是長駐 VPS，則可在 Streamlit app 啟動時呼叫 start_scheduler_once()。
"""

from __future__ import annotations

import argparse
import json
import os
import time
import traceback
from dataclasses import dataclass, asdict
from datetime import datetime
from pathlib import Path
from typing import Callable, Iterable

try:
    from zoneinfo import ZoneInfo
except Exception:  # pragma: no cover
    ZoneInfo = None

# 直接引用手動建立期別的同一個函式
from modules.payment_reconciliation import create_period


DEFAULT_TZ = "Asia/Taipei"
DEFAULT_CONFIG_PATH = Path("schedule_config.json")
DEFAULT_LOG_PATH = Path("schedule_run_log.txt")
LOCK_PATH = Path(".period_scheduler.lock")


@dataclass
class RegionConfig:
    """單一地區設定。root_folder_id 必須是該地區 Google Drive 根資料夾 ID。"""
    region_name: str
    root_folder_id: str
    enabled: bool = True


@dataclass
class ScheduleConfig:
    """
    排程設定。

    days:
      例：["10", "20"] 或 [10, 20]
      10 號通常產生 YYYYMM-1
      20 號通常產生 YYYYMM-2

    time_hhmm:
      台北時間 HH:MM，例如 "05:30"

    run_all_regions:
      True 時會跑 regions 內所有 enabled 地區。
      False 時仍會依 regions enabled 判斷，方便保留單區模式。

    enabled:
      False 時排程不會執行。
    """
    days: list[int]
    time_hhmm: str
    regions: list[RegionConfig]
    enabled: bool = True
    run_all_regions: bool = True
    timezone: str = DEFAULT_TZ
    last_run_key: str | None = None


def _now(tz_name: str = DEFAULT_TZ) -> datetime:
    if ZoneInfo is None:
        return datetime.now()
    return datetime.now(ZoneInfo(tz_name))


def _normalize_days(days: Iterable[int | str]) -> list[int]:
    result: list[int] = []
    for d in days:
        if isinstance(d, str):
            d = d.strip()
            if not d:
                continue
            result.append(int(d))
        else:
            result.append(int(d))
    return sorted(set(result))


def parse_days_text(days_text: str) -> list[int]:
    """
    將 UI 輸入的「10,20」轉成 [10, 20]。
    """
    return _normalize_days(days_text.replace("，", ",").split(","))


def calc_period(now_dt: datetime | None = None, day: int | None = None) -> str:
    """
    依日期產生期別：
    - 每月 10 日：YYYYMM-1
    - 每月 20 日：YYYYMM-2
    - 若你設定其他日期：1-15 日歸 -1，16 日後歸 -2
    """
    now_dt = now_dt or _now()
    d = int(day or now_dt.day)
    suffix = "1" if d <= 15 else "2"
    return f"{now_dt.year}{now_dt.month:02d}-{suffix}"


def _write_log(log_path: Path, msg: str, tz_name: str = DEFAULT_TZ) -> None:
    log_path.parent.mkdir(parents=True, exist_ok=True)
    ts = _now(tz_name).strftime("%Y-%m-%d %H:%M:%S")
    with log_path.open("a", encoding="utf-8") as f:
        f.write(f"[{ts}] {msg}\n")


def load_config(path: str | Path = DEFAULT_CONFIG_PATH) -> ScheduleConfig:
    path = Path(path)
    data = json.loads(path.read_text(encoding="utf-8"))

    regions = [
        RegionConfig(
            region_name=r["region_name"],
            root_folder_id=r["root_folder_id"],
            enabled=bool(r.get("enabled", True)),
        )
        for r in data.get("regions", [])
    ]

    return ScheduleConfig(
        days=_normalize_days(data.get("days", [10, 20])),
        time_hhmm=str(data.get("time_hhmm", "05:30")),
        regions=regions,
        enabled=bool(data.get("enabled", True)),
        run_all_regions=bool(data.get("run_all_regions", True)),
        timezone=str(data.get("timezone", DEFAULT_TZ)),
        last_run_key=data.get("last_run_key"),
    )


def save_config(config: ScheduleConfig, path: str | Path = DEFAULT_CONFIG_PATH) -> None:
    path = Path(path)
    payload = asdict(config)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")


def save_config_from_ui(
    *,
    days_text: str,
    time_hhmm: str,
    regions: list[dict],
    enabled: bool,
    run_all_regions: bool,
    path: str | Path = DEFAULT_CONFIG_PATH,
    timezone: str = DEFAULT_TZ,
) -> ScheduleConfig:
    """
    給 Streamlit UI 使用的儲存函式。

    regions 格式：
    [
      {"region_name": "台北", "root_folder_id": "...", "enabled": True},
      ...
    ]
    """
    config = ScheduleConfig(
        days=parse_days_text(days_text),
        time_hhmm=time_hhmm.strip(),
        regions=[
            RegionConfig(
                region_name=r["region_name"],
                root_folder_id=r["root_folder_id"],
                enabled=bool(r.get("enabled", True)),
            )
            for r in regions
        ],
        enabled=enabled,
        run_all_regions=run_all_regions,
        timezone=timezone,
    )
    save_config(config, path)
    return config


def _acquire_daily_lock(run_key: str) -> bool:
    """
    避免同一分鐘 / 同一天重複執行。
    回傳 True 表示本次可以執行。
    """
    if LOCK_PATH.exists():
        old = LOCK_PATH.read_text(encoding="utf-8").strip()
        if old == run_key:
            return False
    LOCK_PATH.write_text(run_key, encoding="utf-8")
    return True


def should_run_now(config: ScheduleConfig, now_dt: datetime | None = None) -> tuple[bool, str]:
    """
    判斷目前是否符合排程。
    回傳：(是否執行, run_key)
    """
    now_dt = now_dt or _now(config.timezone)
    hhmm = now_dt.strftime("%H:%M")
    run_key = f"{now_dt.strftime('%Y-%m-%d')} {config.time_hhmm}"

    if not config.enabled:
        return False, run_key
    if now_dt.day not in config.days:
        return False, run_key
    if hhmm != config.time_hhmm:
        return False, run_key
    if config.last_run_key == run_key:
        return False, run_key

    return True, run_key


def run_create_period_for_regions(
    *,
    config: ScheduleConfig,
    period: str,
    log_path: str | Path = DEFAULT_LOG_PATH,
    log_fn: Callable[[str], None] | None = None,
) -> dict:
    """
    真正執行建立期別。
    這裡會呼叫 payment_reconciliation.create_period()，
    也就是和手動按鈕同一套 GAS createPeriod 流程。
    """
    log_path = Path(log_path)

    def log(msg: str) -> None:
        if log_fn:
            log_fn(msg)
        _write_log(log_path, msg, config.timezone)

    results: dict[str, dict] = {}
    enabled_regions = [r for r in config.regions if r.enabled]

    if not enabled_regions:
        raise RuntimeError("沒有任何啟用地區，請檢查 schedule_config.json 的 regions。")

    log(f"🚀 排程開始：period={period}，地區數={len(enabled_regions)}")

    for r in enabled_regions:
        try:
            log(f"🔄 建立期別開始：{r.region_name} / {period}")
            result = create_period(
                root_folder_id=r.root_folder_id,
                period=period,
                region_name=r.region_name,
                log_fn=log,
            )
            results[r.region_name] = {"success": True, "result": result}
            log(f"✅ 建立期別完成：{r.region_name} / {period}")
        except Exception as e:
            results[r.region_name] = {"success": False, "error": str(e)}
            log(f"❌ 建立期別失敗：{r.region_name} / {period} / {e}")
            log(traceback.format_exc())

    log(f"🏁 排程結束：period={period}")
    return results


def run_once_if_due(
    *,
    config_path: str | Path = DEFAULT_CONFIG_PATH,
    log_path: str | Path = DEFAULT_LOG_PATH,
    force: bool = False,
) -> dict | None:
    """
    執行一次檢查：
    - 若目前符合日期與時間，就建立期別
    - 若 force=True，忽略日期時間，直接執行

    cron 建議每分鐘呼叫一次：
      * * * * * cd /path/to/app && python -m modules.scheduler_service --run-once
    """
    config_path = Path(config_path)
    config = load_config(config_path)
    now_dt = _now(config.timezone)
    run, run_key = should_run_now(config, now_dt)

    if not force and not run:
        return None

    if not force and not _acquire_daily_lock(run_key):
        _write_log(Path(log_path), f"略過重複執行：{run_key}", config.timezone)
        return None

    period = calc_period(now_dt, now_dt.day)
    results = run_create_period_for_regions(
        config=config,
        period=period,
        log_path=log_path,
    )

    config.last_run_key = run_key
    save_config(config, config_path)

    return results


def start_scheduler_once(
    *,
    config_path: str | Path = DEFAULT_CONFIG_PATH,
    log_path: str | Path = DEFAULT_LOG_PATH,
    interval_seconds: int = 30,
) -> None:
    """
    給 Streamlit app 啟動時呼叫。
    注意：只有在 Python process 長駐時才可靠。
    """
    import threading

    marker = "_PERIOD_SCHEDULER_THREAD_STARTED"
    if os.environ.get(marker) == "1":
        return

    os.environ[marker] = "1"

    def loop() -> None:
        _write_log(Path(log_path), "背景排程器已啟動", DEFAULT_TZ)
        while True:
            try:
                run_once_if_due(config_path=config_path, log_path=log_path)
            except Exception as e:
                _write_log(Path(log_path), f"背景排程器錯誤：{e}", DEFAULT_TZ)
                _write_log(Path(log_path), traceback.format_exc(), DEFAULT_TZ)
            time.sleep(interval_seconds)

    t = threading.Thread(target=loop, daemon=True)
    t.start()


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--config", default=str(DEFAULT_CONFIG_PATH))
    parser.add_argument("--log", default=str(DEFAULT_LOG_PATH))
    parser.add_argument("--run-once", action="store_true", help="只檢查一次，符合排程才執行。")
    parser.add_argument("--force", action="store_true", help="忽略日期時間，立即執行一次。")
    parser.add_argument("--daemon", action="store_true", help="常駐執行，每 30 秒檢查一次。")
    args = parser.parse_args()

    if args.force:
        run_once_if_due(config_path=args.config, log_path=args.log, force=True)
        return

    if args.run_once:
        run_once_if_due(config_path=args.config, log_path=args.log, force=False)
        return

    if args.daemon:
        while True:
            try:
                run_once_if_due(config_path=args.config, log_path=args.log, force=False)
            except Exception as e:
                _write_log(Path(args.log), f"daemon 錯誤：{e}")
                _write_log(Path(args.log), traceback.format_exc())
            time.sleep(30)
        return

    parser.print_help()


if __name__ == "__main__":
    main()
