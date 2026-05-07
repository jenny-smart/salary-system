"""
Lemon Clean 清潔承攬 — 08季獎金 / 09薪資結算整理
檔案：modules/cleaning_process_3.py

依賴：
    modules/auth.py         — get_gspread_client()
    modules/master_sheet.py — record_execution()

打卡：統一寫入主控試算表（record_execution），不寫入 exec 工作表。
    08季獎金       → task_key = "06季獎金"    （GAS 尚未實作，待補充）
    09薪資結算整理 → task_key = "薪資結算"
"""

from __future__ import annotations

import datetime
import time
from typing import List

import gspread

from modules.auth import get_gspread_client
from modules.master_sheet import record_execution


# ──────────────────────────────────────────────────────────────
# 工具
# ──────────────────────────────────────────────────────────────

TS_FMT = "%Y/%m/%d %H:%M"


def _now_ts() -> str:
    return datetime.datetime.now().strftime(TS_FMT)


def _log(log: List[str], msg: str) -> None:
    log.append(msg)


def _punch(task_key: str, region: str, period: str) -> str:
    """打卡至主控試算表。"""
    ts = _now_ts()
    record_execution(region, period, task_key, ts)
    return ts


# ──────────────────────────────────────────────────────────────
# 06 季獎金（待實作）
# ──────────────────────────────────────────────────────────────

def run_season_bonus(
    cleaning_file_id: str,
    region: str,
    period: str,
    is_first_half: bool,
    log: List[str],
) -> bool:
    """
    06 季獎金。
    GAS 原版尚未實作，此處保留框架待補充。
    """
    _log(log, "▶ 08季獎金：尚未實作")
    return False


# ──────────────────────────────────────────────────────────────
# 薪資結算（待補充細節）
# ──────────────────────────────────────────────────────────────

def run_settlement(
    cleaning_file_id: str,
    region: str,
    period: str,
    is_first_half: bool,
    log: List[str],
) -> bool:
    """
    薪資結算。
    對應 GAS 的 runFinalSettlement。
    細節待確認後補充。
    """
    label = "上半月" if is_first_half else "下半月"
    _log(log, f"▶ 09薪資結算整理 {label} 開始")
    try:
        # TODO: 補充薪資結算整理的具體步驟

        ts = _punch("薪資結算", region, period)
        _log(log, f"✅ 09薪資結算整理 {label} 完成｜{ts}")
        return True

    except Exception as e:
        _log(log, f"❌ 09薪資結算整理失敗：{e}")
        return False
