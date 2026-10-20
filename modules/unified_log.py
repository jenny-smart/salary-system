"""各系統統一執行 Log：寫到主控表（Jenny's Lemonhometools）「salary-system執行Log-{年度}」。

tool-system、orders-system、order-system、salary-system 每個系統每年一張、欄位相同。
各地區打卡表維持原樣，這裡另外同步一份；新年度第一次寫入時自動建立工作表。
寫入失敗只印警告，不影響原本流程。
"""
from __future__ import annotations

import os
from datetime import datetime
from typing import Any

import pytz

TAIPEI_TZ = pytz.timezone("Asia/Taipei")
MASTER_SPREADSHEET_ID = "1nNAXy6rvBnGR8ACnqKKzKNA4-UwZtZp47i806EPmR_8"
REPO = "salary-system"
HEADERS = ["執行時間", "系統", "功能", "執行方式", "區域", "期別/日期", "目標位置", "來源檔案", "結果", "訊息", "執行者"]

_ensured: set[str] = set()


def sheet_title(year: int | None = None) -> str:
    return f"{REPO}執行Log-{year or datetime.now(TAIPEI_TZ).year}"


def build_row(*, system: str = "薪資系統", function: str = "", run_type: str = "", area: str = "",
              period: str = "", target: str = "", source_file: str = "", status: str = "成功",
              message: Any = "", operator: str = "") -> list[str]:
    return [datetime.now(TAIPEI_TZ).strftime("%Y-%m-%d %H:%M:%S"), system, function, run_type, area,
            str(period or ""), target, source_file, status, str(message if message is not None else "")[:2000],
            operator]


def _worksheet(spreadsheet):
    title = sheet_title()
    if title in _ensured:
        return spreadsheet.worksheet(title)
    try:
        ws = spreadsheet.worksheet(title)
    except Exception:
        ws = spreadsheet.add_worksheet(title=title, rows=1000, cols=len(HEADERS))
        ws.update(range_name="A1", values=[HEADERS])
        ws.freeze(rows=1)
    _ensured.add(title)
    return ws


def write(spreadsheet=None, **fields: Any) -> None:
    """寫一筆統一 Log；欄位見 build_row。絕不拋出例外。"""
    try:
        if spreadsheet is None:
            from modules.auth import open_spreadsheet

            spreadsheet = open_spreadsheet(os.getenv("MASTER_SPREADSHEET_ID", "").strip() or MASTER_SPREADSHEET_ID)
        _worksheet(spreadsheet).append_row(build_row(**fields), value_input_option="RAW",
                                            insert_data_option="INSERT_ROWS")
    except Exception as exc:
        print(f"[unified_log] 寫入統一執行 Log 失敗（不影響主流程）：{exc}", flush=True)
