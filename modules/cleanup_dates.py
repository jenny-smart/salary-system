"""
大掃除日期設定
檔案：modules/cleanup_dates.py

在網頁「🧹 大掃除日期」設定一次，存放在永久設定表（與地區設定同一份試算表）的「大掃除日期」分頁；
前置作業執行時自動寫入各區清潔承攬檔的 薪資表／專案薪資表 B2006:B2008：
    B2006 大掃除1開始、B2007 大掃除2開始、B2008 大掃除最後一天（當天仍算大掃除）
每年 11、12 月若尚未設定本年度日期（沒有設定或已設定的日期已結束），網頁顯示提醒。
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime
from typing import List, Optional

import pytz

TAIPEI_TZ = pytz.timezone("Asia/Taipei")
SHEET_NAME = "大掃除日期"
HEADER = ["大掃除1開始", "大掃除2開始", "大掃除最後一天", "更新時間"]
TARGET_SHEETS = ("薪資表", "專案薪資表")
TARGET_RANGE = "B2006:B2008"


@dataclass(frozen=True)
class CleanupDates:
    stage1_start: date
    stage2_start: date
    last_day: date

    def errors(self) -> List[str]:
        errs = []
        if not self.stage1_start < self.stage2_start:
            errs.append("大掃除2開始必須晚於大掃除1開始")
        if not self.stage2_start <= self.last_day:
            errs.append("大掃除最後一天不可早於大掃除2開始")
        return errs

    def as_cells(self) -> List[List[str]]:
        return [[_fmt(self.stage1_start)], [_fmt(self.stage2_start)], [_fmt(self.last_day)]]


def _fmt(d: date) -> str:
    return f"{d.year}/{d.month}/{d.day}"


def _parse(value) -> Optional[date]:
    text = str(value or "").strip()
    for fmt in ("%Y/%m/%d", "%Y-%m-%d"):
        try:
            return datetime.strptime(text, fmt).date()
        except ValueError:
            continue
    return None


def parse_row(row) -> Optional[CleanupDates]:
    """設定分頁第 2 列 → CleanupDates；任何一格缺漏或格式錯誤回傳 None。"""
    cells = list(row or []) + [""] * 3
    parsed = [_parse(c) for c in cells[:3]]
    if any(p is None for p in parsed):
        return None
    return CleanupDates(*parsed)


def needs_reminder(today: date, dates: Optional[CleanupDates]) -> bool:
    """11、12 月且尚未設定本年度日期（沒有設定，或設定的大掃除已經結束）時提醒。"""
    if today.month not in (11, 12):
        return False
    return dates is None or dates.last_day < today


# ──────────────────────────────────────────────────────────────
# 讀寫設定表
# ──────────────────────────────────────────────────────────────

def _open_settings(spreadsheet_id: str):
    from modules.auth import open_spreadsheet
    return open_spreadsheet(spreadsheet_id)


def load(spreadsheet_id: str) -> Optional[CleanupDates]:
    ss = _open_settings(spreadsheet_id)
    try:
        ws = ss.worksheet(SHEET_NAME)
    except Exception:
        return None
    rows = ws.get("A2:C2") or [[]]
    return parse_row(rows[0] if rows else [])


def save(spreadsheet_id: str, dates: CleanupDates) -> None:
    errs = dates.errors()
    if errs:
        raise ValueError("；".join(errs))
    ss = _open_settings(spreadsheet_id)
    try:
        ws = ss.worksheet(SHEET_NAME)
    except Exception:
        ws = ss.add_worksheet(title=SHEET_NAME, rows=10, cols=len(HEADER))
    stamp = datetime.now(TAIPEI_TZ).strftime("%Y/%m/%d %H:%M:%S")
    ws.update(
        values=[HEADER, [_fmt(dates.stage1_start), _fmt(dates.stage2_start), _fmt(dates.last_day), stamp]],
        range_name="A1:D2",
        value_input_option="RAW",
    )


# ──────────────────────────────────────────────────────────────
# 前置作業：寫入清潔承攬檔
# ──────────────────────────────────────────────────────────────

def apply_to_cleaning_file(ss, dates: Optional[CleanupDates], log: List[str]) -> None:
    """把大掃除日期寫入 薪資表／專案薪資表 B2006:B2008；未設定時維持檔案原日期並提醒。"""
    if dates is None:
        log.append("    ⚠️ 尚未在網頁設定大掃除日期，薪資表 B2006:B2008 維持原日期")
        return
    if dates.errors():
        log.append(f"    ⚠️ 大掃除日期設定有誤（{'；'.join(dates.errors())}），維持原日期")
        return
    titles = {ws.title for ws in ss.worksheets()}
    for name in TARGET_SHEETS:
        if name not in titles:
            continue
        ss.worksheet(name).update(
            values=dates.as_cells(), range_name=TARGET_RANGE, value_input_option="USER_ENTERED",
        )
    log.append(
        f"    大掃除日期已寫入 B2006:B2008：{_fmt(dates.stage1_start)}／"
        f"{_fmt(dates.stage2_start)}／{_fmt(dates.last_day)}"
    )


def apply_from_settings(ss, log: List[str]) -> None:
    """前置作業呼叫：讀設定表再寫入；讀取失敗不中斷前置作業。"""
    try:
        from modules.master_sheet import MASTER_SHEET_ID
        dates = load(MASTER_SHEET_ID)
    except Exception as e:
        log.append(f"    ⚠️ 讀取大掃除日期設定失敗，B2006:B2008 維持原日期：{e}")
        return
    apply_to_cleaning_file(ss, dates, log)
