"""
匯入引擎切換與稽核
檔案：modules/import_engine.py

目的：
    逐步把「寫 IMPORTRANGE → 等載入 → 轉靜態值」改成「Python 直接讀來源 → 篩選 → 寫靜態值」，
    但隨時可以切回 IMPORTRANGE，且每次執行都有紀錄可以稽核。

三種引擎（主控試算表「匯入引擎設定」分頁，A=作業、B=引擎、C=備註）：
    IMPORTRANGE  原本做法（預設；分頁不存在、找不到作業、讀取失敗、值不認得都用這個）
    COMPARE      照原本 IMPORTRANGE 執行（結果以它為準），另外用 Python 算一次並逐格比對，
                 結果寫入「匯入引擎Log」。比對失敗不影響原本流程。
    PYTHON       用 Python 直接讀來源寫入靜態值；若讀取來源失敗，自動退回 IMPORTRANGE 並記錄。

切回原本做法：把「匯入引擎設定」該作業的 B 欄改成 IMPORTRANGE（或整列刪掉）即可，不用重新部署。
"""

from __future__ import annotations

import time
from datetime import datetime
from typing import Iterable, List, Sequence

import pytz

TAIPEI_TZ = pytz.timezone("Asia/Taipei")

ENGINE_IMPORTRANGE = "IMPORTRANGE"
ENGINE_COMPARE = "COMPARE"
ENGINE_PYTHON = "PYTHON"
ENGINES = (ENGINE_IMPORTRANGE, ENGINE_COMPARE, ENGINE_PYTHON)
DEFAULT_ENGINE = ENGINE_IMPORTRANGE

SETTINGS_SHEET = "匯入引擎設定"
SETTINGS_HEADER = ["作業", "引擎", "備註"]

LOG_SHEET = "匯入引擎Log"
LOG_HEADER = [
    "執行時間", "地區", "期別", "作業", "引擎", "結果",
    "IMPORTRANGE筆數", "Python筆數", "差異格數", "差異摘要", "備註",
]

_CACHE_TTL_SECONDS = 60
_cache: dict = {"at": 0.0, "map": None}


# ──────────────────────────────────────────────────────────────
# 引擎設定
# ──────────────────────────────────────────────────────────────

def parse_engine_settings(rows: Sequence[Sequence]) -> dict:
    """「匯入引擎設定」分頁的列 → {作業: 引擎}。第 1 列為標題；不認得的值略過。"""
    result = {}
    for row in list(rows)[1:]:
        if not row:
            continue
        task = str(row[0] if len(row) > 0 else "").strip()
        engine = str(row[1] if len(row) > 1 else "").strip().upper()
        if task and engine in ENGINES:
            result[task] = engine
    return result


def _load_settings() -> dict:
    now = time.time()
    if _cache["map"] is not None and now - _cache["at"] < _CACHE_TTL_SECONDS:
        return _cache["map"]
    from modules.auth import open_spreadsheet
    from modules.master_sheet import MASTER_SHEET_ID

    ss = open_spreadsheet(MASTER_SHEET_ID)
    try:
        ws = ss.worksheet(SETTINGS_SHEET)
    except Exception:
        ws = ss.add_worksheet(title=SETTINGS_SHEET, rows=50, cols=len(SETTINGS_HEADER))
        ws.update("A1", [SETTINGS_HEADER, ["01專員請款", DEFAULT_ENGINE, "IMPORTRANGE／COMPARE／PYTHON"]])
    mapping = parse_engine_settings(ws.get_all_values())
    _cache.update(at=now, map=mapping)
    return mapping


def get_engine(task_key: str, log: List[str] | None = None) -> str:
    """取得作業的匯入引擎；任何錯誤都回傳預設 IMPORTRANGE。"""
    try:
        engine = _load_settings().get(task_key, DEFAULT_ENGINE)
    except Exception as e:
        if log is not None:
            log.append(f"    ⚠️ 讀取「{SETTINGS_SHEET}」失敗，使用 {DEFAULT_ENGINE}：{e}")
        return DEFAULT_ENGINE
    return engine


# ──────────────────────────────────────────────────────────────
# 篩選（對齊 Sheets FILTER 的 = 比較）
# ──────────────────────────────────────────────────────────────

def _sheets_equal(cell, target) -> bool:
    """Sheets 的 = 比較文字時不分大小寫；數字以數值比較。"""
    if isinstance(cell, bool) or isinstance(target, bool):
        return cell == target
    if isinstance(cell, (int, float)) and isinstance(target, (int, float)):
        return float(cell) == float(target)
    if isinstance(cell, (int, float)) != isinstance(target, (int, float)):
        return False
    return str(cell).casefold() == str(target).casefold()


def pad_rows(rows: Iterable[Sequence], width: int) -> List[list]:
    out = []
    for row in rows:
        r = list(row)[:width]
        r += [""] * (width - len(r))
        out.append(r)
    return out


def filter_rows(rows: Iterable[Sequence], key_index: int, value, width: int) -> List[list]:
    """回傳 key_index 欄等於 value 的列（補齊到 width 欄），順序與來源相同。"""
    padded = pad_rows(rows, width)
    return [r for r in padded if _sheets_equal(r[key_index], value)]


# ──────────────────────────────────────────────────────────────
# 比對
# ──────────────────────────────────────────────────────────────

def _norm(value):
    if value is None:
        return ""
    if isinstance(value, bool):
        return value
    if isinstance(value, (int, float)):
        f = float(value)
        return int(f) if f.is_integer() else round(f, 9)
    return str(value)


def compare_blocks(expected: Sequence[Sequence], actual: Sequence[Sequence], width: int) -> List[tuple]:
    """逐格比對兩個區塊，回傳差異清單 [(第幾列(1起), 第幾欄(1起), 期望值, 實際值), ...]。"""
    exp = pad_rows(expected, width)
    act = pad_rows(actual, width)
    n = max(len(exp), len(act))
    blank = [""] * width
    diffs = []
    for i in range(n):
        e_row = exp[i] if i < len(exp) else blank
        a_row = act[i] if i < len(act) else blank
        for j in range(width):
            if _norm(e_row[j]) != _norm(a_row[j]):
                diffs.append((i + 1, j + 1, e_row[j], a_row[j]))
    return diffs


def summarize_diffs(diffs: Sequence[tuple], start_row: int, limit: int = 5) -> str:
    def col(n):
        s = ""
        while n > 0:
            n, r = divmod(n - 1, 26)
            s = chr(65 + r) + s
        return s

    parts = [
        f"{col(c)}{start_row + r - 1}: IMPORTRANGE={e!r} / Python={a!r}"
        for r, c, e, a in list(diffs)[:limit]
    ]
    if len(diffs) > limit:
        parts.append(f"…共 {len(diffs)} 格")
    return "；".join(parts)


# ──────────────────────────────────────────────────────────────
# 寫入靜態值（PYTHON 引擎）
# ──────────────────────────────────────────────────────────────

def write_static_block(ws, start_row: int, rows: List[list], width: int, log: List[str]) -> int:
    """把 rows 以 RAW 寫入 A{start_row} 起，寫完讀回驗證一致且無公式。回傳列數。"""
    if not rows:
        return 0

    def col(n):
        s = ""
        while n > 0:
            n, r = divmod(n - 1, 26)
            s = chr(65 + r) + s
        return s

    end_row = start_row + len(rows) - 1
    rng = f"A{start_row}:{col(width)}{end_row}"
    ws.update(rng, rows, value_input_option="RAW")

    written = ws.get(rng, value_render_option="UNFORMATTED_VALUE") or []
    diffs = compare_blocks(rows, written, width)
    if diffs:
        raise RuntimeError(f"Python 寫入後讀回不一致：{summarize_diffs(diffs, start_row)}")
    formulas = ws.get(rng, value_render_option="FORMULA") or []
    if any(str(v or "").lstrip().startswith("=") for r in formulas for v in r):
        raise RuntimeError("Python 寫入後仍有公式，已停止後續流程")
    log.append(f"    ✅ Python 寫入 {rng} 並驗證完成（{len(rows)} 列）")
    return len(rows)


# ──────────────────────────────────────────────────────────────
# 稽核紀錄
# ──────────────────────────────────────────────────────────────

def append_engine_log(
    region: str, period: str, task_key: str, engine: str, result: str,
    importrange_count="", python_count="", diff_count="", diff_summary="", note="",
    log: List[str] | None = None,
) -> None:
    """寫入主控試算表「匯入引擎Log」；失敗只記在日誌，不中斷流程。"""
    try:
        from modules.auth import open_spreadsheet
        from modules.master_sheet import MASTER_SHEET_ID, _get_or_create_log_sheet

        ss = open_spreadsheet(MASTER_SHEET_ID)
        sheet = _get_or_create_log_sheet(ss, LOG_SHEET, LOG_HEADER)
        row = [
            datetime.now(TAIPEI_TZ).strftime("%Y/%m/%d %H:%M:%S"),
            region, period, task_key, engine, result,
            importrange_count, python_count, diff_count, diff_summary, note,
        ]
        sheet.append_row(row, value_input_option="RAW")
    except Exception as e:
        if log is not None:
            log.append(f"    ⚠️ 「{LOG_SHEET}」寫入失敗：{e}")
