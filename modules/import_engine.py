"""
匯入引擎切換與稽核
檔案：modules/import_engine.py

目的：
    逐步把「寫 IMPORTRANGE → 等載入 → 轉靜態值」改成「Python 直接讀來源 → 篩選 → 寫靜態值」，
    但隨時可以切回 IMPORTRANGE，且每次執行都有紀錄可以稽核。

匯入方式在網頁「執行設定」選擇（預設 IMPORTRANGE，選 PYTHON 時本次執行的 00／01／02 都用 Python）；
選 PYTHON 時，每次寫入都記到主控試算表（LemonSalarySystem）「Python匯入-{期別}」分頁，
並在執行日誌顯示該分頁與寫入位置的連結供確認。
排程等沒有經過網頁的執行，沿用「匯入引擎設定」分頁（不存在就是 IMPORTRANGE，不會自動建立）。

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
from typing import Callable, Iterable, List, Sequence

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

REPORT_SHEET_PREFIX = "Python匯入-"
REPORT_HEADER = [
    "執行時間", "地區", "作業", "匯入方式", "結果", "IMPORTRANGE列數", "Python列數",
    "差異格數", "差異摘要", "寫入位置", "連結",
]

_CACHE_TTL_SECONDS = 60
_cache: dict = {"at": 0.0, "map": None}
_override: dict = {"engine": None}


def set_engine_override(engine: str | None) -> None:
    """網頁選的匯入方式；設定後優先於「匯入引擎設定」分頁。None 表示不覆寫。"""
    value = str(engine or "").strip().upper()
    _override["engine"] = value if value in ENGINES else None


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
        mapping = {}
    else:
        mapping = parse_engine_settings(ws.get_all_values())
    _cache.update(at=now, map=mapping)
    return mapping


def get_engine(task_key: str, log: List[str] | None = None) -> str:
    """取得作業的匯入引擎：網頁選擇 > 「匯入引擎設定」分頁 > IMPORTRANGE；任何錯誤都回傳 IMPORTRANGE。"""
    if _override["engine"]:
        return _override["engine"]
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


_ERROR_TOKENS = (
    "#REF!", "#N/A", "#VALUE!", "#ERROR!", "#NAME?", "#NUM!", "#DIV/0!", "LOADING",
)


def is_sheet_error(value) -> bool:
    """與原本 _wait_and_convert 相同的錯誤判定。"""
    text = str(value or "").strip().upper()
    return any(token in text for token in _ERROR_TOKENS)


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


def col_letter(n: int) -> str:
    """欄號（1-based）→ 欄字母，如 19 → 'S'。"""
    s = ""
    while n > 0:
        n, r = divmod(n - 1, 26)
        s = chr(65 + r) + s
    return s


def summarize_diffs(diffs: Sequence[tuple], start_row: int, limit: int = 5, start_col: int = 1) -> str:
    parts = [
        f"{col_letter(start_col + c - 1)}{start_row + r - 1}: IMPORTRANGE={e!r} / Python={a!r}"
        for r, c, e, a in list(diffs)[:limit]
    ]
    if len(diffs) > limit:
        parts.append(f"…共 {len(diffs)} 格")
    return "；".join(parts)


def block_range(start_row: int, n_rows: int, width: int, start_col: int = 1) -> str:
    return (
        f"{col_letter(start_col)}{start_row}:"
        f"{col_letter(start_col + width - 1)}{start_row + n_rows - 1}"
    )


# ──────────────────────────────────────────────────────────────
# 寫入靜態值（PYTHON 引擎）
# ──────────────────────────────────────────────────────────────

def write_static_block(
    ws, start_row: int, rows: List[list], width: int, log: List[str], start_col: int = 1,
) -> int:
    """把 rows 以 RAW 寫入（預設 A 欄起），寫完讀回驗證一致且無公式。回傳列數。"""
    if not rows:
        return 0
    rows = pad_rows(rows, width)
    rng = block_range(start_row, len(rows), width, start_col)
    ws.update(rng, rows, value_input_option="RAW")

    written = ws.get(rng, value_render_option="UNFORMATTED_VALUE") or []
    diffs = compare_blocks(rows, written, width)
    if diffs:
        raise RuntimeError(
            f"Python 寫入後讀回不一致：{summarize_diffs(diffs, start_row, start_col=start_col)}"
        )
    formulas = ws.get(rng, value_render_option="FORMULA") or []
    if any(str(v or "").lstrip().startswith("=") for r in formulas for v in r):
        raise RuntimeError("Python 寫入後仍有公式，已停止後續流程")
    log.append(f"    ✅ Python 寫入 {rng} 並驗證完成（{len(rows)} 列）")
    return len(rows)


# ──────────────────────────────────────────────────────────────
# 共用切換流程
# ──────────────────────────────────────────────────────────────

def run_import(
    task_key: str, region: str, period: str, log: List[str], *,
    ws, start_row: int, width: int, start_col: int = 1,
    importrange: Callable[[], int],
    python_rows: Callable[[], List[list]],
    python_write: Callable[[List[list]], int] | None = None,
) -> int:
    """
    依「匯入引擎設定」執行匯入，回傳本次匯入列數。
      importrange()   原本做法（寫公式→等載入→轉靜態值），回傳列數
      python_rows()   Python 讀來源並算出要寫入的區塊（不寫入）
      python_write()  PYTHON 模式的寫入；預設寫到 start_row／start_col 並驗證
    """
    engine = get_engine(task_key, log)
    log.append(f"    匯入引擎：{engine}")

    if engine == ENGINE_PYTHON:
        try:
            rows = python_rows()
        except Exception as e:
            log.append(f"    ⚠️ Python 讀取來源失敗，改用 IMPORTRANGE：{e}")
            num_rows = importrange()
            append_engine_log(
                region, period, task_key, engine, "退回IMPORTRANGE",
                importrange_count=num_rows, note=f"Python 讀取失敗：{e}", log=log,
            )
            append_report(
                region, period, task_key, engine, "❌ Python失敗，已改用IMPORTRANGE",
                ws, start_row, start_col, importrange_count=num_rows,
                diff_summary=f"Python 讀取失敗：{e}", log=log,
            )
            return num_rows
        if python_write is not None:
            num_rows = python_write(rows)
        else:
            num_rows = write_static_block(ws, start_row, rows, width, log, start_col)
        append_engine_log(
            region, period, task_key, engine, "完成", python_count=num_rows,
            note=f"寫入 {col_letter(start_col)}{start_row} 起", log=log,
        )
        append_report(
            region, period, task_key, engine, "✅ Python寫入完成（讀回一致）",
            ws, start_row, start_col, python_count=num_rows, log=log,
        )
        return num_rows

    num_rows = importrange()

    if engine == ENGINE_COMPARE:
        # 稽核：以 IMPORTRANGE 結果為準，另以 Python 算一次逐格比對；比對失敗不影響流程。
        try:
            py_rows = python_rows()
            n = max(num_rows, len(py_rows), 1)
            actual = ws.get(
                block_range(start_row, n, width, start_col),
                value_render_option="UNFORMATTED_VALUE",
            ) or []
            actual = actual[:num_rows]
            diffs = compare_blocks(actual, py_rows, width)
            result = "一致" if not diffs else "不一致"
            log.append(
                f"    比對結果：{result}（IMPORTRANGE {num_rows} 列／Python {len(py_rows)} 列，"
                f"差異 {len(diffs)} 格）"
            )
            append_engine_log(
                region, period, task_key, engine, result,
                importrange_count=num_rows, python_count=len(py_rows),
                diff_count=len(diffs),
                diff_summary=summarize_diffs(diffs, start_row, start_col=start_col),
                log=log,
            )
            append_report(
                region, period, task_key, engine,
                "✅ 一致，可切換 PYTHON" if not diffs else "❌ 不一致，先不要切換",
                ws, start_row, start_col, importrange_count=num_rows,
                python_count=len(py_rows), diff_count=len(diffs),
                diff_summary=summarize_diffs(diffs, start_row, start_col=start_col), log=log,
            )
        except Exception as e:
            log.append(f"    ⚠️ 比對失敗（不影響匯入結果）：{e}")
            append_engine_log(
                region, period, task_key, engine, "比對失敗",
                importrange_count=num_rows, note=str(e), log=log,
            )
            append_report(
                region, period, task_key, engine, "❌ Python比對失敗，先不要切換",
                ws, start_row, start_col, importrange_count=num_rows,
                diff_summary=str(e), log=log,
            )
    return num_rows


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


def sheet_url(spreadsheet_id: str, gid=None, cell: str = "") -> str:
    url = f"https://docs.google.com/spreadsheets/d/{spreadsheet_id}/edit"
    if gid is not None:
        url += f"#gid={gid}" + (f"&range={cell}" if cell else "")
    return url


def append_report(
    region: str, period: str, task_key: str, engine: str, result: str,
    ws, start_row: int, start_col: int,
    importrange_count="", python_count="", diff_count="", diff_summary="",
    log: List[str] | None = None,
) -> str:
    """
    試跑比對／PYTHON 每次匯入都記到主控試算表「Python匯入-{期別}」分頁（不存在自動新增），
    並在日誌顯示結果、寫入位置連結與紀錄分頁連結。失敗只記日誌，不中斷流程。
    """
    label = "試跑比對" if engine == ENGINE_COMPARE else engine
    if log is not None:
        log.append(f"    {result}（{label}）")
    try:
        from modules.auth import open_spreadsheet
        from modules.master_sheet import MASTER_SHEET_ID, _get_or_create_log_sheet

        cell = f"{col_letter(start_col)}{start_row}"
        target_url = sheet_url(ws.spreadsheet.id, ws.id, cell)
        ss = open_spreadsheet(MASTER_SHEET_ID)
        report = _get_or_create_log_sheet(ss, f"{REPORT_SHEET_PREFIX}{period}", REPORT_HEADER)
        report.append_row([
            datetime.now(TAIPEI_TZ).strftime("%Y/%m/%d %H:%M:%S"),
            region, task_key, label, result, importrange_count, python_count,
            diff_count, diff_summary, f"{ws.title}!{cell} 起",
            f'=HYPERLINK("{target_url}","開啟{ws.title}")',
        ], value_input_option="USER_ENTERED")
        report_url = sheet_url(MASTER_SHEET_ID, report.id)
        if log is not None:
            log.append(f"    🔗 寫入位置：{target_url}")
            log.append(f"    🔗 本期匯入紀錄：{report_url}")
        return report_url
    except Exception as e:
        if log is not None:
            log.append(f"    ⚠️ 「{REPORT_SHEET_PREFIX}{period}」寫入失敗（不影響匯入結果）：{e}")
        return ""
