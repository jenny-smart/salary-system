"""每期別作業盤點：金流對帳／清潔承攬／其他承攬／承攬作業。

資料來源是主控表「salary-system執行Log-{年度}」（modules.unified_log）：
salaryapp 每執行完一項功能都會記一筆（系統、功能、區域、期別、結果），
這裡依「期別＋地區＋系統＋功能」取最後一筆結果，判斷是否已完成。

建立期別資料夾另外接受排程打卡（function＝排程期別資料夾／手動期別資料夾）。
"""
from __future__ import annotations

from dataclasses import dataclass

# 不適合批次執行的功能：仍會顯示在盤點表，但不能勾選一併執行
NOT_BATCHABLE = {
    "⑩ 一鍵執行": "尚未實作",
}

# 功能完成的替代判斷：功能 → 打卡（record_execution）作業名稱
ALIASES = {
    ("💰 金流對帳", "① 建立期別資料夾與檔案（手動）"): ("排程期別資料夾", "手動期別資料夾"),
}

DONE, FAILED, NONE = "done", "failed", "none"


@dataclass
class Status:
    state: str = NONE
    time: str = ""
    message: str = ""


def log_years(period: str, now_year: int) -> list[int]:
    """期別所屬年度＋今年（跨年執行時 Log 會寫在新年度工作表）。"""
    years = []
    try:
        years.append(int(str(period)[:4]))
    except ValueError:
        pass
    if now_year not in years:
        years.append(now_year)
    return years


def build_inventory(rows: list[list[str]], period: str, areas: list[str],
                    function_map: dict[str, list[str]]) -> dict:
    """rows 為統一 Log 的資料列（不含標題，欄位順序同 unified_log.HEADERS）。

    回傳 {(area, system, function): Status}。
    """
    period = str(period or "").strip()
    wanted_areas = set(areas)
    alias_index = {}
    for (system, func), keys in ALIASES.items():
        for key in keys:
            alias_index.setdefault(key, []).append((system, func))

    result = {
        (area, system, func): Status()
        for area in areas for system, funcs in function_map.items() for func in funcs
    }

    def cell(row, idx):
        return str(row[idx]).strip() if idx < len(row) and row[idx] is not None else ""

    # 依執行時間排序，後面的覆蓋前面的
    for row in sorted(rows, key=lambda r: cell(r, 0)):
        ts, system, func, run_type, area, row_period = (cell(row, i) for i in range(6))
        status, message = cell(row, 8), cell(row, 9)
        if row_period != period or area not in wanted_areas:
            continue
        key = (area, system, func)
        if key in result and "Python" in run_type and status in ("成功", "失敗"):
            result[key] = Status(DONE if status == "成功" else FAILED, ts, message)
            continue
        for alias_system, alias_func in alias_index.get(func, []):
            alias_key = (area, alias_system, alias_func)
            if alias_key in result and status == "成功" and result[alias_key].state != DONE:
                result[alias_key] = Status(DONE, ts, f"打卡：{func}")
    return result


def build_queue(selected: list[tuple[str, str, str]], areas: list[str],
                function_map: dict[str, list[str]], engine: str = "PYTHON",
                period: str = "") -> list[dict]:
    """把勾選的 (area, system, function) 排成執行佇列。

    順序：地區 → 系統（金流→清潔→其他→承攬作業）→ 功能編號，
    同一地區內維持原本的前後相依順序。
    """
    chosen = set(selected)
    queue = []
    for area in areas:
        for system, funcs in function_map.items():
            for func in funcs:
                if (area, system, func) in chosen:
                    queue.append({"area": area, "system": system, "function": func,
                                  "engine": engine, "period": period})
    return queue


def drop_area(queue: list[dict], area: str) -> tuple[list[dict], int]:
    """某地區失敗時，移除該地區剩餘項目（其他地區照常執行）。"""
    kept = [item for item in queue if item.get("area") != area]
    return kept, len(queue) - len(kept)


def load_log_rows(period: str, now_year: int, spreadsheet=None) -> list[list[str]]:
    """讀取統一 Log（期別年度＋今年），找不到工作表就略過。"""
    from modules import unified_log

    if spreadsheet is None:
        import os
        from modules.auth import open_spreadsheet

        spreadsheet = open_spreadsheet(
            os.getenv("MASTER_SPREADSHEET_ID", "").strip() or unified_log.MASTER_SPREADSHEET_ID
        )
    rows: list[list[str]] = []
    for year in log_years(period, now_year):
        try:
            ws = spreadsheet.worksheet(unified_log.sheet_title(year))
        except Exception:
            continue
        rows.extend(ws.get_all_values()[1:])
    return rows
