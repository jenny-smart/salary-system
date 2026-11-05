"""每期別作業盤點：金流對帳／清潔承攬／其他承攬／承攬作業。

資料來源是主控表「salary-system執行Log-{年度}」（modules.unified_log）：
salaryapp 每執行完一項功能都會記一筆（系統、功能、區域、期別、結果），
這裡依「期別＋地區＋系統＋功能」取最後一筆結果，判斷是否已完成。

另外也採計「打卡」：主控表各地區工作表（record_execution／GAS recordExecution 寫入）
在本期欄有完成時間，就算該功能已完成。GAS 執行、或統一 Log 上線前跑過的期別都靠這個判斷。
功能對應的打卡作業名稱見 PUNCH_KEYS；沒列的功能預設用去掉編號的功能名稱。
"""
from __future__ import annotations

from dataclasses import dataclass

# 不適合批次執行的功能：仍會顯示在盤點表，但不能勾選一併執行
NOT_BATCHABLE = {
    "⑩ 一鍵執行": "尚未實作",
    "④ 寄出承攬費通知信": "寄信需確認後單獨執行",
    "⑥ 寄出工具包押金通知信": "寄信需確認後單獨執行",
}

# 功能完成的替代判斷：功能 → 打卡（record_execution）作業名稱（任一個有打卡即算完成）
_OTHER_SERVICES = ("水洗", "家電", "收納", "座椅", "地毯")
ALIASES = {
    ("💰 金流對帳", "① 建立期別資料夾與檔案（手動）"): ("排程期別資料夾", "手動期別資料夾"),
    ("💰 金流對帳", "② 期別訂單轉檔（xls/xlsx → Google Sheet）"): ("期別訂單轉檔",),
    ("💰 金流對帳", "③ 期別訂單搬運"): ("複製期別訂單",),
    ("💰 金流對帳", "④ 期別訂單加工"): ("加工-排序",),
    ("💰 金流對帳", "⑤ 期別訂單分類"): ("複製清潔訂單列數",),
    ("💰 金流對帳", "⑥ 金流對帳轉檔（zip/csv/xlsx → Google Sheet）"): ("期別發票轉檔", "期別預收轉檔"),
    ("💰 金流對帳", "⑦ 搬運退款＋預收"): ("複製預收", "複製已退款全部加收"),
    ("💰 金流對帳", "⑧ 搬運發票＋藍新"): ("複製發票",),
    ("📦 其他承攬", "① 執行前置作業"): tuple(f"{s}前置" for s in _OTHER_SERVICES),
    ("📦 其他承攬", "② 執行全部結算"): tuple(f"{s}結算" for s in _OTHER_SERVICES),
    ("📦 其他承攬", "③ 產出全部PDF"): tuple(f"{s}薪資單" for s in _OTHER_SERVICES),
    ("📨 承攬作業", "① 承攬費通知信"): ("承攬費通知信", "清潔承攬mail"),
}
# 沒有打卡、只能看 Python 執行紀錄的功能（避免功能名稱剛好等於別的打卡項目）
NO_PUNCH = {"⑨ 搬運ATM", "⑩ 金流對帳彙總＋檢核", "⑩ 一鍵執行", "⑮ 產生PDF", "⑯ 產生專案PDF"}


def punch_keys(system: str, func: str) -> tuple[str, ...]:
    if (system, func) in ALIASES:
        return ALIASES[(system, func)]
    if func in NO_PUNCH:
        return ()
    return (func.split(" ", 1)[-1].strip(),)


def _norm_time(text: str) -> str:
    return str(text or "").strip().replace("/", "-")

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
                    function_map: dict[str, list[str]],
                    punches: dict[str, dict[str, str]] | None = None) -> dict:
    """rows 為統一 Log 的資料列（不含標題，欄位順序同 unified_log.HEADERS）。
    punches 為 {地區: {打卡作業名稱: 完成時間}}（主控表地區工作表本期欄）。

    回傳 {(area, system, function): Status}。
    """
    period = str(period or "").strip()
    wanted_areas = set(areas)
    alias_index = {}
    for system, funcs in function_map.items():
        for func in funcs:
            for key in punch_keys(system, func):
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

    # 主控表打卡：沒紀錄、或打卡時間晚於最後一次失敗，都算完成
    for area, times in (punches or {}).items():
        if area not in wanted_areas:
            continue
        for key, punched_at in times.items():
            if not str(punched_at).strip():
                continue
            for system, func in alias_index.get(key, []):
                current = result[(area, system, func)]
                if current.state == NONE or (
                        current.state == FAILED and _norm_time(punched_at) > _norm_time(current.time)):
                    result[(area, system, func)] = Status(DONE, _norm_time(punched_at), f"打卡：{key}")
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


def load_punch_times(period: str, areas: list[str], spreadsheet=None) -> dict[str, dict[str, str]]:
    """讀主控表各地區工作表：A 欄作業名稱，第 1 列期別欄（ID/筆數），右邊一欄是完成時間。"""
    if spreadsheet is None:
        from modules.auth import open_spreadsheet
        from modules.master_sheet import MASTER_SHEET_ID

        spreadsheet = open_spreadsheet(MASTER_SHEET_ID)
    out: dict[str, dict[str, str]] = {}
    for area in areas:
        try:
            values = spreadsheet.worksheet(area).get_all_values()
        except Exception:
            continue
        if not values:
            continue
        header = [str(v).strip() for v in values[0]]
        if str(period).strip() not in header:
            continue
        time_idx = header.index(str(period).strip()) + 1
        out[area] = {
            str(row[0]).strip(): str(row[time_idx]).strip()
            for row in values[1:]
            if row and str(row[0]).strip() and time_idx < len(row) and str(row[time_idx]).strip()
        }
    return out
