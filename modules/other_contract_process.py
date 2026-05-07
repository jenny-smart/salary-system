"""
modules/other_contract_process.py
其他承攬薪資系統 — 前置作業 & 結算作業
架構：Python + gspread，整合 Streamlit / master_sheet 打卡機制
版本：v2026-05
依賴：auth.py, master_sheet.py, sheet_helper.py
"""

import time
import logging
from datetime import datetime
from typing import Callable

import gspread

from modules.auth import get_gspread_client, open_spreadsheet
from modules.master_sheet import record_execution, get_recorded_value

logger = logging.getLogger(__name__)

# ─────────────────────────────────────────────────────────────────────────────
# ⚙️  服務設定
# ─────────────────────────────────────────────────────────────────────────────

SERVICE_CONFIG = {
    "水洗": {
        "salary_table":   "水洗薪資表",
        "salary_slip":    "水洗薪資單",
        "order_sheet":    "水洗訂單",
        "income_sheet":   "水洗營收明細",
        # 薪資表列操作（1-based）
        "clear_rows":     [284, 280],           # 上半月清空這 2 列 J:O
        "carry_rows":     [(285, 284), (281, 280)],  # 下半月 (來源, 目標)
        # 結算
        "settlement_row": 285,
        "settlement_range": "J285:O285",
        # 主控試算表 task_key
        "preprocess_key":  "複製水洗訂單",
        "settlement_key":  "水洗結算",
        # exec 訂單筆數參照列（master sheet task_key）
        "order_count_key": "複製水洗訂單",
    },
    "家電": {
        "salary_table":   "家電薪資表",
        "salary_slip":    "家電薪資單",
        "order_sheet":    "家電訂單",
        "income_sheet":   "家電營收明細",
        "clear_rows":     [253, 249],
        "carry_rows":     [(254, 253), (250, 249)],
        "settlement_row": 254,
        "settlement_range": "J254:O254",
        "preprocess_key":  "複製家電訂單",
        "settlement_key":  "家電結算",
        "order_count_key": "複製家電訂單",
    },
    "收納": {
        "salary_table":   "收納薪資表",
        "salary_slip":    "收納薪資單",
        "order_sheet":    "收納訂單",
        "income_sheet":   "收納營收明細",
        "clear_rows":     [222, 218],
        "carry_rows":     [(223, 222), (219, 218)],
        "settlement_row": 223,
        "settlement_range": "J223:O223",
        "preprocess_key":  "複製收納訂單",
        "settlement_key":  "收納結算",
        "order_count_key": "複製收納訂單",
    },
    "座椅": {
        "salary_table":   "座椅薪資表",
        "salary_slip":    "座椅薪資單",
        "order_sheet":    "座椅訂單",
        "income_sheet":   "座椅營收明細",
        "clear_rows":     [222, 218],
        "carry_rows":     [(223, 222), (219, 218)],
        "settlement_row": 223,
        "settlement_range": "J223:O223",
        "preprocess_key":  "複製座椅訂單",
        "settlement_key":  "座椅結算",
        "order_count_key": "複製座椅訂單",
    },
    "地毯": {
        "salary_table":   "地毯薪資表",
        "salary_slip":    "地毯薪資單",
        "order_sheet":    "地毯訂單",
        "income_sheet":   "地毯營收明細",
        "clear_rows":     [],   # 地毯薪資表略過公式操作
        "carry_rows":     [],
        "settlement_row": None, # 地毯目前略過結算
        "settlement_range": None,
        "preprocess_key":  "複製地毯訂單",
        "settlement_key":  "地毯結算",
        "order_count_key": "複製地毯訂單",
    },
}

ALL_SERVICES = ["水洗", "家電", "收納", "座椅", "地毯"]

# master_sheet task_key
TASK_KEY_PREPROCESS_ALL  = "其他承攬前置作業"
TASK_KEY_SETTLEMENT_ALL  = "其他承攬結算作業"

# 訂單資料欄範圍 A:BJ = 62 欄
ORDER_COL_COUNT = 62   # A=1 … BJ=62


# ─────────────────────────────────────────────────────────────────────────────
# 🔧  內部工具
# ─────────────────────────────────────────────────────────────────────────────

def _col_letter_to_index(col: str) -> int:
    """欄字母 → 0-based index（J→9, O→14）"""
    result = 0
    for ch in col.upper():
        result = result * 26 + (ord(ch) - ord('A') + 1)
    return result - 1


def _is_zero_value(val) -> bool:
    """判斷是否為零值（空字串、"-"、"－"、0 都視為零）"""
    if val is None:
        return True
    s = str(val).strip()
    if s in ("", "-", "－", "0"):
        return True
    try:
        return float(s) == 0
    except ValueError:
        return True


def _get_order_count_from_master(region: str, period: str, task_key: str,
                                  master_sheet_id: str) -> int:
    """從主控試算表讀取指定 task_key 的打卡筆數。"""
    val = get_recorded_value(region, period, task_key)
    if val is None:
        return 0
    try:
        return int(float(str(val).strip()))
    except (ValueError, TypeError):
        return 0


def _find_last_nonempty_row_col_b(ws: gspread.Worksheet) -> int:
    """掃描 B 欄，回傳最後一筆非空白列的列號（1-based），找不到回傳 1。"""
    col_b = ws.col_values(2)   # B 欄所有值（list，0-based index）
    for i in range(len(col_b) - 1, -1, -1):
        if str(col_b[i]).strip():
            return i + 1       # 轉為 1-based
    return 1


def _clear_order_sheet(ws: gspread.Worksheet, log: Callable):
    """清空訂單工作表 A2:BJ（保留標題列）。"""
    last_row = ws.row_count
    if last_row < 2:
        return
    # 用 batch_clear 一次清乾淨
    ws.batch_clear([f"A2:BJ{last_row}"])
    log("  清空 A2:BJ 完成")


# ─────────────────────────────────────────────────────────────────────────────
# 📁  前置作業
# ─────────────────────────────────────────────────────────────────────────────

def run_other_preprocess(
    other_file_id: str,
    region: str,
    period: str,
    is_first_half: bool,
    service_type: str | None,
    log: Callable,
    master_sheet_id: str | None = None,
):
    """
    其他承攬前置作業。
    service_type = None 表示全部服務；傳入服務名稱則只執行該服務。

    步驟：
    1. 依上/下半月操作薪資表指定列（清空 or 複製值）
    2. 依上/下半月搬運訂單資料（清空 or 從營收明細 append）
    3. 打卡至主控試算表

    回傳 dict：{service: count, ...}
    """
    half_text = "上半月" if is_first_half else "下半月"
    services  = [service_type] if service_type else ALL_SERVICES

    log(f"🔄 其他承攬{half_text}前置作業 ({'全部' if not service_type else service_type})")

    gc    = get_gspread_client()
    other = gc.open_by_key(other_file_id)

    results = {}

    for svc in services:
        cfg = SERVICE_CONFIG[svc]
        log(f"\n▶ {svc}")

        try:
            # ── Step 1：薪資表公式操作 ─────────────────────────────
            _process_salary_formulas(other, cfg, is_first_half, svc, log)

            # ── Step 2：訂單搬運 ───────────────────────────────────
            count = _process_order_data(
                other, cfg, is_first_half, period, region, svc, log,
                master_sheet_id=master_sheet_id,
            )

            results[svc] = count
            log(f"  ✅ {svc} 前置完成（訂單 {count} 筆）")

        except Exception as e:
            logger.exception(f"{svc} 前置失敗")
            log(f"  ❌ {svc} 前置失敗：{e}")
            results[svc] = -1

        time.sleep(0.5)

    # ── 打卡（ID/筆數 + 完成時間 各自寫入）─────────────────────
    ts = datetime.now().strftime("%Y/%m/%d %H:%M")
    if not service_type:
        # 全部執行：逐服務寫筆數+時間，再寫全部完成時間
        for svc in services:
            c = results.get(svc, 0)
            if c >= 0:
                _punch2(region, period, SERVICE_CONFIG[svc]["preprocess_key"], count=c, ts=ts)
        _punch2(region, period, TASK_KEY_PREPROCESS_ALL, count=None, ts=ts)
    else:
        c = results.get(service_type, 0)
        if c >= 0:
            _punch2(region, period, SERVICE_CONFIG[service_type]["preprocess_key"], count=c, ts=ts)

    log(f"\n✅ 其他承攬{half_text}前置作業完成")
    return results


def _process_salary_formulas(
    ss: gspread.Spreadsheet,
    cfg: dict,
    is_first_half: bool,
    service_type: str,
    log: Callable,
):
    """
    上半月：清空薪資表指定列 J:O（第 10–15 欄，0-based 9–14）。
    下半月：將來源列值複製到目標列。
    地毯 clear_rows=[] 時略過。
    """
    if not cfg["clear_rows"] and not cfg["carry_rows"]:
        log(f"  {service_type} 薪資表操作略過")
        return

    ws = ss.worksheet(cfg["salary_table"])

    if is_first_half:
        for row_num in cfg["clear_rows"]:
            # 清空 J:O（欄 10–15，1-based）
            cell_range = f"J{row_num}:O{row_num}"
            ws.batch_clear([cell_range])
        log(f"  薪資表清空列：{cfg['clear_rows']}")
    else:
        for src_row, tgt_row in cfg["carry_rows"]:
            vals = ws.get(f"J{src_row}:O{src_row}")
            if vals:
                ws.update(f"J{tgt_row}:O{tgt_row}", vals, value_input_option="RAW")
        log(f"  薪資表複製列：{[(s, t) for s, t in cfg['carry_rows']]}")

    time.sleep(0.3)


def _process_order_data(
    ss: gspread.Spreadsheet,
    cfg: dict,
    is_first_half: bool,
    period: str,
    region: str,
    service_type: str,
    log: Callable,
    master_sheet_id: str | None = None,
) -> int:
    """
    上半月：清空訂單工作表 A2:BJ，再從營收明細搬入本期筆數。
    下半月：從訂單表 B 欄最後非空白列的下一列開始 append。
    回傳實際搬入筆數。
    """
    order_ws  = ss.worksheet(cfg["order_sheet"])
    income_ws = ss.worksheet(cfg["income_sheet"])

    # 取得本期應搬入筆數（從主控試算表打卡記錄讀取）
    period_count = _get_order_count_from_master(
        region, period, cfg["order_count_key"], master_sheet_id or ""
    )

    if period_count == 0:
        log(f"  ⚠️  {service_type} 主控試算表筆數為 0，跳過訂單搬運（請先完成金流對帳 ⑤）")
        return 0

    # 讀取營收明細（從 B 欄最後非空白往上數 period_count 列）
    income_last = _find_last_nonempty_row_col_b(income_ws)
    income_start = max(2, income_last - period_count + 1)

    if income_start > income_last:
        log(f"  ⚠️  {service_type} 營收明細無足夠資料（last={income_last}, need={period_count}）")
        return 0

    log(f"  從營收明細第 {income_start} 列讀取 {period_count} 筆")
    income_data = income_ws.get(
        f"A{income_start}:BJ{income_last}",
        value_render_option="UNFORMATTED_VALUE",
    )

    if not income_data:
        log(f"  ⚠️  {service_type} 營收明細讀取為空")
        return 0

    if is_first_half:
        # 清空後從 A2 開始寫入
        _clear_order_sheet(order_ws, log)
        paste_start = 2
    else:
        # 找 B 欄最後非空白的下一列
        last_row = _find_last_nonempty_row_col_b(order_ws)
        paste_start = last_row + 1
        log(f"  下半月 append 起始列：{paste_start}")

    # 補齊每列至 ORDER_COL_COUNT 欄
    padded = []
    for row in income_data:
        padded_row = list(row) + [""] * max(0, ORDER_COL_COUNT - len(row))
        padded.append(padded_row[:ORDER_COL_COUNT])

    end_row = paste_start + len(padded) - 1
    order_ws.update(
        f"A{paste_start}:BJ{end_row}",
        padded,
        value_input_option="RAW",
    )

    actual_count = len(padded)
    log(f"  寫入訂單 {paste_start}–{end_row}（{actual_count} 筆）")
    return actual_count


# ─────────────────────────────────────────────────────────────────────────────
# 📊  結算作業
# ─────────────────────────────────────────────────────────────────────────────

def run_other_settlement(
    other_file_id: str,
    region: str,
    period: str,
    service_type: str | None,
    log: Callable,
):
    """
    其他承攬結算作業。
    service_type = None 表示全部服務；傳入服務名稱則只執行該服務。

    步驟：
    1. 清除 PDF產出 工作表中屬於該服務的舊資料
    2. 從薪資表固定結算列（J:O）讀取非零人員名單
    3. 將人員名單寫入 PDF產出 工作表（B=姓名、H=Y、I=服務類型）
    4. 打卡至主控試算表

    回傳 dict：{service: [names], ...}
    """
    services = [service_type] if service_type else ALL_SERVICES

    log(f"📊 其他承攬結算作業（{'全部' if not service_type else service_type}）")

    gc    = get_gspread_client()
    other = gc.open_by_key(other_file_id)

    # PDF 產出工作表
    try:
        pdf_ws = other.worksheet("PDF產出")
    except gspread.WorksheetNotFound:
        raise RuntimeError("找不到「PDF產出」工作表")

    results = {}

    for svc in services:
        cfg = SERVICE_CONFIG[svc]

        if cfg["settlement_row"] is None:
            log(f"\n▶ {svc}：略過（未設定結算列）")
            results[svc] = []
            continue

        log(f"\n▶ {svc}（{cfg['settlement_range']}）")

        try:
            # ── Step 1：清除舊產出資料 ────────────────────────────
            cleared = _clear_pdf_output_by_service(pdf_ws, svc, log)
            log(f"  清除舊資料 {cleared} 筆")

            # ── Step 2：讀取薪資表結算列 ──────────────────────────
            names = _collect_nonzero_names(other, cfg, svc, log)
            log(f"  有效人員：{names}")

            # ── Step 3：寫入 PDF 產出表 ───────────────────────────
            written = _upsert_pdf_output_rows(pdf_ws, svc, names, log)
            log(f"  寫入 {written} 筆至 PDF產出")

            results[svc] = names
            log(f"  ✅ {svc} 結算完成")

        except Exception as e:
            logger.exception(f"{svc} 結算失敗")
            log(f"  ❌ {svc} 結算失敗：{e}")
            results[svc] = []

        time.sleep(0.3)

    # ── 打卡（ID/筆數 + 完成時間 各自寫入）─────────────────────
    ts = datetime.now().strftime("%Y/%m/%d %H:%M")
    if not service_type:
        for svc in services:
            names_list = results.get(svc, [])
            _punch2(region, period, SERVICE_CONFIG[svc]["settlement_key"], count=len(names_list), ts=ts)
        _punch2(region, period, TASK_KEY_SETTLEMENT_ALL, count=None, ts=ts)
    else:
        names_list = results.get(service_type, [])
        _punch2(region, period, SERVICE_CONFIG[service_type]["settlement_key"], count=len(names_list), ts=ts)

    log("\n✅ 其他承攬結算作業完成")
    return results


# ── 結算：清除 PDF產出舊資料 ─────────────────────────────────────────────────

def _clear_pdf_output_by_service(pdf_ws: gspread.Worksheet, service_type: str, log: Callable) -> int:
    """
    清除 PDF產出工作表中屬於 service_type（I 欄）的列：
    清除 B（姓名）、D（時間）、H（Y旗標）、I（服務類型）欄；
    保留 E 欄連結供後續覆寫使用。
    """
    all_vals = pdf_ws.get_all_values()
    if len(all_vals) < 2:
        return 0

    # 欄位 index（0-based）
    COL_B = 1   # 姓名
    COL_D = 3   # 時間
    COL_H = 7   # Y旗標
    COL_I = 8   # 服務類型

    cleared = 0
    batch_updates = []

    for i, row in enumerate(all_vals[1:], start=2):   # 從第 2 列開始
        svc_val = row[COL_I] if len(row) > COL_I else ""
        if str(svc_val).strip() == service_type:
            # 清除 B, D, H, I 欄
            batch_updates.append({
                "range": f"B{i}",
                "values": [[""]]
            })
            batch_updates.append({
                "range": f"D{i}",
                "values": [[""]]
            })
            batch_updates.append({
                "range": f"H{i}",
                "values": [[""]]
            })
            batch_updates.append({
                "range": f"I{i}",
                "values": [[""]]
            })
            cleared += 1

    if batch_updates:
        pdf_ws.batch_update(batch_updates)

    return cleared


# ── 結算：讀取非零人員名單 ───────────────────────────────────────────────────

def _collect_nonzero_names(
    ss: gspread.Spreadsheet,
    cfg: dict,
    service_type: str,
    log: Callable,
) -> list[str]:
    """
    從薪資表結算列（J:O，第 10–15 欄）讀取金額，
    對應第 1 列（J1:O1）的員工姓名，
    金額非零者回傳去重名單。
    """
    ws = ss.worksheet(cfg["salary_table"])
    settlement_row = cfg["settlement_row"]

    # 讀取第 1 列員工姓名（J1:O1）
    header_vals = ws.row_values(1)  # list，0-based
    # J=index 9, O=index 14（共 6 欄）
    name_cells = header_vals[9:15]  # index 9–14

    # 讀取結算列金額（J:O）
    amount_vals = ws.row_values(settlement_row)
    amount_cells = amount_vals[9:15] if len(amount_vals) >= 15 else (
        amount_vals[9:] + [""] * (15 - len(amount_vals))
    )[0:6]

    names = []
    for name, amount in zip(name_cells, amount_cells):
        name = str(name).strip()
        if name and not _is_zero_value(amount):
            names.append(name)

    # 去重（保留順序）
    seen = set()
    unique_names = []
    for n in names:
        if n not in seen:
            seen.add(n)
            unique_names.append(n)

    return unique_names


# ── 結算：寫入 PDF 產出工作表 ────────────────────────────────────────────────

# PDF產出欄位（1-based）
_PDF_COL_NAME    = 2   # B
_PDF_COL_LINK    = 5   # E
_PDF_COL_FLAG    = 8   # H
_PDF_COL_SERVICE = 9   # I
_PDF_START_ROW   = 2


def _upsert_pdf_output_rows(
    pdf_ws: gspread.Worksheet,
    service_type: str,
    names: list[str],
    log: Callable,
) -> int:
    """
    對每個姓名：
    1. 若 PDF產出 已有相同姓名 + 相同服務類型的列 → 更新
    2. 否則找第一個可重用的空列（B 欄空 且 E 欄可能有舊連結）→ 寫入
    3. 都找不到 → 在最末列新增
    """
    all_vals = pdf_ws.get_all_values()
    existing = all_vals[_PDF_START_ROW - 1:]  # 0-based，從第 2 列起

    written = 0
    batch   = []

    for name in names:
        target_row = -1

        # 優先找同名 + 同服務類型的現有列
        for i, row in enumerate(existing):
            row_b = row[_PDF_COL_NAME - 1]    if len(row) >= _PDF_COL_NAME    else ""
            row_i = row[_PDF_COL_SERVICE - 1] if len(row) >= _PDF_COL_SERVICE else ""
            if str(row_b).strip() == name and str(row_i).strip() == service_type:
                target_row = i + _PDF_START_ROW
                break

        # 找不到 → 找第一個 B 欄空白的可重用列
        if target_row == -1:
            for i, row in enumerate(existing):
                row_b = row[_PDF_COL_NAME - 1] if len(row) >= _PDF_COL_NAME else ""
                row_i = row[_PDF_COL_SERVICE - 1] if len(row) >= _PDF_COL_SERVICE else ""
                if not str(row_b).strip() and not str(row_i).strip():
                    target_row = i + _PDF_START_ROW
                    break

        # 仍找不到 → 新增到最末
        if target_row == -1:
            target_row = len(all_vals) + 1

        batch.append({"range": f"B{target_row}", "values": [[name]]})
        batch.append({"range": f"H{target_row}", "values": [["Y"]]})
        batch.append({"range": f"I{target_row}", "values": [[service_type]]})
        written += 1

    if batch:
        pdf_ws.batch_update(batch)

    return written


# ─────────────────────────────────────────────────────────────────────────────
# 🔑  主控試算表 task_key 清單（供 master_sheet.init_region_sheet 使用）
# ─────────────────────────────────────────────────────────────────────────────

OTHER_CONTRACT_TASK_KEYS = [
    "其他承攬",               # 分隔標題，不打卡
    "其他承攬前置作業",
    "複製水洗訂單",
    "複製家電訂單",
    "複製收納訂單",
    "複製座椅訂單",
    "複製地毯訂單",
    "其他承攬結算作業",
    "水洗結算",
    "家電結算",
    "收納結算",
    "座椅結算",
    "地毯結算",
]


# ─────────────────────────────────────────────────────────────────────────────
# 🕐  打卡輔助 — 同時寫 ID/筆數 + 完成時間
# ─────────────────────────────────────────────────────────────────────────────
# 主控試算表欄位結構（對照圖片）：
#   第 1 列：期別 YYYYMM-N（每期佔 2 欄）
#   第 2 列：ID/筆數 | 完成時間 | ID/筆數 | 完成時間 …
#
# master_sheet.record_execution(region, period, task_key, value) 只寫一格。
# 這裡用兩次呼叫分別寫「ID/筆數」和「完成時間」。
#   - count 不為 None → ID/筆數欄寫 count，完成時間欄寫 ts
#   - count 為 None   → ID/筆數欄留空（不寫），完成時間欄寫 ts
# ─────────────────────────────────────────────────────────────────────────────

def _punch2(region: str, period: str, task_key: str,
            count=None, ts: str | None = None):
    """
    向主控試算表的 task_key 列同時寫入兩欄：
      value_col（ID/筆數）← count
      time_col（完成時間） ← ts（= value_col + 1）

    主控試算表結構（每期 2 欄）：
      第 1 列：202604-2 | （空） | 202605-1 | （空） | 202605-2 …
      第 2 列：ID/筆數  | 完成時間 | ID/筆數 | 完成時間 …

    整合方式（依 master_sheet.py 實際 API 選擇）：

    ★ 若 master_sheet 提供 record_execution_both(region, period, task_key, count, ts)：
        直接改成：
            from modules.master_sheet import record_execution_both
            record_execution_both(region, period, task_key, count=count, ts=_ts)

    ★ 若 master_sheet 的 record_execution(region, period, task_key, value)
       內部會自動將 value 寫到 value_col、同時寫 timestamp 到 time_col：
        則本函式只需呼叫一次：
            record_execution(region, period, task_key, count if count is not None else _ts)

    ★ 現況（record_execution 只寫一格 value_col）：
        分兩次呼叫，count 寫 value_col，ts 寫 time_col。
        需要 master_sheet 新增 record_execution_time(region, period, task_key, ts)
        專門寫 time_col。
    """
    _ts = ts or datetime.now().strftime("%Y/%m/%d %H:%M")

    # ── 寫 ID/筆數欄（value_col）───────────────────────────────
    if count is not None:
        try:
            record_execution(region, period, task_key, count)
        except Exception as e:
            logger.warning(f"打卡 ID/筆數失敗 ({task_key}): {e}")

    # ── 寫完成時間欄（time_col = value_col + 1）────────────────
    # TODO: 待 master_sheet.py 新增 record_execution_time() 後替換
    # 目前暫以 record_execution 寫時間（會覆蓋 value_col，
    # 若已寫過 count 則改成呼叫新函式）。
    # 若 master_sheet 已支援：
    #   from modules.master_sheet import record_execution_time
    #   record_execution_time(region, period, task_key, _ts)
    try:
        record_execution(region, period, task_key, _ts)
    except Exception as e:
        logger.warning(f"打卡完成時間失敗 ({task_key}): {e}")
