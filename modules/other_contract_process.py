"""
modules/other_contract_process.py
其他承攬薪資系統 — 前置作業 / 結算作業 / PDF 產出
版本：v2026-05c
依賴：auth.py, master_sheet.py
"""

from __future__ import annotations

import io
import time
import logging
import datetime
import requests
import re
from typing import Callable, List

import gspread

from modules.auth import get_gspread_client, get_drive_service, get_credentials
from modules.master_sheet import record_execution, record_batch, get_recorded_values
from modules.period_utils import format_taipei_time

logger = logging.getLogger(__name__)

# ─────────────────────────────────────────────────────────────────────────────
# ⚙️  服務設定
# ─────────────────────────────────────────────────────────────────────────────

SERVICE_CONFIG = {
    "水洗": {
        "salary_table":    "水洗薪資表",
        "salary_slip":     "水洗薪資單",
        "order_sheet":     "水洗訂單",
        "income_sheet":    "水洗營收明細",
        "clear_rows":      [280, 284],          # 上半月清空 J:O 這兩列
        "carry_rows":      [(285, 284), (279, 280)],  # 下半月 (來源→目標)
        "settlement_row":  285,                 # 結算讀取列
        "order_count_row": 40,                  # 主控試算表當期搬運筆數列號
        "preprocess_key":  "水洗前置",
        "classification_key": "複製水洗訂單列數",
        "settlement_key":  "水洗結算",
        "pdf_key":         "水洗薪資單",
        "file_title":      "水洗承攬服務費",
        "note_cell":       "AC43", "note_row": 43,
        "detail_title_row": 45, "detail_start_row": 46,
        "detail_title": ["", "服務日期（星期）", "客戶姓名", "服務數量", "服務項目"],
    },
    "家電": {
        "salary_table":    "家電薪資表",
        "salary_slip":     "家電薪資單",
        "order_sheet":     "家電訂單",
        "income_sheet":    "家電營收明細",
        "clear_rows":      [249, 253],          # 上半月清空
        "carry_rows":      [(254, 253), (249, 250)],  # 下半月複製
        "settlement_row":  254,
        "order_count_row": 41,
        "preprocess_key":  "家電前置",
        "classification_key": "複製家電訂單列數",
        "settlement_key":  "家電結算",
        "pdf_key":         "家電薪資單",
        "file_title":      "家電承攬服務費",
        "note_cell":       "AC36", "note_row": 36,
        "detail_title_row": 37, "detail_start_row": 38,
        "detail_title": ["", "服務日期（星期）", "客戶姓名", "服務數量", "服務人"],
    },
    "收納": {
        "salary_table":    "收納薪資表",
        "salary_slip":     "收納薪資單",
        "order_sheet":     "收納訂單",
        "income_sheet":    "收納營收明細",
        "clear_rows":      [],
        "carry_rows":      [],
        "settlement_row":  254,
        "order_count_row": 42,
        "preprocess_key":  "收納前置",
        "classification_key": "複製收納訂單列數",
        "settlement_key":  "收納結算",
        "pdf_key":         "收納薪資單",
        "file_title":      "收納承攬服務費",
        "note_cell":       "", "note_row": 0,
        "detail_title_row": 29, "detail_start_row": 30,
        "detail_title": ["", "服務日期（星期）", "客戶姓名", "服務時數", "服務項目"],
    },
    "座椅": {
        "salary_table":    "座椅薪資表",
        "salary_slip":     "座椅薪資單",
        "order_sheet":     "座椅訂單",
        "income_sheet":    "座椅營收明細",
        "clear_rows":      [],
        "carry_rows":      [],
        "settlement_row":  254,
        "order_count_row": 43,
        "preprocess_key":  "座椅前置",
        "classification_key": "複製座椅訂單列數",
        "settlement_key":  "座椅結算",
        "pdf_key":         "座椅薪資單",
        "file_title":      "座椅承攬服務費",
        "note_cell":       "", "note_row": 0,
        "detail_title_row": 29, "detail_start_row": 30,
        "detail_title": ["", "服務日期（星期）", "客戶姓名", "服務數量", "服務項目"],
    },
    "地毯": {
        "salary_table":    "地毯薪資表",
        "salary_slip":     "地毯薪資單",
        "order_sheet":     "地毯訂單",
        "income_sheet":    "地毯營收明細",
        "clear_rows":      [],
        "carry_rows":      [],
        "settlement_row":  254,
        "order_count_row": 44,
        "preprocess_key":  "地毯前置",
        "classification_key": "複製地毯訂單列數",
        "settlement_key":  "地毯結算",
        "pdf_key":         "地毯薪資單",
        "file_title":      "地毯承攬服務費",
        "note_cell":       "", "note_row": 0,
        "detail_title_row": 29, "detail_start_row": 30,
        "detail_title": ["", "服務日期（星期）", "客戶姓名", "服務數量", "服務項目"],
    },
}

ALL_SERVICES        = ["水洗", "家電", "收納", "座椅", "地毯"]
PDF_LIST_SHEET      = "PDF產出"
SALARY_SUMMARY_SHEET = "薪資總表"
ORDER_COL_COUNT     = 62   # A:BJ
TS_FMT              = "%Y/%m/%d %H:%M"


# ─────────────────────────────────────────────────────────────────────────────
# 🔧  工具函式
# ─────────────────────────────────────────────────────────────────────────────

def _is_zero(val) -> bool:
    if val is None:
        return True
    s = str(val).strip().replace(",", "").replace("，", "")
    if s in ("", "-", "－", "0"):
        return True
    try:
        return float(s) == 0
    except ValueError:
        return True


def _last_nonempty_row_b(ws: gspread.Worksheet) -> int:
    """B 欄最後非空白列號（1-based）；找不到回傳 1。"""
    vals = ws.col_values(2)
    for i in range(len(vals) - 1, -1, -1):
        if str(vals[i]).strip():
            return i + 1
    return 1


def _first_empty_row_b(ws: gspread.Worksheet, start_row: int = 2) -> int:
    """由上往下找 B 欄第一個空白列，對齊 GAS 行為。"""
    vals = ws.get(f"B{start_row}:B{ws.row_count}") or []
    for offset in range(ws.row_count - start_row + 1):
        value = vals[offset][0] if offset < len(vals) and vals[offset] else ""
        if not str(value).strip():
            return start_row + offset
    return ws.row_count + 1


def _sheets_service():
    from googleapiclient.discovery import build
    return build("sheets", "v4", credentials=get_credentials(), cache_discovery=False)


def _clear_order_from(ws: gspread.Worksheet, start_row: int):
    if start_row > ws.row_count:
        return
    ws.batch_clear([f"A{start_row}:BJ{ws.row_count}"])
    ws.spreadsheet.batch_update({"requests": [{
        "repeatCell": {
            "range": {
                "sheetId": ws.id,
                "startRowIndex": start_row - 1,
                "endRowIndex": ws.row_count,
                "startColumnIndex": 0,
                "endColumnIndex": ORDER_COL_COUNT,
            },
            "cell": {},
            "fields": "userEnteredFormat",
        }
    }]})


def _read_income_rows_and_backgrounds(
    spreadsheet_id: str, ws: gspread.Worksheet, start_row: int
) -> tuple[list[list], list[list[dict]]]:
    last_row = max(ws.row_count, start_row)
    values = ws.get(
        f"A{start_row}:BJ{last_row}",
        value_render_option="UNFORMATTED_VALUE",
    ) or []
    request = _sheets_service().spreadsheets().get(
        spreadsheetId=spreadsheet_id,
        ranges=[f"'{ws.title}'!A{start_row}:BJ{last_row}"],
        includeGridData=True,
        fields="sheets.data.rowData.values.effectiveFormat.backgroundColor",
    )
    request.headers["Accept-Encoding"] = "identity"
    payload = request.execute()
    try:
        grid_rows = payload["sheets"][0]["data"][0].get("rowData", [])
    except (KeyError, IndexError):
        grid_rows = []

    rows, backgrounds = [], []
    for idx, row in enumerate(values):
        padded = (list(row) + [""] * ORDER_COL_COUNT)[:ORDER_COL_COUNT]
        if not any(str(cell).strip() for cell in padded):
            continue
        fmt_values = grid_rows[idx].get("values", []) if idx < len(grid_rows) else []
        colors = []
        for col in range(ORDER_COL_COUNT):
            try:
                color = fmt_values[col]["effectiveFormat"]["backgroundColor"]
            except (KeyError, IndexError, TypeError):
                color = {"red": 1, "green": 1, "blue": 1}
            colors.append(color)
        rows.append(padded)
        backgrounds.append(colors)
    return rows, backgrounds


def _write_backgrounds(ws: gspread.Worksheet, start_row: int, backgrounds: list[list[dict]]):
    if not backgrounds:
        return
    rows = [{
        "values": [{"userEnteredFormat": {"backgroundColor": color}} for color in colors]
    } for colors in backgrounds]
    ws.spreadsheet.batch_update({"requests": [{
        "updateCells": {
            "range": {
                "sheetId": ws.id,
                "startRowIndex": start_row - 1,
                "endRowIndex": start_row - 1 + len(rows),
                "startColumnIndex": 0,
                "endColumnIndex": ORDER_COL_COUNT,
            },
            "rows": rows,
            "fields": "userEnteredFormat.backgroundColor",
        }
    }]})


def _apply_order_date_formats(ws: gspread.Worksheet, start_row: int, row_count: int):
    """訂單 C／D／H 欄固定為日期格式，避免顯示 Sheets 日期序號。"""
    if row_count <= 0:
        return
    requests = []
    for col_index in (2, 3, 7):  # C, D, H（0-based）
        requests.append({
            "repeatCell": {
                "range": {
                    "sheetId": ws.id,
                    "startRowIndex": start_row - 1,
                    "endRowIndex": start_row - 1 + row_count,
                    "startColumnIndex": col_index,
                    "endColumnIndex": col_index + 1,
                },
                "cell": {
                    "userEnteredFormat": {
                        "numberFormat": {"type": "DATE", "pattern": "yyyy/m/d"}
                    }
                },
                "fields": "userEnteredFormat.numberFormat",
            }
        })
    ws.spreadsheet.batch_update({"requests": requests})


def _get_cell(ws: gspread.Worksheet, row: int, col: int) -> str:
    try:
        return str(ws.cell(row, col).value or "").strip()
    except Exception:
        return ""


def _find_other_file(root_folder_id: str, period: str, region: str) -> str:
    """從根目錄/期別資料夾依檔名「其他承攬」找出試算表 ID。"""
    drive = get_drive_service()

    def _find_folder(parent: str, name: str):
        q = (f"'{parent}' in parents and name='{name}' "
             f"and mimeType='application/vnd.google-apps.folder' and trashed=false")
        res = drive.files().list(q=q, fields="files(id)", supportsAllDrives=True,
                                 includeItemsFromAllDrives=True, pageSize=5).execute()
        files = res.get("files", [])
        return files[0]["id"] if files else None

    period_id = _find_folder(root_folder_id, period)
    if not period_id:
        raise FileNotFoundError(f"找不到期別資料夾：{period}")

    q = (f"'{period_id}' in parents and name contains '其他承攬' "
         f"and mimeType='application/vnd.google-apps.spreadsheet' and trashed=false")
    res = drive.files().list(q=q, fields="files(id, name)", supportsAllDrives=True,
                             includeItemsFromAllDrives=True, pageSize=5).execute()
    files = res.get("files", [])
    if not files:
        raise FileNotFoundError(f"在 {period} 資料夾找不到其他承攬試算表")
    return files[0]["id"]


# ─────────────────────────────────────────────────────────────────────────────
# 📁  前置作業
# ─────────────────────────────────────────────────────────────────────────────

def run_other_preprocess(
    root_folder_id: str,
    region: str,
    period: str,
    is_first_half: bool,
    service_type: str | None,
    log: Callable,
    **kwargs,
) -> dict:
    """
    其他承攬前置作業。
    service_type=None → 全部服務；傳入名稱 → 單一服務（補跑用）。

    與 GAS 相同：水洗/家電處理薪資表；訂單依 B 欄第一空白列分段，
    並同步 A:BJ 的資料與背景色。
    """
    half = "上半月" if is_first_half else "下半月"
    svcs = [service_type] if service_type else ALL_SERVICES
    log(f"🔄 其他承攬{half}前置作業（{'全部' if not service_type else service_type}）")

    try:
        other_file_id = _find_other_file(root_folder_id, period, region)
        log(f"  找到其他承攬試算表：{other_file_id}")
    except FileNotFoundError as e:
        log(f"❌ {e}")
        return {}

    gc    = get_gspread_client()
    other = gc.open_by_key(other_file_id)
    results = {}
    classification_keys = [SERVICE_CONFIG[s]["classification_key"] for s in svcs]
    recorded_counts = get_recorded_values(region, period, classification_keys)

    for svc in svcs:
        cfg = SERVICE_CONFIG[svc]
        log(f"\n▶ {svc}")
        try:
            if svc in ("水洗", "家電"):
                _process_salary_formulas(other, cfg, is_first_half, svc, log)
            count = _process_order_data(
                other, cfg, is_first_half, svc, region, period, log,
                expected_count=recorded_counts.get(cfg["classification_key"]),
            )
            results[svc] = count
            log(f"  ✅ {svc} 完成（搬入 {count} 筆）")
        except Exception as e:
            logger.exception(f"{svc} 前置失敗")
            log(f"  ❌ {svc} 前置失敗：{e}")
            results[svc] = -1
        time.sleep(0.5)

    # 打卡
    ts    = format_taipei_time(fmt=TS_FMT)
    batch = []
    for svc in svcs:
        c = results.get(svc, 0)
        if c >= 0:
            batch.append({"task_key": SERVICE_CONFIG[svc]["preprocess_key"], "count": c})
    record_batch(region, period, batch)

    log(f"\n✅ 其他承攬{half}前置作業完成")
    return results


def _has_income_data(ss: gspread.Spreadsheet, cfg: dict) -> bool:
    """檢查營收明細 B 欄是否有非空白資料（至少一筆有效資料）。"""
    income_ws = ss.worksheet(cfg["income_sheet"])
    b_vals    = income_ws.col_values(2)   # B 欄
    return any(str(v).strip() for v in b_vals[1:])  # 跳過標題列


def _process_salary_formulas(
    ss: gspread.Spreadsheet,
    cfg: dict,
    is_first_half: bool,
    svc: str,
    log: Callable,
):
    """
    薪資表公式操作——僅在該服務營收明細有資料時才執行。
    上半月：清空指定列 J:O。
    下半月：將來源列值複製到目標列。
    """
    ws = ss.worksheet(cfg["salary_table"])
    if is_first_half:
        ws.batch_clear([f"J{r}:O{r}" for r in cfg["clear_rows"]])
        log(f"  薪資表清空列：{cfg['clear_rows']}")
    else:
        for src, tgt in cfg["carry_rows"]:
            vals = ws.get(f"J{src}:O{src}", value_render_option="UNFORMATTED_VALUE")
            if vals:
                ws.update(f"J{tgt}:O{tgt}", vals, value_input_option="RAW")
        log(f"  薪資表複製列：{[(src, tgt) for src, tgt in cfg['carry_rows']]}")
    time.sleep(0.3)


def _process_order_data(
    ss: gspread.Spreadsheet,
    cfg: dict,
    is_first_half: bool,
    svc: str,
    region: str,
    period: str,
    log: Callable,
    expected_count=None,
) -> int:
    """依中控打卡筆數精確搬運本期分類資料，重跑時不重複追加。"""
    income_ws = ss.worksheet(cfg["income_sheet"])
    order_ws  = ss.worksheet(cfg["order_sheet"])

    try:
        expected_count = int(float(str(expected_count).strip())) if expected_count else 0
    except (TypeError, ValueError):
        expected_count = 0
    if expected_count <= 0:
        log(f"  {svc} 本期分類貼入列數為 0，略過")
        return 0

    last_income_row = _last_nonempty_row_b(income_ws)
    income_start = max(2, last_income_row - expected_count + 1)
    rows, backgrounds = _read_income_rows_and_backgrounds(ss.id, income_ws, income_start)
    if len(rows) != expected_count:
        raise ValueError(
            f"{svc} 本期應有 {expected_count} 筆，但營收明細尾端只讀到 {len(rows)} 筆"
        )

    if is_first_half:
        paste_start = 2
    else:
        last_order_row = _last_nonempty_row_b(order_ws)
        existing_start = max(2, last_order_row - expected_count + 1)
        existing_ids = order_ws.get(f"B{existing_start}:B{last_order_row}") or []
        source_ids = [[row[1]] for row in rows]
        if existing_ids == source_ids:
            paste_start = existing_start
            log(f"  {svc} 偵測到本期資料已存在，覆寫第 {paste_start} 列起，不重複追加")
        else:
            paste_start = last_order_row + 1

    _clear_order_from(order_ws, paste_start)
    if not rows:
        log(f"  {svc} 營收明細無本期資料，略過")
        return 0

    if svc == "水洗":
        for row in rows:
            row[4] = re.sub(r"^\s*3\s*水洗\s*[:：]\s*", "", str(row[4] or ""))
        log("  水洗訂單 E 欄已移除「3水洗：」前綴")

    end_row = paste_start + len(rows) - 1
    if end_row > order_ws.row_count:
        order_ws.add_rows(end_row - order_ws.row_count)
    order_ws.update(f"A{paste_start}:BJ{end_row}", rows, value_input_option="RAW")
    _write_backgrounds(order_ws, paste_start, backgrounds)
    _apply_order_date_formats(order_ws, paste_start, len(rows))
    log(f"  {svc} 訂單寫入第 {paste_start}–{end_row} 列（{len(rows)} 筆，含背景色）")
    return len(rows)


# ─────────────────────────────────────────────────────────────────────────────
# 📊  結算作業
# ─────────────────────────────────────────────────────────────────────────────

def run_other_settlement(
    root_folder_id: str,
    region: str,
    period: str,
    service_type: str | None,
    log: Callable,
    is_first_half: bool | None = None,
    **kwargs,
) -> dict:
    """
    其他承攬結算作業。

    固定順序：
    1. 先操作「薪資總表」
       - 上半月：E > 0 → E→P、B→Q；Q 比對 H → I→N、J→O
       - 下半月：F > 0 → F→W、B→X；X 比對 H → I→U、J→V
       - A 欄服務名稱保留，並直接作為本次 PDF 名單的服務名稱來源
    2. 再清空「PDF產出」B2:I
    3. 直接用本次薪資總表篩選結果建立 PDF 名單
       - B = 姓名
       - H = Y
       - I = 服務名稱
       - 自第 2 列起三欄完全同列對齊
    """
    if is_first_half is None:
        if str(period).endswith("-1"):
            is_first_half = True
        elif str(period).endswith("-2"):
            is_first_half = False
        else:
            raise ValueError(f"無法由期別判斷上下半月：{period}，請傳入 is_first_half")

    half = "上半月" if is_first_half else "下半月"
    log(f"📊 其他承攬{half}結算作業" + (f"（{service_type}）" if service_type else ""))

    try:
        other_file_id = _find_other_file(root_folder_id, period, region)
        log(f"  找到其他承攬試算表：{other_file_id}")
    except FileNotFoundError as e:
        log(f"❌ {e}")
        return {}

    gc = get_gspread_client()
    other = gc.open_by_key(other_file_id)

    # Step 1：先操作薪資總表，並保留本次篩選結果做 PDF 名單來源。
    try:
        records = _prepare_salary_summary_settlement(
            other,
            is_first_half=is_first_half,
            service_type=service_type,
            log=log,
        )
    except gspread.WorksheetNotFound:
        log(f"❌ 找不到「{SALARY_SUMMARY_SHEET}」工作表")
        return {}
    except Exception as e:
        logger.exception("薪資總表結算失敗")
        log(f"❌ 薪資總表結算失敗：{e}")
        return {}

    # Step 2：薪資總表完成後，才清空 PDF產出 B2:I。
    try:
        pdf_ws = other.worksheet(PDF_LIST_SHEET)
    except gspread.WorksheetNotFound:
        log(f"❌ 找不到「{PDF_LIST_SHEET}」工作表")
        return {}

    log("  清空 PDF產出 B2:I...")
    last_row = max(pdf_ws.row_count, 2)
    pdf_ws.batch_clear([f"B2:I{last_row}"])
    log("  清空完成")

    # Step 3：直接用本次薪資總表篩選結果建立 PDF 名單。
    if records:
        values = [[r["name"], "Y", r["service"]] for r in records]
        end_row = 2 + len(values) - 1
        if end_row > pdf_ws.row_count:
            pdf_ws.add_rows(end_row - pdf_ws.row_count)

        # 一次寫 B、H、I，所有資料使用相同 row index，保證同列對齊。
        pdf_ws.update(f"B2:B{end_row}", [[v[0]] for v in values], value_input_option="RAW")
        pdf_ws.update(f"H2:H{end_row}", [[v[1]] for v in values], value_input_option="RAW")
        pdf_ws.update(f"I2:I{end_row}", [[v[2]] for v in values], value_input_option="RAW")
        log(f"  PDF產出名單寫入 {len(values)} 筆（B=姓名、H=Y、I=服務名稱）")
    else:
        log("  本次薪資總表無符合條件人員，PDF產出名單維持空白")

    # 依服務統計本次實際篩選結果並打卡。
    results = {svc: [] for svc in ALL_SERVICES}
    for record in records:
        results.setdefault(record["service"], []).append(record["name"])

    if service_type:
        punch_services = [service_type]
    else:
        punch_services = ALL_SERVICES

    batch_punch = [
        {
            "task_key": SERVICE_CONFIG[svc]["settlement_key"],
            "count": len(results.get(svc, [])),
        }
        for svc in punch_services
        if svc in SERVICE_CONFIG
    ]
    record_batch(region, period, batch_punch)

    log("\n✅ 其他承攬結算作業完成")
    return results


def _positive_number(value) -> bool:
    """符合結算條件：可解析為數字且 > 0。"""
    if value is None:
        return False
    text = str(value).strip().replace(",", "").replace("，", "")
    if not text:
        return False
    try:
        return float(text) > 0
    except (TypeError, ValueError):
        return False


def _prepare_salary_summary_settlement(
    ss: gspread.Spreadsheet,
    is_first_half: bool,
    service_type: str | None,
    log: Callable,
) -> list[dict]:
    """整理「薪資總表」結算區，並回傳本次 PDF 名單來源。

    來源 A:J，自第 3 列起：
    - A：服務名稱
    - B：姓名
    - E/F：上/下半月篩選金額
    - H：比對姓名
    - I/J：比對後帶回資料

    上半月輸出 N:Q = I, J, E, B
    下半月輸出 U:X = I, J, F, B
    """
    ws = ss.worksheet(SALARY_SUMMARY_SHEET)
    last_row = max(ws.row_count, 3)
    rows = ws.get(f"A3:J{last_row}", value_render_option="UNFORMATTED_VALUE") or []

    # H → (I, J)。同名時沿用第一筆。
    lookup = {}
    for row in rows:
        padded = list(row) + [""] * (10 - len(row))
        h_name = str(padded[7] or "").strip()
        if h_name and h_name not in lookup:
            lookup[h_name] = (padded[8], padded[9])

    output = []
    records = []
    for row in rows:
        padded = list(row) + [""] * (10 - len(row))
        service = str(padded[0] or "").strip()  # A
        name = str(padded[1] or "").strip()     # B
        amount = padded[4] if is_first_half else padded[5]  # E / F

        if not name or not _positive_number(amount):
            continue
        if service_type and service != service_type:
            continue

        i_val, j_val = lookup.get(name, ("", ""))
        output.append([i_val, j_val, amount, name])
        records.append({"service": service, "name": name})

    if is_first_half:
        clear_range = f"N3:Q{last_row}"
        start_col = "N"
        end_col = "Q"
    else:
        clear_range = f"U3:X{last_row}"
        start_col = "U"
        end_col = "X"

    ws.batch_clear([clear_range])
    if output:
        end_row = 3 + len(output) - 1
        ws.update(
            f"{start_col}3:{end_col}{end_row}",
            output,
            value_input_option="RAW",
        )

    half = "上半月" if is_first_half else "下半月"
    log(f"  薪資總表 {half}篩選完成：{len(records)} 筆")
    if records:
        log("  PDF 名單來源：" + "、".join(f"{r['service']}/{r['name']}" for r in records))
    return records


# ─────────────────────────────────────────────────────────────────────────────
# 📄  PDF 產出
# ─────────────────────────────────────────────────────────────────────────────

def run_other_pdf(
    root_folder_id: str,
    region: str,
    period: str,
    service_type: str | None,
    log: Callable,
    **kwargs,
) -> dict:
    """
    其他承攬 PDF 產出（對齊 cleaning_pdf.py 架構）。
    service_type=None → 全部；傳入名稱 → 單一服務。

    流程：
    1. 讀取 PDF產出 工作表，篩選 H=Y 且 I=服務類型
    2. 逐人：薪資單 AD2 寫入姓名 → export API → 存 Drive
    3. 成功：D欄=時間、E欄=連結、H欄清空
    4. 失敗：保留 H=Y 以便重跑
    PDF 存放路徑：根目錄/期別/期別/（與清潔承攬共用同一根目錄）
    """
    svcs = [service_type] if service_type else ALL_SERVICES
    log(f"📄 其他承攬PDF產出（{'全部' if not service_type else service_type}）")

    try:
        other_file_id = _find_other_file(root_folder_id, period, region)
        log(f"  找到其他承攬試算表：{other_file_id}")
    except FileNotFoundError as e:
        log(f"❌ {e}")
        return {"pdfs": {}, "failed": [], "success_count": 0}

    # 直接使用 Python 匯出 PDF。Drive 上傳優先使用使用者 OAuth，
    # OAuth 不可用時才退回服務帳戶，再失敗則保留下載模式。
    log("  使用 Python 直接產出 PDF（OAuth Drive 優先）")

    gc    = get_gspread_client()
    other = gc.open_by_key(other_file_id)

    try:
        pdf_ws = other.worksheet(PDF_LIST_SHEET)
    except gspread.WorksheetNotFound:
        log(f"❌ 找不到「{PDF_LIST_SHEET}」工作表")
        return {"pdfs": {}, "failed": [], "success_count": 0}

    raw    = pdf_ws.get("A2:I", value_render_option="UNFORMATTED_VALUE") or []
    result = {"pdfs": {}, "uploaded": {}, "failed": [], "success_count": 0}

    token                  = _get_access_token()
    oauth_drive, folder_id = _prepare_drive_output(root_folder_id, period, log)

    for svc in svcs:
        cfg = SERVICE_CONFIG[svc]

        targets = [
            {"name": str(r[1]).strip(), "row": i + 2}
            for i, r in enumerate(raw)
            if (len(r) > 8
                and str(r[1]).strip()
                and str(r[7]).strip() == "Y"
                and str(r[8]).strip() == svc)
        ]

        if not targets:
            log(f"\n▶ {svc}：無待產出人員")
            continue

        log(f"\n▶ {svc}：{len(targets)} 人")

        try:
            ws_slip  = other.worksheet(cfg["salary_slip"])
            data_ws  = other.worksheet(cfg["salary_table"])
            salary_data = data_ws.get_all_values()
            slip_gid = ws_slip.id
        except gspread.WorksheetNotFound:
            log(f"  ❌ 找不到薪資單工作表：{cfg['salary_slip']}")
            result["failed"].extend([t["name"] for t in targets])
            continue

        for idx, target in enumerate(targets):
            name = target["name"]
            row  = target["row"]
            log(f"  [{idx+1}/{len(targets)}] {name}")

            try:
                # AD2 寫入姓名，等公式連動
                ws_slip.update_cell(2, 30, name)
                details = _build_detail_rows(svc, salary_data, name)
                if not details:
                    pdf_ws.update_cell(row, 5, "⚠️ 無資料，未產出")
                    log(f"    ⚠️ {name} 沒有服務資料，略過")
                    continue
                _write_salary_details(ws_slip, cfg, details)
                time.sleep(2.0)

                # 找 AB:AH 最後有值的列
                export_vals = ws_slip.get("AB1:AH") or []
                last_row = 1
                for k in range(len(export_vals) - 1, -1, -1):
                    if any(str(v).strip() for v in export_vals[k]):
                        last_row = k + 1
                        break
                last_row     = max(last_row, 20)
                export_range = f"AB1:AH{last_row}"
                log(f"    匯出範圍：{export_range}")

                pdf_bytes = _export_pdf(
                    token=token,
                    spreadsheet_id=other_file_id,
                    sheet_gid=slip_gid,
                    export_range=export_range,
                )

                if len(pdf_bytes) < 1000:
                    raise ValueError(f"PDF 過小（{len(pdf_bytes)} bytes），可能為空白頁")

                file_name = f"{period} 檸檬家事｜{cfg['file_title']}_{name}.pdf"
                now_str   = format_taipei_time(fmt=TS_FMT)
                updates   = [
                    {"range": f"D{row}", "values": [[now_str]]},
                    {"range": f"H{row}", "values": [[""]]},
                ]
                uploaded = False

                if oauth_drive and folder_id:
                    try:
                        existing_url = _get_cell(pdf_ws, row, 5)
                        drive_url    = _upload_or_update_drive(
                            oauth_drive, folder_id, pdf_bytes, file_name, existing_url
                        )
                        if not existing_url:
                            updates.append({"range": f"E{row}", "values": [[drive_url]]})
                        uploaded = True
                        result["uploaded"][file_name] = drive_url
                        log(f"    ✅ {name} 上傳完成")
                    except Exception as ue:
                        log(f"    ⚠️ Drive 上傳失敗，保留下載：{ue}")

                if not uploaded:
                    result["pdfs"][file_name] = pdf_bytes
                    log(f"    ✅ {name} PDF 產出（請用下載按鈕儲存）")

                pdf_ws.spreadsheet.values_batch_update({
                    "valueInputOption": "USER_ENTERED",
                    "data": [
                        {"range": f"'{pdf_ws.title}'!{u['range']}", "values": u["values"]}
                        for u in updates
                    ],
                })
                result["success_count"] += 1

            except Exception as e:
                log(f"    ❌ {name} 失敗：{e}")
                result["failed"].append(name)

            time.sleep(0.8)

    # 打卡
    batch_punch = []
    if service_type:
        batch_punch.append({"task_key": SERVICE_CONFIG[service_type]["pdf_key"], "count": None})
    else:
        for svc in svcs:
            batch_punch.append({"task_key": SERVICE_CONFIG[svc]["pdf_key"], "count": None})
    record_batch(region, period, batch_punch)

    log(f"\n✅ PDF產出完成：成功 {result['success_count']} 份，失敗 {len(result['failed'])} 份")
    if result["pdfs"]:
        log("  請點擊下方下載按鈕儲存 PDF")
    return result


def _staff_list(value, pattern=r"[、,，\s]+") -> list[str]:
    return [part.strip() for part in re.split(pattern, str(value or "")) if part.strip()]


def _date_text(value, weekday) -> str:
    text = str(value or "").strip()
    weekday = str(weekday or "").strip()
    for fmt in ("%Y/%m/%d", "%Y-%m-%d", "%m/%d/%Y"):
        try:
            text = datetime.datetime.strptime(text, fmt).strftime("%Y/%m/%d")
            break
        except ValueError:
            pass
    return f"{text} ({weekday})"


def _build_detail_rows(service_type: str, data: list[list], target_name: str) -> list[list]:
    details = []
    for row in data:
        row = list(row) + [""] * max(0, 9 - len(row))
        date_text = _date_text(row[1], row[2])
        customer = str(row[3] or "").strip()

        if service_type == "水洗":
            if target_name not in _staff_list(row[6]):
                continue
            raw_item = re.sub(r"^\s*3\s*水洗[:：]\s*", "", str(row[4] or "")).strip()
            label = raw_item.split("：", 1)[-1]
            details.append(["", f"{date_text}｜{label}", customer, row[8], row[5]])
        elif service_type == "收納":
            if target_name not in _staff_list(row[6], r"[、,，\sXx]+"):
                continue
            raw_item = str(row[4] or "").strip()
            label = raw_item.split("：", 1)[-1]
            details.append(["", f"{date_text}｜{label}", customer, row[7], raw_item])
        else:
            if str(row[6] or "").strip() != target_name:
                continue
            item = str(row[4] or "").strip()
            details.append(["", f"{date_text}｜{item}", customer, row[5], item])

    for index, detail in enumerate(details, 1):
        detail[0] = index
    return details


def _write_salary_details(ws: gspread.Worksheet, cfg: dict, details: list[list]):
    title_row = cfg["detail_title_row"]
    start_row = cfg["detail_start_row"]
    ws.batch_clear([f"AB{title_row}:AF{ws.row_count}"])
    required_last = start_row + len(details) - 1
    if required_last > ws.row_count:
        ws.add_rows(required_last - ws.row_count)
    ws.update(f"AB{title_row}:AF{title_row}", [cfg["detail_title"]], value_input_option="RAW")
    ws.update(f"AB{start_row}:AF{required_last}", details, value_input_option="USER_ENTERED")


# ─────────────────────────────────────────────────────────────────────────────
# 🔑  Drive / PDF export 工具（對齊 cleaning_pdf.py）
# ─────────────────────────────────────────────────────────────────────────────

def _get_access_token() -> str:
    import google.auth.transport.requests
    creds = get_credentials()
    if not creds.token or not creds.valid:
        creds.refresh(google.auth.transport.requests.Request())
    return creds.token


def _export_pdf(
    token: str,
    spreadsheet_id: str,
    sheet_gid: int,
    export_range: str,
) -> bytes:
    params = {
        "exportFormat": "pdf", "format": "pdf",
        "gid": str(sheet_gid), "range": export_range,
        "size": "A4", "portrait": "true", "fitw": "true",
        "sheetnames": "false", "printtitle": "false",
        "pagenum": "false", "gridlines": "false", "fzr": "false",
        "top_margin": "0.5", "bottom_margin": "0.5",
        "left_margin": "0.5", "right_margin": "0.5",
    }
    resp = requests.get(
        f"https://docs.google.com/spreadsheets/d/{spreadsheet_id}/export",
        params=params,
        headers={"Authorization": f"Bearer {token}"},
        timeout=60,
    )
    if resp.status_code != 200:
        raise ValueError(f"PDF export 失敗 HTTP {resp.status_code}: {resp.text[:200]}")
    if not resp.content.startswith(b"%PDF"):
        raise ValueError(f"回傳非 PDF：{resp.text[:200]}")
    return resp.content


def _prepare_drive_output(root_folder_id: str, period: str, log: Callable):
    errors = []
    for label, factory in [
        ("使用者 OAuth", _get_oauth_drive_service),
        ("服務帳戶", get_drive_service),
    ]:
        try:
            drive = factory()
            folder_id = _get_or_create_pdf_folder(root_folder_id, period, drive)
            log(f"  Drive 資料夾準備完成（{label}）")
            return drive, folder_id
        except Exception as e:
            errors.append(f"{label}：{e}")
    log(f"  ⚠️ Drive 未啟用，改走下載模式：{'；'.join(errors)}")
    return None, None


def _get_oauth_drive_service():
    import streamlit as st
    from google.oauth2.credentials import Credentials
    from googleapiclient.discovery import build
    import google.auth.transport.requests

    cfg   = st.secrets["oauth_drive"]
    creds = Credentials(
        token=None, refresh_token=cfg["refresh_token"],
        token_uri=cfg["token_uri"], client_id=cfg["client_id"],
        client_secret=cfg["client_secret"],
        scopes=["https://www.googleapis.com/auth/drive"],
    )
    creds.refresh(google.auth.transport.requests.Request())
    return build("drive", "v3", credentials=creds)


def _get_or_create_pdf_folder(root_id: str, period: str, drive) -> str:
    """根目錄/期別/期別（三層），回傳最內層 ID。"""
    def _foc(parent: str, name: str) -> str:
        q = (f"'{parent}' in parents and name='{name}' "
             f"and mimeType='application/vnd.google-apps.folder' and trashed=false")
        res   = drive.files().list(q=q, fields="files(id)", supportsAllDrives=True,
                                   includeItemsFromAllDrives=True, pageSize=5).execute()
        files = res.get("files", [])
        if files:
            return files[0]["id"]
        return drive.files().create(
            body={"name": name, "mimeType": "application/vnd.google-apps.folder",
                  "parents": [parent]},
            fields="id", supportsAllDrives=True,
        ).execute()["id"]

    return _foc(_foc(root_id, period), period)


def _upload_or_update_drive(
    oauth_drive, folder_id: str, pdf_bytes: bytes,
    file_name: str, existing_url: str = "",
) -> str:
    import re
    from googleapiclient.http import MediaIoBaseUpload

    media = MediaIoBaseUpload(io.BytesIO(pdf_bytes), mimetype="application/pdf")
    m     = re.search(r"/d/([a-zA-Z0-9_-]+)", str(existing_url))
    eid   = m.group(1) if m else None

    if eid:
        oauth_drive.files().update(
            fileId=eid, body={"name": file_name},
            media_body=media, supportsAllDrives=True,
        ).execute()
        fid = eid
        url = existing_url
    else:
        res = oauth_drive.files().create(
            body={"name": file_name, "parents": [folder_id]},
            media_body=media, fields="id", supportsAllDrives=True,
        ).execute()
        fid = res["id"]
        url = f"https://drive.google.com/file/d/{fid}/view"

    try:
        oauth_drive.permissions().create(
            fileId=fid, body={"type": "anyone", "role": "reader"},
            supportsAllDrives=True,
        ).execute()
    except Exception as e:
        if "already" not in str(e).lower():
            raise
    return url


# ─────────────────────────────────────────────────────────────────────────────
# 🔑  主控試算表 task_key 清單
# ─────────────────────────────────────────────────────────────────────────────

OTHER_CONTRACT_TASK_KEYS = [
    "其他承攬",
    "水洗前置", "家電前置", "收納前置", "座椅前置", "地毯前置",
    "水洗結算", "家電結算", "收納結算", "座椅結算", "地毯結算",
    "水洗薪資單", "家電薪資單", "收納薪資單", "座椅薪資單", "地毯薪資單",
]
