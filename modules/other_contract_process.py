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
from typing import Callable, List

import gspread

from modules.auth import get_gspread_client, get_drive_service, get_credentials
from modules.master_sheet import record_execution, record_batch, get_recorded_value

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
        "preprocess_key":  "複製水洗訂單列數",
        "settlement_key":  "水洗結算",
        "pdf_key":         "水洗PDF",
        "file_title":      "水洗承攬服務費",
    },
    "家電": {
        "salary_table":    "家電薪資表",
        "salary_slip":     "家電薪資單",
        "order_sheet":     "家電訂單",
        "income_sheet":    "家電營收明細",
        "clear_rows":      [249, 253],          # 上半月清空
        "carry_rows":      [(254, 253), (248, 249)],  # 下半月複製
        "settlement_row":  254,
        "order_count_row": 41,
        "preprocess_key":  "複製家電訂單列數",
        "settlement_key":  "家電結算",
        "pdf_key":         "家電PDF",
        "file_title":      "家電承攬服務費",
    },
    "收納": {
        "salary_table":    "收納薪資表",
        "salary_slip":     "收納薪資單",
        "order_sheet":     "收納訂單",
        "income_sheet":    "收納營收明細",
        "clear_rows":      [218, 222],
        "carry_rows":      [(223, 222), (217, 218)],
        "settlement_row":  223,
        "order_count_row": 42,
        "preprocess_key":  "複製收納訂單列數",
        "settlement_key":  "收納結算",
        "pdf_key":         "收納PDF",
        "file_title":      "收納承攬服務費",
    },
    "座椅": {
        "salary_table":    "座椅薪資表",
        "salary_slip":     "座椅薪資單",
        "order_sheet":     "座椅訂單",
        "income_sheet":    "座椅營收明細",
        "clear_rows":      [218, 222],          # 上半月清空（與收納相同）
        "carry_rows":      [(223, 222), (217, 218)],  # 下半月複製（與收納相同）
        "settlement_row":  223,
        "order_count_row": 43,
        "preprocess_key":  "複製座椅訂單列數",
        "settlement_key":  "座椅結算",
        "pdf_key":         "座椅PDF",
        "file_title":      "座椅承攬服務費",
    },
    "地毯": {
        "salary_table":    "地毯薪資表",
        "salary_slip":     "地毯薪資單",
        "order_sheet":     "地毯訂單",
        "income_sheet":    "地毯營收明細",
        "clear_rows":      [211, 215],
        "carry_rows":      [(216, 215), (210, 211)],
        "settlement_row":  216,
        "order_count_row": 44,
        "preprocess_key":  "複製地毯訂單列數",
        "settlement_key":  "地毯結算",
        "pdf_key":         "地毯PDF",
        "file_title":      "地毯承攬服務費",
    },
}

ALL_SERVICES        = ["水洗", "家電", "收納", "座椅", "地毯"]
TASK_KEY_PRE_ALL    = "其他承攬前置作業"
TASK_KEY_SETTLE_ALL = "其他承攬結算作業"
PDF_LIST_SHEET      = "PDF產出"
ORDER_COL_COUNT     = 62   # A:BJ
TS_FMT              = "%Y/%m/%d %H:%M"


# ─────────────────────────────────────────────────────────────────────────────
# 🔧  工具函式
# ─────────────────────────────────────────────────────────────────────────────

def _is_zero(val) -> bool:
    if val is None:
        return True
    s = str(val).strip()
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

    每個服務步驟：
    1. 薪資表公式操作（上半月清空 J:O 指定列；下半月複製值）
    2. 訂單工作表
       - 先檢查營收明細是否有資料（B 欄非空）
       - 有資料：上半月先清空 A2:BJ 再貼入；下半月從 B 欄最後非空下一列 append
       - 無資料：跳過（上半月也不清空）
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

    for svc in svcs:
        cfg = SERVICE_CONFIG[svc]
        log(f"\n▶ {svc}")
        try:
            # Step 1：確認營收明細是否有資料
            has_data = _has_income_data(other, cfg)
            if not has_data:
                log(f"  {svc} 營收明細空白，跳過薪資表操作與訂單搬運")
                results[svc] = 0
                continue

            # Step 2：薪資表公式操作（有資料才執行）
            _process_salary_formulas(other, cfg, is_first_half, svc, log)

            # Step 3：訂單搬運（從主控試算表讀取本期筆數）
            count = _process_order_data(
                other, cfg, is_first_half, svc, region, period, log
            )
            results[svc] = count
            log(f"  ✅ {svc} 完成（搬入 {count} 筆）")
        except Exception as e:
            logger.exception(f"{svc} 前置失敗")
            log(f"  ❌ {svc} 前置失敗：{e}")
            results[svc] = -1
        time.sleep(0.5)

    # 打卡
    ts    = datetime.datetime.now().strftime(TS_FMT)
    batch = []
    for svc in svcs:
        c = results.get(svc, 0)
        if c >= 0:
            batch.append({"task_key": SERVICE_CONFIG[svc]["preprocess_key"], "count": c})
    if not service_type:
        batch.append({"task_key": TASK_KEY_PRE_ALL, "count": None})
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
) -> int:
    """
    訂單搬運：從金流對帳主控試算表讀取本期該服務的搬運列數，
    再從營收明細取對應筆數搬入訂單工作表。
    上半月：先清空 A2:BJ，從營收明細第一筆開始貼入。
    下半月：從訂單 B 欄最後非空白下一列 append。
    """
    income_ws = ss.worksheet(cfg["income_sheet"])
    order_ws  = ss.worksheet(cfg["order_sheet"])

    # 從主控試算表固定列號讀取本期搬運筆數
    # order_count_row：水洗=40, 家電=41, 收納=42, 座椅=43, 地毯=44
    # 主控試算表結構：A欄=作業名稱，每期佔2欄（ID/筆數 + 完成時間）
    # 用 get_recorded_value 依 task_key 讀取（底層會找對應列的 ID/筆數欄）
    period_count_val = get_recorded_value(region, period, cfg["preprocess_key"])
    try:
        period_count = int(float(str(period_count_val).strip())) if period_count_val else 0
    except (ValueError, TypeError):
        period_count = 0

    if period_count == 0:
        log(f"  {svc} 主控試算表無搬運筆數（第 {cfg['order_count_row']} 列），跳過訂單搬運")
        log(f"    （請先完成金流對帳⑤，確認「{cfg['preprocess_key']}」已打卡）")
        return 0

    # 從營收明細讀取資料（B 欄最後非空白往上數 period_count 列）
    last_income_row = _last_nonempty_row_b(income_ws)
    income_start    = max(2, last_income_row - period_count + 1)
    log(f"  {svc} 從主控讀得本期筆數：{period_count}，營收明細第 {income_start}–{last_income_row} 列")

    income_data = income_ws.get(
        f"A{income_start}:BJ{last_income_row}",
        value_render_option="UNFORMATTED_VALUE",
    ) or []

    if not income_data:
        log(f"  {svc} 營收明細讀取為空，跳過")
        return 0

    # 補齊欄數
    padded = []
    for row in income_data:
        padded_row = list(row) + [""] * max(0, ORDER_COL_COUNT - len(row))
        padded.append(padded_row[:ORDER_COL_COUNT])

    if is_first_half:
        order_ws.batch_clear([f"A2:BJ{order_ws.row_count}"])
        log(f"  {svc} 訂單清空完成")
        paste_start = 2
    else:
        last        = _last_nonempty_row_b(order_ws)
        paste_start = last + 1
        log(f"  {svc} 下半月 append 起始列：{paste_start}")

    end_row = paste_start + len(padded) - 1
    order_ws.update(f"A{paste_start}:BJ{end_row}", padded, value_input_option="RAW")
    log(f"  {svc} 訂單寫入第 {paste_start}–{end_row} 列（{len(padded)} 筆）")
    return len(padded)


# ─────────────────────────────────────────────────────────────────────────────
# 📊  結算作業
# ─────────────────────────────────────────────────────────────────────────────

def run_other_settlement(
    root_folder_id: str,
    region: str,
    period: str,
    service_type: str | None,
    log: Callable,
    **kwargs,
) -> dict:
    """
    其他承攬結算作業。
    service_type=None → 全部；傳入名稱 → 單一服務（補跑用）。

    步驟（不論上下半月相同）：
    1. 清空 PDF產出工作表 B2:I（整個清空）
    2. 各服務讀薪資表結算列（J1:O1=姓名，結算列=金額），非零者納入
    3. 依序寫入 PDF產出：B=姓名、H=Y、I=服務類型
    """
    svcs = [service_type] if service_type else ALL_SERVICES
    log(f"📊 其他承攬結算作業（{'全部' if not service_type else service_type}）")

    try:
        other_file_id = _find_other_file(root_folder_id, period, region)
        log(f"  找到其他承攬試算表：{other_file_id}")
    except FileNotFoundError as e:
        log(f"❌ {e}")
        return {}

    gc    = get_gspread_client()
    other = gc.open_by_key(other_file_id)

    try:
        pdf_ws = other.worksheet(PDF_LIST_SHEET)
    except gspread.WorksheetNotFound:
        log(f"❌ 找不到「{PDF_LIST_SHEET}」工作表")
        return {}

    # Step 1：清空 B2:I
    log("  清空 PDF產出 B2:I...")
    last_row = max(pdf_ws.row_count, 2)
    pdf_ws.batch_clear([f"B2:I{last_row}"])
    log("  清空完成")

    results     = {}
    next_row    = 2   # PDF產出從第 2 列起順序寫入

    for svc in svcs:
        cfg = SERVICE_CONFIG[svc]
        if cfg["settlement_row"] is None:
            log(f"\n▶ {svc}：略過（無結算列）")
            results[svc] = []
            continue

        log(f"\n▶ {svc}（第 {cfg['settlement_row']} 列）")
        try:
            names = _collect_nonzero_names(other, cfg, svc, log)
            log(f"  有效人員：{names if names else '（無）'}")

            if names:
                batch = []
                for name in names:
                    batch.append({"range": f"B{next_row}", "values": [[name]]})
                    batch.append({"range": f"H{next_row}", "values": [["Y"]]})
                    batch.append({"range": f"I{next_row}", "values": [[svc]]})
                    next_row += 1
                pdf_ws.batch_update(batch)
                log(f"  寫入 {len(names)} 筆")

            results[svc] = names
        except Exception as e:
            logger.exception(f"{svc} 結算失敗")
            log(f"  ❌ {svc} 結算失敗：{e}")
            results[svc] = []
        time.sleep(0.3)

    # 打卡
    ts    = datetime.datetime.now().strftime(TS_FMT)
    batch_punch = []
    for svc in svcs:
        batch_punch.append({
            "task_key": SERVICE_CONFIG[svc]["settlement_key"],
            "count":    len(results.get(svc, [])),
        })
    if not service_type:
        batch_punch.append({"task_key": TASK_KEY_SETTLE_ALL, "count": None})
    record_batch(region, period, batch_punch)

    log("\n✅ 其他承攬結算作業完成")
    return results


def _collect_nonzero_names(
    ss: gspread.Spreadsheet,
    cfg: dict,
    svc: str,
    log: Callable,
) -> list[str]:
    """薪資表 J1:O1 = 姓名；結算列 J:O = 金額；非零、去重，回傳名單。"""
    ws  = ss.worksheet(cfg["salary_table"])
    row = cfg["settlement_row"]

    header  = ws.row_values(1)
    amounts = ws.row_values(row)

    names = header[9:15]
    amts  = (amounts[9:15] if len(amounts) >= 15
             else amounts[9:] + [""] * (15 - len(amounts)))

    seen, result = set(), []
    for name, amt in zip(names, amts):
        name = str(name).strip()
        if name and not _is_zero(amt) and name not in seen:
            seen.add(name)
            result.append(name)
    return result


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
                time.sleep(3.0)

                # 找 AB 欄最後有值的列
                ab_vals  = ws_slip.col_values(28)
                last_row = 1
                for k in range(len(ab_vals) - 1, -1, -1):
                    if str(ab_vals[k]).strip():
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
                now_str   = datetime.datetime.now().strftime(TS_FMT)
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
    try:
        drive     = _get_oauth_drive_service()
        folder_id = _get_or_create_pdf_folder(root_folder_id, period, drive)
        log("  Drive 資料夾準備完成")
        return drive, folder_id
    except Exception as e:
        log(f"  ⚠️ Drive 未啟用，改走下載模式：{e}")
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
        return existing_url

    res = oauth_drive.files().create(
        body={"name": file_name, "parents": [folder_id]},
        media_body=media, fields="id", supportsAllDrives=True,
    ).execute()
    fid = res["id"]
    oauth_drive.permissions().create(
        fileId=fid, body={"type": "anyone", "role": "reader"},
        supportsAllDrives=True,
    ).execute()
    return f"https://drive.google.com/file/d/{fid}/view"


# ─────────────────────────────────────────────────────────────────────────────
# 🔑  主控試算表 task_key 清單
# ─────────────────────────────────────────────────────────────────────────────

OTHER_CONTRACT_TASK_KEYS = [
    "其他承攬",
    "其他承攬前置作業",
    "複製水洗訂單列數", "複製家電訂單列數", "複製收納訂單列數",
    "複製座椅訂單列數", "複製地毯訂單列數",
    "其他承攬結算作業",
    "水洗結算", "家電結算", "收納結算", "座椅結算", "地毯結算",
    "水洗PDF", "家電PDF", "收納PDF", "座椅PDF", "地毯PDF",
]
