"""
Lemon Clean 清潔承攬 — 前置作業 & 00調薪
檔案：modules/cleaning_process.py

依賴：
    modules/auth.py           — get_gspread_client()
    modules/drive_helper.py   — find_file_in_folder()
    modules/master_sheet.py   — record_execution()
    modules/common_process.py — run_common_process()

清潔承攬試算表（exec 工作表）關鍵儲存格（供程式讀取，不在此打卡）：
    B1  → 期別 YYYYMM（如 202605）
    C2  → 請款試算表 ID
    C3  → 薪資試算表 ID
    C4  → 名冊試算表 ID
    C5  → 金流對帳試算表 ID
    C8  → 上半月搬入清潔訂單筆數
    D8  → 下半月搬入清潔訂單筆數

打卡：統一寫入主控試算表（master_sheet.py → record_execution）
    前置作業 → task_key = "前置作業"
    00調薪   → task_key = "00調薪"

找清潔承攬檔案：
    Drive 路徑：{root_folder_id}/{period}/{period}清潔承攬-{region}
    呼叫 find_cleaning_file(root_folder_id, period, region) 取得 file_id
"""

from __future__ import annotations

import re
import time
from datetime import datetime
from typing import List, Tuple

import gspread
from gspread.utils import rowcol_to_a1

from modules.auth import get_gspread_client
from modules.drive_helper import find_file_in_folder
from modules.master_sheet import record_execution
from modules.common_process import run_common_process


# ──────────────────────────────────────────────────────────────
# 常數
# ──────────────────────────────────────────────────────────────

SUMMARY_START = 4     # 場次時數薪資總表資料起始列
SUMMARY_END   = 120   # 場次時數薪資總表資料結束列

TS_FMT = "%Y/%m/%d %H:%M"


# ──────────────────────────────────────────────────────────────
# 找清潔承攬檔案
# ──────────────────────────────────────────────────────────────

def find_cleaning_file(root_folder_id: str, period: str, region: str) -> str:
    """
    從 Drive 找到清潔承攬試算表 ID。
    路徑：{root_folder_id}/{period}/{period}清潔承攬-{region}

    Args:
        root_folder_id: config.yaml 中該地區的 root_folder_id
        period:         期別字串，如 "202605-1"
        region:         地區名稱，如 "新北"

    Returns:
        Google Sheets 檔案 ID

    Raises:
        FileNotFoundError: 找不到期別資料夾或清潔承攬檔案
    """
    from modules.auth import get_drive_service
    drive = get_drive_service()

    # 1. 在根目錄找期別資料夾
    period_folder = find_file_in_folder(
        drive, root_folder_id, period, mime="application/vnd.google-apps.folder"
    )
    if not period_folder:
        raise FileNotFoundError(f"找不到期別資料夾：{period}（根目錄 {root_folder_id}）")

    # 2. 在期別資料夾找清潔承攬試算表
    file_name = f"{period}清潔承攬-{region}"
    file_id = find_file_in_folder(
        drive, period_folder["id"], file_name,
        mime="application/vnd.google-apps.spreadsheet"
    )
    if not file_id:
        raise FileNotFoundError(f"找不到清潔承攬檔案：{file_name}")

    return file_id["id"]


# ──────────────────────────────────────────────────────────────
# 工具函數
# ──────────────────────────────────────────────────────────────

def _now_ts() -> str:
    return datetime.now().strftime(TS_FMT)


def _col_letter(n: int) -> str:
    """欄號（1-based）→ 欄字母，如 12 → 'L'"""
    result = ""
    while n > 0:
        n, r = divmod(n - 1, 26)
        result = chr(65 + r) + result
    return result


def _last_nonempty_row(ws: gspread.Worksheet, col: int = 2) -> int:
    """找指定欄（1-based）最後一筆非空白列號，找不到回傳 0。"""
    vals = ws.col_values(col)
    for i in range(len(vals) - 1, -1, -1):
        if str(vals[i]).strip():
            return i + 1
    return 0


def _punch_exec(ws_exec: gspread.Worksheet, row: int, is_first_half: bool) -> str:
    """寫入 exec 工作表的完成時間戳（C 欄=上半月，D 欄=下半月）"""
    col = 3 if is_first_half else 4
    ts  = _now_ts()
    ws_exec.update_cell(row, col, ts)
    return ts


def _log(log_lines: List[str], msg: str):
    log_lines.append(msg)


def _get_first_empty_by_col_b(ws: gspread.Worksheet) -> int:
    """找 B 欄最後非空白列的下一列。"""
    last = _last_nonempty_row(ws, col=2)
    return max(2, last + 1)


def _pad_row(row: list, width: int = 62) -> list:
    """補齊列寬到指定欄數。"""
    return row + [""] * (width - len(row)) if len(row) < width else row


# ──────────────────────────────────────────────────────────────
# 底色工具
# ──────────────────────────────────────────────────────────────

def _get_backgrounds(
    ws: gspread.Worksheet,
    start_row: int,
    start_col: int,
    num_rows: int,
    num_cols: int,
) -> List[List[str]]:
    """透過 Sheets API 取底色，回傳 num_rows × num_cols 的 hex 色碼陣列。"""
    try:
        a1_start = rowcol_to_a1(start_row, start_col)
        a1_end   = rowcol_to_a1(start_row + num_rows - 1, start_col + num_cols - 1)
        resp = ws.spreadsheet.client.request(
            "get",
            f"https://sheets.googleapis.com/v4/spreadsheets/{ws.spreadsheet.id}",
            params={
                "ranges": f"'{ws.title}'!{a1_start}:{a1_end}",
                "fields": "sheets(data(rowData(values(userEnteredFormat/backgroundColor))))",
            },
        ).json()
        rows_data = (
            resp.get("sheets", [{}])[0]
                .get("data", [{}])[0]
                .get("rowData", [])
        )
        bgs = []
        for row_d in rows_data:
            row_bg = []
            for cell in row_d.get("values", []):
                bg = cell.get("userEnteredFormat", {}).get("backgroundColor", {})
                rv = int(round(bg.get("red",   1) * 255))
                gv = int(round(bg.get("green", 1) * 255))
                bv = int(round(bg.get("blue",  1) * 255))
                row_bg.append(
                    f"#{rv:02x}{gv:02x}{bv:02x}"
                    if (rv, gv, bv) != (255, 255, 255) else ""
                )
            while len(row_bg) < num_cols:
                row_bg.append("")
            bgs.append(row_bg)
        while len(bgs) < num_rows:
            bgs.append([""] * num_cols)
        return bgs
    except Exception:
        return [[""] * num_cols for _ in range(num_rows)]


def _apply_backgrounds(
    ws: gspread.Worksheet,
    start_row: int,
    start_col: int,
    bgs: List[List[str]],
):
    """批次套用底色（只處理非空白、非白色的儲存格）。"""
    if not bgs:
        return
    requests = []
    for r_i, row_bg in enumerate(bgs):
        for c_i, color in enumerate(row_bg):
            if not color or color.lower() in ("#ffffff", ""):
                continue
            hex_c = color.lstrip("#")
            if len(hex_c) != 6:
                continue
            red   = int(hex_c[0:2], 16) / 255
            green = int(hex_c[2:4], 16) / 255
            blue  = int(hex_c[4:6], 16) / 255
            requests.append({
                "repeatCell": {
                    "range": {
                        "sheetId":          ws.id,
                        "startRowIndex":    start_row - 1 + r_i,
                        "endRowIndex":      start_row + r_i,
                        "startColumnIndex": start_col - 1 + c_i,
                        "endColumnIndex":   start_col + c_i,
                    },
                    "cell": {
                        "userEnteredFormat": {
                            "backgroundColor": {
                                "red": red, "green": green, "blue": blue
                            }
                        }
                    },
                    "fields": "userEnteredFormat.backgroundColor",
                }
            })
    if requests:
        ws.spreadsheet.batch_update({"requests": requests})


# ──────────────────────────────────────────────────────────────
# 前置作業
# ──────────────────────────────────────────────────────────────

def run_preparation(
    cleaning_file_id: str,
    region: str,
    period: str,
    is_first_half: bool,
    log: List[str],
) -> bool:
    """
    前置作業主函數。

    Args:
        cleaning_file_id: 清潔承攬試算表 ID
        region:           地區名稱（用於主控打卡）
        period:           期別字串，如 "202605-1"
        is_first_half:    是否為上半月
        log:              日誌列表（in-place append）

    Returns:
        True 成功，False 失敗
    """
    label = "上半月" if is_first_half else "下半月"
    _log(log, f"▶ 前置作業 {label} 開始")

    try:
        gc = get_gspread_client()
        ss = gc.open_by_key(cleaning_file_id)

        ws_exec    = ss.worksheet("執行")
        ws_salary  = ss.worksheet("薪資表")
        ws_revenue = ss.worksheet("清潔營收明細")
        ws_order   = ss.worksheet("清潔訂單")
        ws_proj    = ss.worksheet("專案訂單")

        # 讀取本期搬入筆數
        count_cell  = "C8" if is_first_half else "D8"
        period_count = int(ws_exec.acell(count_cell).value or 0)

        # ── 步驟1：薪資表特定列處理 ──────────────────────────
        _log(log, "  步驟1：薪資表特定列處理")
        _prep_step1_salary_rows(ws_salary, is_first_half, log)

        # ── 步驟2：讀取清潔營收明細 ──────────────────────────
        _log(log, "  步驟2：讀取清潔營收明細")
        revenue_rows, revenue_bgs = _prep_step2_read_revenue(
            ws_revenue, period_count, log
        )
        if not revenue_rows:
            raise ValueError(
                "清潔營收明細中未找到本期資料，請確認金流對帳已完成分類搬入"
            )

        # ── 步驟3：清空 / 找接續列 ───────────────────────────
        _log(log, "  步驟3：清空或接續訂單工作表")
        order_start = _prep_step3_prepare_sheets(ws_order, ws_proj, is_first_half, log)

        # ── 步驟4：分流搬入 ──────────────────────────────────
        _log(log, "  步驟4：分流搬入")
        n_count, p_count = _prep_step4_split_paste(
            ws_order, ws_proj, revenue_rows, revenue_bgs,
            order_start, is_first_half, log
        )
        _log(log, f"  步驟4 完成：清潔訂單 {n_count} 筆，專案訂單 {p_count} 筆")

        # ── 步驟5：移除檸檬人 ────────────────────────────────
        _log(log, "  步驟5：移除清潔訂單中的檸檬人")
        lemon_count = _prep_step5_remove_lemon(ws_order, log)
        _log(log, f"  步驟5 完成：處理 {lemon_count} 筆")

        # ── 步驟6：共用 QRS 流程 ─────────────────────────────
        _log(log, "  步驟6：執行共用 QRS 流程")
        run_common_process(cleaning_file_id, "清潔訂單")
        _log(log, "  步驟6 完成")

        # ── 打卡 ─────────────────────────────────────────────
        ts = _now_ts()
        record_execution(region, period, "前置作業", ts)
        _log(log, f"✅ 前置作業 {label} 完成｜{ts}")
        return True

    except Exception as e:
        _log(log, f"❌ 前置作業失敗：{e}")
        return False


# ── 步驟1 ────────────────────────────────────────────────────

def _prep_step1_salary_rows(
    ws_salary: gspread.Worksheet,
    is_first_half: bool,
    log: List[str],
):
    """
    上半月：清空 L2039 及 L2043（整列至最後欄）
    下半月：L2044→L2043、L2038→L2039（貼值）
    """
    last_col    = ws_salary.col_count
    last_letter = _col_letter(last_col)

    def _range(row: int) -> str:
        return f"L{row}:{last_letter}{row}"

    if is_first_half:
        ws_salary.batch_clear([_range(2039), _range(2043)])
        _log(log, "    上半月：已清空 L2039 及 L2043")
    else:
        v2044 = ws_salary.get(_range(2044)) or [[]]
        ws_salary.update(_range(2043), v2044, value_input_option="RAW")
        v2038 = ws_salary.get(_range(2038)) or [[]]
        ws_salary.update(_range(2039), v2038, value_input_option="RAW")
        _log(log, "    下半月：2044→2043、2038→2039 貼值完成")


# ── 步驟2 ────────────────────────────────────────────────────

def _prep_step2_read_revenue(
    ws_revenue: gspread.Worksheet,
    period_count: int,
    log: List[str],
) -> Tuple[List[List], List[List]]:
    """
    從清潔營收明細 B 欄最後非空白列往上數 period_count 筆。
    """
    if period_count <= 0:
        _log(log, "    ⚠️ exec C8/D8 筆數為 0")
        return [], []

    last_row = _last_nonempty_row(ws_revenue, col=2)
    if last_row < 2:
        return [], []

    start_row = max(2, last_row - period_count + 1)
    num_rows  = last_row - start_row + 1

    raw = ws_revenue.get(f"A{start_row}:BJ{last_row}") or []
    values = [_pad_row(r) for r in raw]

    bgs = _get_backgrounds(ws_revenue, start_row, 1, num_rows, 62)
    _log(log, f"    讀取第 {start_row} 列起，共 {num_rows} 筆")
    return values, bgs


# ── 步驟3 ────────────────────────────────────────────────────

def _prep_step3_prepare_sheets(
    ws_order: gspread.Worksheet,
    ws_proj: gspread.Worksheet,
    is_first_half: bool,
    log: List[str],
) -> int:
    if is_first_half:
        ws_order.batch_clear(["A2:BJ"])
        ws_proj.batch_clear(["A2:BJ"])
        _log(log, "    上半月：清潔訂單 & 專案訂單已清空")
        return 2
    else:
        start = _get_first_empty_by_col_b(ws_order)
        _log(log, f"    下半月：清潔訂單接續列 = {start}")
        return start


# ── 步驟4 ────────────────────────────────────────────────────

def _prep_step4_split_paste(
    ws_order: gspread.Worksheet,
    ws_proj: gspread.Worksheet,
    values: List[List],
    bgs: List[List],
    order_start: int,
    is_first_half: bool,
    log: List[str],
) -> Tuple[int, int]:
    """
    Y 欄（index 24）= "1299" → 專案訂單
    其餘 → 清潔訂單
    """
    normal_v, normal_bg = [], []
    proj_v,   proj_bg   = [], []

    for i, row in enumerate(values):
        y_val = str(row[24]).strip() if len(row) > 24 else ""
        bg    = bgs[i] if i < len(bgs) else [""] * 62
        if y_val == "1299":
            proj_v.append(row)
            proj_bg.append(bg)
        else:
            normal_v.append(row)
            normal_bg.append(bg)

    def _paste(ws: gspread.Worksheet, start: int, data: List[List], b: List[List]):
        if not data:
            return
        end_row = start + len(data) - 1
        ws.update(f"A{start}:BJ{end_row}", data, value_input_option="RAW")
        _apply_backgrounds(ws, start, 1, b)

    _paste(ws_order, order_start, normal_v, normal_bg)

    proj_start = 2 if is_first_half else _get_first_empty_by_col_b(ws_proj)
    _paste(ws_proj, proj_start, proj_v, proj_bg)

    return len(normal_v), len(proj_v)


# ── 步驟5 ────────────────────────────────────────────────────

def _prep_step5_remove_lemon(
    ws_order: gspread.Worksheet,
    log: List[str],
) -> int:
    """
    AH 欄（col 34）移除「檸檬人」；AH 清空後 J 欄（col 10）連帶清空。
    """
    all_ah = ws_order.col_values(34)  # AH = col 34
    updates = []
    count   = 0

    for i in range(1, len(all_ah)):  # 跳過標題列 (index 0)
        ah = str(all_ah[i]) if i < len(all_ah) else ""
        if "檸檬人" not in ah:
            continue
        cleaned = " X ".join(
            s.strip()
            for s in re.split(r"\s*[Xx×Ｘ]\s*", ah)
            if s.strip() and "檸檬人" not in s
        )
        row = i + 1  # col_values 從 index 0 = 第 1 列
        updates.append({"range": f"AH{row}", "values": [[cleaned]]})
        if not cleaned:
            updates.append({"range": f"J{row}", "values": [[""]]})
        count += 1

    if updates:
        ws_order.spreadsheet.values_batch_update({
            "valueInputOption": "RAW",
            "data": updates,
        })
    return count


# ──────────────────────────────────────────────────────────────
# 00 調薪
# ──────────────────────────────────────────────────────────────

def run_adjustment(
    cleaning_file_id: str,
    region: str,
    period: str,
    is_first_half: bool,
    log: List[str],
) -> bool:
    """
    00調薪主函數。

    Args:
        cleaning_file_id: 清潔承攬試算表 ID
        region:           地區名稱（用於主控打卡）
        period:           期別字串，如 "202605-1"
        is_first_half:    是否為上半月
        log:              日誌列表（in-place append）

    Returns:
        True 成功，False 失敗
    """
    label = "上半月" if is_first_half else "下半月"
    _log(log, f"▶ 00調薪 {label} 開始")

    try:
        gc = get_gspread_client()
        ss = gc.open_by_key(cleaning_file_id)

        ws_exec    = ss.worksheet("執行")
        ws_adjust  = ss.worksheet("00調薪")
        ws_salary  = ss.worksheet("薪資表")
        ws_summary = ss.worksheet("場次時數薪資總表")

        # 讀取 exec 關鍵儲存格
        yyyymm    = str(ws_exec.acell("B1").value or "").strip()
        salary_id = str(ws_exec.acell("C3").value or "").strip()
        roster_id = str(ws_exec.acell("C4").value or "").strip()

        if len(yyyymm) != 6 or not yyyymm.isdigit():
            raise ValueError(f"exec B1 期別格式錯誤：{yyyymm!r}，應為 YYYYMM")
        if not salary_id:
            raise ValueError("exec C3 薪資試算表 ID 為空")
        if not roster_id:
            raise ValueError("exec C4 名冊試算表 ID 為空")

        # ── 步驟1：上半月清空 S3:AL ──────────────────────────
        if is_first_half:
            _log(log, "  步驟1：清空 S3:AL")
            ws_adjust.batch_clear(["S3:AL"])

        # ── 步驟2：匯入專員名冊 → S3:W ──────────────────────
        _log(log, "  步驟2：匯入專員名冊 S3:W")
        _adj_import_roster(ws_adjust, roster_id, yyyymm, log)

        # ── 步驟3：匯入調薪資料 → Y3:AF ─────────────────────
        _log(log, "  步驟3：匯入調薪資料 Y3:AF")
        _adj_import_salary_k_r(ws_adjust, salary_id, yyyymm, log)

        # ── 步驟4：匯入調薪資料 → AG3:AL ────────────────────
        _log(log, "  步驟4：匯入調薪資料 AG3:AL")
        _adj_import_salary_aa_af(ws_adjust, salary_id, yyyymm, log)

        # ── 步驟5：轉為靜態值 ────────────────────────────────
        _log(log, "  步驟5：S3:AL 轉為靜態值")
        num_rows = _adj_convert_to_values(ws_adjust, log)
        if num_rows == 0:
            raise ValueError("00調薪 S 欄無有效資料，請確認 IMPORTRANGE 已授權")

        # ── 步驟6：設定 A:O 計算公式 ────────────────────────
        _log(log, "  步驟6：設定 A:O 計算公式")
        _adj_set_formulas_a_to_o(ws_adjust, num_rows, log)

        # ── 步驟7：更新場次時數薪資總表 A 欄 ────────────────
        _log(log, "  步驟7：更新場次時數薪資總表 A 欄")
        _adj_update_summary_a(ws_adjust, ws_summary, num_rows, log)

        # ── 步驟8：設定場次時數薪資總表 B-G 欄公式 ──────────
        _log(log, "  步驟8：設定場次時數薪資總表 B-G 公式")
        _adj_set_summary_b_to_g(ws_summary, num_rows, log)

        # ── 步驟9：設定場次時數薪資總表 H-K 欄 ──────────────
        _log(log, "  步驟9：設定場次時數薪資總表 H-K 欄")
        _adj_set_summary_h_to_k(ws_summary, roster_id, yyyymm, num_rows, log)

        # ── 步驟10：設定 P-Q（上）/ W-X（下）欄 ─────────────
        _log(log, "  步驟10：設定 P-Q / W-X 欄")
        _adj_set_summary_pq_or_wx(ws_summary, is_first_half, log)

        # ── 步驟11：設定 N-O（上）/ U-V（下）欄 ─────────────
        _log(log, "  步驟11：設定 N-O / U-V 欄")
        _adj_set_summary_no_or_uv(ws_summary, is_first_half, log)

        # ── 步驟12：更新薪資表 L1:1 員工名單 ────────────────
        _log(log, "  步驟12：更新薪資表 L1:1 員工名單")
        _adj_update_salary_l1(ws_adjust, ws_salary, is_first_half, log)

        # ── 步驟13：設定期別標記 E1 ──────────────────────────
        _log(log, "  步驟13：設定期別標記 E1")
        ws_adjust.update_cell(1, 5, -1 if is_first_half else -2)  # E1

        # ── 打卡 ─────────────────────────────────────────────
        ts = _now_ts()
        record_execution(region, period, "00調薪", ts)
        _log(log, f"✅ 00調薪 {label} 完成｜{ts}")
        return True

    except Exception as e:
        _log(log, f"❌ 00調薪失敗：{e}")
        return False


# ── 步驟2：匯入專員名冊 ──────────────────────────────────────

def _adj_import_roster(
    ws_adjust: gspread.Worksheet,
    roster_id: str,
    yyyymm: str,
    log: List[str],
):
    formula = (
        f'=ARRAYFORMULA(IFERROR(FILTER('
        f'IMPORTRANGE("{roster_id}","{yyyymm}專員名冊!B2:F"),'
        f'IMPORTRANGE("{roster_id}","{yyyymm}專員名冊!B2:B")<>""),""))'
    )
    ws_adjust.update_cell(3, 19, formula)  # S3 = row 3, col 19
    _log(log, f"    S3 IMPORTRANGE 已寫入（名冊 {yyyymm}）")
    time.sleep(5)


# ── 步驟3：匯入調薪資料 K:R ──────────────────────────────────

def _adj_import_salary_k_r(
    ws_adjust: gspread.Worksheet,
    salary_id: str,
    yyyymm: str,
    log: List[str],
):
    formula = (
        f'=ARRAYFORMULA(IF(S3:S="",,FILTER('
        f'IMPORTRANGE("{salary_id}","{yyyymm}專員調薪!K3:R"),'
        f'IMPORTRANGE("{salary_id}","{yyyymm}專員調薪!B3:B")=S3:S)))'
    )
    ws_adjust.update_cell(3, 25, formula)  # Y3 = row 3, col 25
    _log(log, "    Y3 IMPORTRANGE 已寫入（調薪 K:R）")
    time.sleep(5)


# ── 步驟4：匯入調薪資料 AA:AF ────────────────────────────────

def _adj_import_salary_aa_af(
    ws_adjust: gspread.Worksheet,
    salary_id: str,
    yyyymm: str,
    log: List[str],
):
    formula = (
        f'=ARRAYFORMULA(IF(S3:S="",,FILTER('
        f'IMPORTRANGE("{salary_id}","{yyyymm}專員調薪!AA3:AF"),'
        f'IMPORTRANGE("{salary_id}","{yyyymm}專員調薪!B3:B")=S3:S)))'
    )
    ws_adjust.update_cell(3, 33, formula)  # AG3 = row 3, col 33
    _log(log, "    AG3 IMPORTRANGE 已寫入（調薪 AA:AF）")
    time.sleep(5)


# ── 步驟5：轉為靜態值 ────────────────────────────────────────

def _adj_convert_to_values(
    ws_adjust: gspread.Worksheet,
    log: List[str],
) -> int:
    """
    等待 S3 有值後，將 S3:AL 整段轉為靜態值。
    回傳有效列數（S 欄非空白列數）。
    S = col 19，AL = col 38，共 20 欄。
    """
    # 等待 S3 有值（最多 30 秒）
    deadline = time.time() + 30
    while time.time() < deadline:
        v = ws_adjust.acell("S3").value
        if v and str(v).strip():
            break
        time.sleep(2)
    else:
        _log(log, "    ⚠️ 等待逾時，S3 仍為空")

    # 找有效列數
    s_vals   = ws_adjust.col_values(19)  # S 欄
    num_rows = 0
    for i in range(2, len(s_vals)):      # index 0=列1, 1=列2, 2=列3...
        if str(s_vals[i]).strip():
            num_rows = i - 1             # 相對第 3 列的偏移數
        else:
            break

    if num_rows == 0:
        _log(log, "    ⚠️ S3:S 無有效資料")
        return 0

    end_row = 2 + num_rows               # row 3 = index 2, end = 2 + num_rows
    data = ws_adjust.get(f"S3:AL{end_row}") or []
    ws_adjust.update(f"S3:AL{end_row}", data, value_input_option="RAW")
    _log(log, f"    S3:AL 轉靜態值完成（{num_rows} 列）")
    return num_rows


# ── 步驟6：設定 A:O 計算公式 ─────────────────────────────────

def _adj_set_formulas_a_to_o(
    ws_adjust: gspread.Worksheet,
    num_rows: int,
    log: List[str],
):
    """
    A=S, B=T（直接等於）
    C/D/E/G → VLOOKUP(A, $S:$AL, offset, FALSE)
        S:AL 欄偏移（S=1）：Z=8, AA=9, AC=11, AF=14
    J:O（6欄）→ FILTER($AG:$AL, $S:$S=A{r})（ARRAYFORMULA 展開至 O 欄）
    """
    batch = []
    for i in range(num_rows):
        r = i + 3
        batch.extend([
            {"range": f"A{r}", "values": [[f"=S{r}"]]},
            {"range": f"B{r}", "values": [[f"=T{r}"]]},
            {"range": f"C{r}", "values": [[
                f'=IFERROR(VLOOKUP(A{r},$S:$AL,8,FALSE),"")' ]]},  # Z
            {"range": f"D{r}", "values": [[
                f'=IFERROR(VLOOKUP(A{r},$S:$AL,9,FALSE),"")' ]]},  # AA
            {"range": f"E{r}", "values": [[
                f'=IFERROR(VLOOKUP(A{r},$S:$AL,11,FALSE),"")' ]]}, # AC
            {"range": f"G{r}", "values": [[
                f'=IFERROR(VLOOKUP(A{r},$S:$AL,14,FALSE),"")' ]]}, # AF
            {"range": f"J{r}", "values": [[
                f'=IFERROR(FILTER($AG:$AL,$S:$S=A{r}),"")' ]]},    # AG:AL
        ])

    # 每批 500 個以內
    for i in range(0, len(batch), 500):
        ws_adjust.spreadsheet.values_batch_update({
            "valueInputOption": "USER_ENTERED",
            "data": batch[i:i + 500],
        })
    _log(log, f"    A:O 公式設定完成（{num_rows} 列）")


# ── 步驟7：更新場次時數薪資總表 A 欄 ────────────────────────

def _adj_update_summary_a(
    ws_adjust: gspread.Worksheet,
    ws_summary: gspread.Worksheet,
    num_rows: int,
    log: List[str],
):
    ws_summary.batch_clear([f"A{SUMMARY_START}:A{SUMMARY_END}"])
    if num_rows <= 0:
        return
    a_vals = ws_adjust.get(f"A3:A{2 + num_rows}") or []
    ws_summary.update(
        f"A{SUMMARY_START}:A{SUMMARY_START + num_rows - 1}",
        a_vals, value_input_option="RAW"
    )
    _log(log, f"    A{SUMMARY_START} 起寫入 {num_rows} 筆姓名")


# ── 步驟8：設定場次時數薪資總表 B-G 欄公式 ──────────────────

def _adj_set_summary_b_to_g(
    ws_summary: gspread.Worksheet,
    num_rows: int,
    log: List[str],
):
    if num_rows <= 0:
        return
    batch = []
    for i in range(num_rows):
        r = i + SUMMARY_START
        batch.extend([
            {"range": f"B{r}", "values": [[
                f"=HLOOKUP(A{r},'薪資表'!$1:$2001,2001,FALSE)"]]},
            {"range": f"C{r}", "values": [[
                f"=HLOOKUP(A{r},'薪資表'!$1:$2013,2013,FALSE)"]]},
            {"range": f"D{r}", "values": [[
                f"=IF(AND(E{r}=0,'薪資單'!$AD$1=$D$1),"
                f"HLOOKUP($A{r},'薪資表'!$1:$2047,2042,FALSE),"
                f"HLOOKUP($A{r},'薪資表'!$1:$2047,2043,FALSE))"]]},
            {"range": f"E{r}", "values": [[
                f"=IF('薪資單'!$AD$1=$E$1,"
                f"HLOOKUP($A{r},'薪資表'!$1:$2047,2044,FALSE),0)"]]},
            {"range": f"F{r}", "values": [[
                f"=HLOOKUP(A{r},'薪資表'!$1:$2042,2039,FALSE)"
                f"+HLOOKUP(A{r},'薪資表'!$1:$2042,2040,FALSE)"]]},
            {"range": f"G{r}", "values": [[
                f"=HLOOKUP($A{r},'薪資表'!$1:$2042,2041,FALSE)"
                f"+HLOOKUP($A{r},'薪資表'!$1:$2042,2042,FALSE)"]]},
        ])
    for i in range(0, len(batch), 500):
        ws_summary.spreadsheet.values_batch_update({
            "valueInputOption": "USER_ENTERED",
            "data": batch[i:i + 500],
        })
    _log(log, f"    B-G 公式設定完成（{num_rows} 列）")


# ── 步驟9：設定場次時數薪資總表 H-K 欄 ──────────────────────

def _adj_set_summary_h_to_k(
    ws_summary: gspread.Worksheet,
    roster_id: str,
    yyyymm: str,
    num_rows: int,
    log: List[str],
):
    ws_summary.batch_clear([f"H{SUMMARY_START}:K{SUMMARY_END}"])
    if num_rows <= 0:
        return
    batch = []
    for i in range(num_rows):
        r = i + SUMMARY_START
        batch.append({"range": f"H{r}", "values": [[f"=A{r}"]]})
        batch.append({"range": f"I{r}", "values": [[
            f'=IFERROR(FILTER('
            f'IMPORTRANGE("{roster_id}","{yyyymm}專員名冊!G2:I"),'
            f'IMPORTRANGE("{roster_id}","{yyyymm}專員名冊!B2:B")=H{r}),"")' ]]})
    for i in range(0, len(batch), 500):
        ws_summary.spreadsheet.values_batch_update({
            "valueInputOption": "USER_ENTERED",
            "data": batch[i:i + 500],
        })
    _log(log, "    H-K 欄設定完成")


# ── 步驟10：P-Q（上）/ W-X（下）欄 ──────────────────────────

def _adj_set_summary_pq_or_wx(
    ws_summary: gspread.Worksheet,
    is_first_half: bool,
    log: List[str],
):
    count = SUMMARY_END - SUMMARY_START + 1

    def _get(col_letter: str) -> List[List]:
        return ws_summary.get(
            f"{col_letter}{SUMMARY_START}:{col_letter}{SUMMARY_END}"
        ) or []

    if is_first_half:
        ws_summary.batch_clear(["N4:Q"])
        d_col = _get("D")
        a_col = _get("A")
        p_data, q_data = [], []
        for i in range(count):
            d = d_col[i][0] if i < len(d_col) and d_col[i] else ""
            a = a_col[i][0] if i < len(a_col) and a_col[i] else ""
            try:
                d_val = float(d) if d else 0
            except ValueError:
                d_val = 0
            if a and d_val > 0:
                p_data.append([d])
                q_data.append([a])
        if p_data:
            ws_summary.update(
                f"P{SUMMARY_START}:P{SUMMARY_START + len(p_data) - 1}",
                p_data, value_input_option="RAW"
            )
            ws_summary.update(
                f"Q{SUMMARY_START}:Q{SUMMARY_START + len(q_data) - 1}",
                q_data, value_input_option="RAW"
            )
        _log(log, f"    上半月 P-Q 寫入 {len(p_data)} 筆")

    else:
        ws_summary.batch_clear(["U4:X"])
        e_col = _get("E")
        a_col = _get("A")
        w_data, x_data = [], []
        for i in range(count):
            e = e_col[i][0] if i < len(e_col) and e_col[i] else ""
            a = a_col[i][0] if i < len(a_col) and a_col[i] else ""
            try:
                e_val = float(e) if e else 0
            except ValueError:
                e_val = 0
            if a and e_val > 0:
                w_data.append([e])
                x_data.append([a])
        if w_data:
            ws_summary.update(
                f"W{SUMMARY_START}:W{SUMMARY_START + len(w_data) - 1}",
                w_data, value_input_option="RAW"
            )
            ws_summary.update(
                f"X{SUMMARY_START}:X{SUMMARY_START + len(x_data) - 1}",
                x_data, value_input_option="RAW"
            )
        _log(log, f"    下半月 W-X 寫入 {len(w_data)} 筆")


# ── 步驟11：N-O（上）/ U-V（下）欄 ──────────────────────────

def _adj_set_summary_no_or_uv(
    ws_summary: gspread.Worksheet,
    is_first_half: bool,
    log: List[str],
):
    """
    上半月 N-O：Q 欄姓名 → 對應 H 欄 → 取 I/J 欄
    下半月 U-V：X 欄姓名 → 對應 H 欄 → 取 I/J 欄
    """
    count = SUMMARY_END - SUMMARY_START + 1

    def _get(col: str) -> List[List]:
        return ws_summary.get(
            f"{col}{SUMMARY_START}:{col}{SUMMARY_END}"
        ) or []

    h_col = _get("H")
    i_col = _get("I")
    j_col = _get("J")

    # H 欄姓名 → 行索引
    h_map: dict[str, int] = {}
    for idx, row in enumerate(h_col):
        name = str(row[0]).strip() if row else ""
        if name:
            h_map[name] = idx

    ref_col  = _get("Q") if is_first_half else _get("X")
    out_cols = ("N", "O") if is_first_half else ("U", "V")
    batch    = []
    matched  = 0

    for i in range(count):
        name = str(ref_col[i][0]).strip() if i < len(ref_col) and ref_col[i] else ""
        if not name:
            continue
        h_idx = h_map.get(name)
        if h_idx is None:
            continue
        r    = i + SUMMARY_START
        i_v  = i_col[h_idx][0] if h_idx < len(i_col) and i_col[h_idx] else ""
        j_v  = j_col[h_idx][0] if h_idx < len(j_col) and j_col[h_idx] else ""
        batch.extend([
            {"range": f"{out_cols[0]}{r}", "values": [[i_v]]},
            {"range": f"{out_cols[1]}{r}", "values": [[j_v]]},
        ])
        matched += 1

    if batch:
        ws_summary.spreadsheet.values_batch_update({
            "valueInputOption": "RAW",
            "data": batch,
        })
    half_label = "上半月 N-O" if is_first_half else "下半月 U-V"
    _log(log, f"    {half_label} 寫入 {matched} 筆")


# ── 步驟12：更新薪資表 L1:1 員工名單 ────────────────────────

def _adj_update_salary_l1(
    ws_adjust: gspread.Worksheet,
    ws_salary: gspread.Worksheet,
    is_first_half: bool,
    log: List[str],
):
    # 從 00調薪 S 欄（col 19）取非空白姓名（跳過前兩列）
    s_vals    = ws_adjust.col_values(19)
    names     = [v for v in s_vals[2:] if v and str(v).strip()]
    new_count = len(names)

    if new_count == 0:
        _log(log, "    ⚠️ S 欄無有效姓名，跳過 L1:1 更新")
        return

    # 目前 L1:1 員工數（L = col 12, index 11）
    l1_row   = ws_salary.row_values(1)
    old_count = sum(1 for v in l1_row[11:] if v and str(v).strip())
    diff      = new_count - old_count

    # 清空舊的 L1:1
    if old_count > 0:
        end_ltr = _col_letter(11 + old_count)
        ws_salary.batch_clear([f"L1:{end_ltr}1"])

    # 寫入新名單
    end_ltr = _col_letter(11 + new_count)
    ws_salary.update(f"L1:{end_ltr}1", [names], value_input_option="RAW")
    _log(log, f"    L1:1 更新：{old_count} → {new_count} 人（diff={diff}）")

    # 若有新增員工，複製 L 欄樣板公式到新欄
    if diff > 0:
        _copy_salary_formulas(ws_salary, old_count, diff, is_first_half, log)

    # 下半月有新增時，清空特定列
    if not is_first_half and diff > 0:
        s_col   = 12 + old_count       # 新增欄起始（L=12）
        e_col   = s_col + diff - 1
        s_ltr   = _col_letter(s_col)
        e_ltr   = _col_letter(e_col)
        ws_salary.batch_clear([
            f"{s_ltr}2039:{e_ltr}2039",
            f"{s_ltr}2043:{e_ltr}2043",
        ])
        _log(log, "    下半月：已清空新增欄位的列 2039 及 2043")


def _copy_salary_formulas(
    ws_salary: gspread.Worksheet,
    old_count: int,
    diff: int,
    is_first_half: bool,
    log: List[str],
):
    """
    從 L 欄（樣板，col 12）複製公式到 (L + old_count) 起的 diff 欄。
    下半月跳過列 2039、2043。
    """
    SKIP = {2039, 2043} if not is_first_half else set()
    SRC_COL   = 12    # L
    START_ROW = 2
    END_ROW   = 2044
    num_rows  = END_ROW - START_ROW + 1

    src_formulas = ws_salary.get(f"L{START_ROW}:L{END_ROW}") or []
    batch = []

    for c in range(diff):
        tgt_col = SRC_COL + old_count + c
        tgt_ltr = _col_letter(tgt_col)

        for i, row_f in enumerate(src_formulas):
            actual_row = START_ROW + i
            if actual_row in SKIP:
                continue
            formula = row_f[0] if row_f else ""
            if not formula:
                continue
            # 替換公式中「L」欄字母（前後非字母）為目標欄字母
            new_formula = re.sub(r'(?<![A-Z])L(?=\d)', tgt_ltr, formula)
            batch.append({
                "range": f"{tgt_ltr}{actual_row}",
                "values": [[new_formula]],
            })

    if batch:
        for i in range(0, len(batch), 500):
            ws_salary.spreadsheet.values_batch_update({
                "valueInputOption": "USER_ENTERED",
                "data": batch[i:i + 500],
            })
        _log(log, f"    薪資公式複製完成（{diff} 欄，{len(batch)} 格）")
