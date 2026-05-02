"""
金流對帳模組
對應 GAS：金流對帳.gs

七個功能：
① 建立期別資料夾與檔案（→ drive_helper）
② 期別訂單轉檔（ZIP解壓縮 + 轉 Google Sheet）
③ 訂單搬運到範本
④ 範本加工（排序、異常標記、儲值金、F/G拆解）
⑤ 分類搬運（清潔/水洗/收納/家電/座椅/地毯）
⑥ 搬運退款＋預收
⑦ 搬運發票＋藍新
"""

import re
import pandas as pd
import streamlit as st
from modules.auth import get_drive_service
from modules.period_utils import get_file_name, is_first_half
from modules.drive_helper import (
    get_folder_by_name,
    find_file_in_folder,
    convert_period_payment_files,
    create_period_folder_and_files,
)
from modules.sheet_helper import (
    open_spreadsheet,
    get_all_data,
    get_paste_row,
    paste_data,
    normalize_all_rows,
    find_last_non_empty_row,
)


# ═══════════════════════════════════════
# 共用：找期別資料夾 ID
# ═══════════════════════════════════════

def _get_period_folder_id(root_folder_id: str, period: str) -> str:
    """找期別資料夾，找不到就拋出例外"""
    drive = get_drive_service()
    folder = get_folder_by_name(drive, root_folder_id, period)
    if not folder:
        raise Exception(f"找不到期別資料夾：{period}，請先執行「建立期別資料夾」")
    return folder["id"]


def _get_period_file_id(root_folder_id: str, period: str, label: str, region_name: str) -> str:
    """找期別特定類型檔案 ID，找不到就拋出例外"""
    drive = get_drive_service()
    folder_id = _get_period_folder_id(root_folder_id, period)
    file_name = get_file_name(period, label, region_name)
    file = find_file_in_folder(drive, folder_id, file_name)
    if not file:
        raise Exception(f"找不到檔案：{file_name}")
    return file["id"]


def _find_sheet_by_keyword(folder_id: str, keyword: str) -> str | None:
    """在資料夾中找包含關鍵字的 Google Sheet，回傳 ID 或 None"""
    drive = get_drive_service()
    q = (
        f"'{folder_id}' in parents and "
        f"mimeType='application/vnd.google-apps.spreadsheet' and "
        f"trashed=false"
    )
    res = drive.files().list(q=q, fields="files(id, name)").execute()
    for f in res.get("files", []):
        if keyword in f["name"]:
            return f["id"]
    return None


# ═══════════════════════════════════════
# ① 建立期別資料夾與檔案
# ═══════════════════════════════════════

def create_period(root_folder_id: str, period: str, region_name: str) -> dict:
    """建立期別資料夾並複製上一期四類檔案"""
    return create_period_folder_and_files(root_folder_id, period, region_name)


# ═══════════════════════════════════════
# ② 期別訂單轉檔
# ═══════════════════════════════════════

def convert_period_orders(root_folder_id: str, period: str, region_name: str) -> dict:
    """
    在期別資料夾中找所有金流相關檔案
    ZIP → 解壓縮 → 轉 Google Sheet
    Excel/CSV → 轉 Google Sheet
    蓋掉同名舊檔
    """
    folder_id = _get_period_folder_id(root_folder_id, period)
    return convert_period_payment_files(folder_id, period, region_name)


# ═══════════════════════════════════════
# ③ 訂單搬運到範本
# ═══════════════════════════════════════

def copy_orders_to_template(
    root_folder_id: str,
    period: str,
    region_name: str,
) -> int:
    """
    找期別金流對帳試算表的第一個工作表
    搬運資料到「範本」工作表
    上半月：清空再貼；下半月：接在最後一筆後面
    """
    file_id = _get_period_file_id(root_folder_id, period, "金流對帳", region_name)
    ss = open_spreadsheet(file_id)

    # 來源：第一個工作表
    source_sheet = ss.worksheets()[0]
    template_sheet = ss.worksheet("範本")

    # 讀取資料
    data = get_all_data(source_sheet, "A2", "BJ")
    if not data:
        raise Exception("來源訂單無資料")

    # 決定貼上位置
    first_half = is_first_half(period)
    start_row = get_paste_row(template_sheet, first_half)

    # 貼上
    count = paste_data(template_sheet, start_row, data)
    return count


# ═══════════════════════════════════════
# ④ 範本加工
# ═══════════════════════════════════════

ABNORMAL_KEYWORDS = ["異動", "請假", "補做", "加時", "減時", "遲到", "薪資", "未服務", "加洗"]


def process_template(
    root_folder_id: str,
    period: str,
    region_name: str,
) -> dict:
    """
    範本加工：
    1. 排序（E欄→H欄→M欄）
    2. 異常標記（AP/AY欄含關鍵字 → K欄寫入，整列標橘）
    3. 水洗類別文字去重
    4. 儲值金標記（E欄含VIP券/儲值金 → A欄寫「儲值金」）
    5. F/G欄服務項目拆解多列
    """
    file_id = _get_period_file_id(root_folder_id, period, "金流對帳", region_name)
    ss = open_spreadsheet(file_id)
    sheet = ss.worksheet("範本")

    data = get_all_data(sheet, "A2", "BJ")
    if not data:
        return {"sort_count": 0, "mark_count": 0, "expand_count": 0}

    # 補齊欄位到 62 欄
    max_cols = 62
    data = [row + [""] * (max_cols - len(row)) for row in data]

    df = pd.DataFrame(data)

    # ── 排序：E(4) → H(7) → M(12) ──
    df = df.sort_values(by=[4, 7, 12], ascending=True).reset_index(drop=True)
    sort_count = len(df)

    # ── 異常標記 ──
    mark_count = 0
    orange_rows = []
    for idx, row in df.iterrows():
        ap = str(row[41]) if len(row) > 41 else ""
        ay = str(row[50]) if len(row) > 50 else ""
        combined = (ap + " " + ay).strip()
        if any(kw in combined for kw in ABNORMAL_KEYWORDS):
            df.at[idx, 10] = combined  # K欄
            orange_rows.append(idx)
            mark_count += 1

    # ── 水洗類別去重 ──
    for idx, row in df.iterrows():
        e_text = str(row[4])
        if "3水洗：" in e_text:
            df.at[idx, 4] = _dedupe_wash_text(e_text)

    # ── 儲值金標記 ──
    for idx, row in df.iterrows():
        e_text = str(row[4])
        if "VIP券" in e_text or "儲值金" in e_text:
            df.at[idx, 0] = "儲值金"

    # ── F/G 欄拆解多列 ──
    expanded_data, expand_count = _expand_fg_rows(df)

    # ── 寫回 ──
    sheet.batch_clear([f"A2:BJ{len(data) + 10}"])
    if expanded_data:
        sheet.update("A2", expanded_data, value_input_option="USER_ENTERED")

    return {
        "sort_count": sort_count,
        "mark_count": mark_count,
        "expand_count": expand_count,
    }


def _dedupe_wash_text(text: str) -> str:
    """移除水洗類別文字中的重複內容"""
    prefix = "3水洗："
    if prefix not in text:
        return text
    idx = text.index(prefix)
    head = text[:idx + len(prefix)]
    tail = text[idx + len(prefix):].strip()
    half = len(tail) // 2
    if half > 0 and tail[:half] == tail[half:]:
        return head + tail[:half]
    return text.replace("噴抽水洗＋除蟎噴抽水洗＋除蟎", "噴抽水洗＋除蟎")


def _parse_service_items(text: str) -> list[dict]:
    """
    解析 F 欄服務項目文字，拆成多個 {name, qty}
    支援：換行、頓號、逗號、X 數量
    """
    raw = str(text).replace("　", " ").replace("Ｘ", "X").strip()
    if not raw:
        return []

    lines = re.split(r"[\n、,，/；;]", raw)
    items = []
    for line in lines:
        line = line.strip().strip('"')
        if not line:
            continue
        match = re.match(r"^(.*?)\s*[Xx×＊*]\s*(\d+)\s*$", line)
        if match:
            items.append({"name": match.group(1).strip(), "qty": match.group(2)})
        else:
            items.append({"name": line, "qty": "1"})
    return items


EXPANDABLE_TYPES = ["水洗", "家電", "座椅", "收納", "地毯", "其他"]


def _expand_fg_rows(df: pd.DataFrame) -> tuple[list, int]:
    """
    F/G 欄服務項目拆解：
    一筆訂單有多個服務項目時，拆成多列
    第一列保留原本的 B 欄訂單編號
    後續列的訂單編號加 -1, -2...
    """
    output = []
    expand_count = 0

    for idx, row in df.iterrows():
        e_text = str(row[4])
        f_text = str(row[5])
        order_id = str(row[1])

        is_expandable = any(t in e_text for t in EXPANDABLE_TYPES)

        if not is_expandable or not f_text.strip():
            output.append(row.tolist())
            continue

        items = _parse_service_items(f_text)
        if len(items) <= 1:
            output.append(row.tolist())
            continue

        # 拆解多列
        for i, item in enumerate(items):
            new_row = row.tolist().copy()
            new_row[5] = item["name"]   # F欄：服務項目名稱
            new_row[6] = item["qty"]    # G欄：數量
            if i > 0:
                new_row[1] = f"{order_id}-{i}"  # B欄：子訂單編號
            output.append(new_row)
            expand_count += 1

    return output, expand_count


# ═══════════════════════════════════════
# ⑤ 分類搬運
# ═══════════════════════════════════════

CLASSIFY_MAP = {
    "清潔": "清潔營收明細",
    "水洗": "水洗營收明細",
    "收納": "收納營收明細",
    "家電": "家電營收明細",
    "座椅": "座椅營收明細",
    "地毯": "地毯營收明細",
}


def copy_classified_data(
    root_folder_id: str,
    period: str,
    region_name: str,
) -> dict:
    """
    從金流對帳範本工作表依 E 欄類別分類
    清潔 → 清潔承攬「清潔營收明細」
    其他 → 其他承攬對應工作表
    """
    reconciliation_id = _get_period_file_id(root_folder_id, period, "金流對帳", region_name)
    cleaning_id = _get_period_file_id(root_folder_id, period, "清潔承攬", region_name)
    other_id = _get_period_file_id(root_folder_id, period, "其他承攬", region_name)

    # 讀範本資料
    ss_rec = open_spreadsheet(reconciliation_id)
    template = ss_rec.worksheet("範本")
    data = get_all_data(template, "A2", "BJ")
    if not data:
        raise Exception("範本無資料，請先執行搬運和加工")

    # 分類
    buckets = {k: [] for k in CLASSIFY_MAP}

    for row in data:
        e_text = str(row[4]) if len(row) > 4 else ""
        if "家電" in e_text:
            buckets["家電"].append(row)
        elif "水洗" in e_text:
            buckets["水洗"].append(row)
        elif "收納" in e_text:
            buckets["收納"].append(row)
        elif "座椅" in e_text:
            buckets["座椅"].append(row)
        elif "地毯" in e_text:
            buckets["地毯"].append(row)
        elif "清潔" in e_text or "1專業清潔" in e_text:
            buckets["清潔"].append(row)

    first_half = is_first_half(period)
    ss_clean = open_spreadsheet(cleaning_id)
    ss_other = open_spreadsheet(other_id)

    counts = {}

    for label, rows in buckets.items():
        if not rows:
            counts[label] = 0
            continue

        sheet_name = CLASSIFY_MAP[label]
        try:
            if label == "清潔":
                target = ss_clean.worksheet(sheet_name)
            else:
                target = ss_other.worksheet(sheet_name)

            start_row = get_paste_row(target, first_half)
            paste_data(target, start_row, rows)
            counts[label] = len(rows)
            st.write(f"✅ {label}：{len(rows)} 筆")

        except Exception as e:
            st.warning(f"⚠️ {sheet_name} 寫入失敗：{e}")
            counts[label] = 0

    return counts


# ═══════════════════════════════════════
# ⑥ 搬運退款＋預收
# ═══════════════════════════════════════

def move_refund_and_prepaid(
    root_folder_id: str,
    period: str,
    region_name: str,
) -> dict:
    """
    1. 先排序範本（E→H→M）
    2. 找已退款全部加收、已退款全部退款、預收的 Google Sheet
    3. 搬運到範本，依 A+B+Y 去重
    """
    reconciliation_id = _get_period_file_id(root_folder_id, period, "金流對帳", region_name)
    folder_id = _get_period_folder_id(root_folder_id, period)

    ss = open_spreadsheet(reconciliation_id)
    template = ss.worksheet("範本")

    # 先排序範本
    _sort_template(template)

    keywords = ["已退款全部加收", "已退款全部退款", "預收"]
    counts = {}
    total_refund_rows = 0
    refund_start_row = None

    for keyword in keywords:
        file_id = _find_sheet_by_keyword(folder_id, keyword)
        if not file_id:
            st.warning(f"⚠️ 找不到 {keyword}，略過")
            counts[keyword] = 0
            continue

        src_ss = open_spreadsheet(file_id)
        src_sheet = src_ss.worksheets()[0]
        rows = get_all_data(src_sheet, "A2", "BJ")
        rows = normalize_all_rows(rows)

        if not rows:
            counts[keyword] = 0
            continue

        start_row = find_last_non_empty_row(template, 2) + 1
        if keyword == "已退款全部加收":
            refund_start_row = start_row

        paste_data(template, start_row, rows)
        counts[keyword] = len(rows)

        if keyword != "預收":
            total_refund_rows += len(rows)

        st.write(f"✅ {keyword}：{len(rows)} 筆")

    # 退款資料去重（A+B+Y 欄組合）
    if total_refund_rows > 0 and refund_start_row:
        deduped = _deduplicate_by_aby(template, refund_start_row, total_refund_rows)
        counts["去重後"] = deduped

    return counts


def _sort_template(sheet) -> None:
    """排序範本工作表（E→H→M）"""
    data = get_all_data(sheet, "A2", "BJ")
    if not data:
        return
    max_cols = 62
    data = [row + [""] * (max_cols - len(row)) for row in data]
    data.sort(key=lambda r: (str(r[4]), str(r[7]), str(r[12])))
    sheet.batch_clear([f"A2:BJ{len(data) + 1}"])
    sheet.update("A2", data, value_input_option="USER_ENTERED")


def _deduplicate_by_aby(sheet, start_row: int, row_count: int) -> int:
    """
    依 A+B+Y 欄（index 0+1+24）去重
    回傳去重後筆數
    """
    all_data = sheet.get(f"A{start_row}:BJ{start_row + row_count - 1}")
    if not all_data:
        return 0

    seen = set()
    unique = []
    for row in all_data:
        a = str(row[0]) if len(row) > 0 else ""
        b = str(row[1]) if len(row) > 1 else ""
        y = str(row[24]) if len(row) > 24 else ""
        key = f"{a}|{b}|{y}"
        if key not in seen:
            seen.add(key)
            unique.append(row)

    if len(unique) < len(all_data):
        sheet.batch_clear([f"A{start_row}:BJ{start_row + row_count - 1}"])
        if unique:
            sheet.update(f"A{start_row}", unique, value_input_option="USER_ENTERED")

    return len(unique)


# ═══════════════════════════════════════
# ⑦ 搬運發票＋藍新
# ═══════════════════════════════════════

INVOICE_BLUENEW_MAP = [
    {"sheet_name": "00發票",    "keyword": "發票",    "range_end": "S"},
    {"sheet_name": "01藍新收款", "keyword": "藍新收款", "range_end": "U"},
    {"sheet_name": "02藍新退款", "keyword": "藍新退款", "range_end": "W"},
]


def move_invoice_and_bluenew(
    root_folder_id: str,
    period: str,
    region_name: str,
) -> dict:
    """
    找期別資料夾中的發票、藍新收款、藍新退款 Google Sheet
    搬運到金流對帳試算表對應工作表
    """
    reconciliation_id = _get_period_file_id(root_folder_id, period, "金流對帳", region_name)
    folder_id = _get_period_folder_id(root_folder_id, period)
    ss = open_spreadsheet(reconciliation_id)
    counts = {}

    for target in INVOICE_BLUENEW_MAP:
        sheet_name = target["sheet_name"]
        keyword = target["keyword"]
        range_end = target["range_end"]

        file_id = _find_sheet_by_keyword(folder_id, keyword)
        if not file_id:
            st.warning(f"⚠️ 找不到 {keyword}，略過")
            counts[keyword] = 0
            continue

        src_ss = open_spreadsheet(file_id)
        src_sheet = src_ss.worksheets()[0]
        rows = get_all_data(src_sheet, "A2", range_end)

        try:
            target_sheet = ss.worksheet(sheet_name)
            target_sheet.batch_clear([f"A2:{range_end}"])
            if rows:
                paste_data(target_sheet, 2, rows)
            counts[keyword] = len(rows)
            st.write(f"✅ {keyword}：{len(rows)} 筆")

        except Exception as e:
            st.warning(f"⚠️ {sheet_name} 寫入失敗：{e}")
            counts[keyword] = 0

    return counts
