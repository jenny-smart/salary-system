"""
金流對帳模組

流程：
上半月 / 下半月：
  ① 建立期別資料夾與檔案
  ② 期別訂單轉檔（xlsx → Google Sheet）
  ③ 訂單搬運到範本
  ④ 範本加工
  ⑤ 分類搬運

下半月額外：
  ② 金流對帳轉檔（已退款/預收/發票/藍新）
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
    find_file_by_keyword,
    create_period_folder_and_files,
    convert_period_order_file,
    convert_payment_files,
)
from modules.sheet_helper import (
    open_spreadsheet,
    get_all_data,
    get_paste_row,
    paste_data,
    find_last_non_empty_row,
)


# ═══════════════════════════════════════
# 共用：找期別資料夾和檔案
# ═══════════════════════════════════════

def _get_period_folder_id(root_folder_id: str, period: str) -> str:
    drive = get_drive_service()
    folder = get_folder_by_name(drive, root_folder_id, period)
    if not folder:
        raise Exception(f"找不到期別資料夾：{period}，請先執行「建立期別資料夾」")
    return folder["id"]


def _get_period_file_id(root_folder_id: str, period: str, label: str, region_name: str) -> str:
    drive = get_drive_service()
    folder_id = _get_period_folder_id(root_folder_id, period)
    file_name = get_file_name(period, label, region_name)
    file = find_file_in_folder(drive, folder_id, file_name)
    if not file:
        raise Exception(f"找不到檔案：{file_name}")
    return file["id"]


def _find_sheet_by_keyword(folder_id: str, keyword: str) -> str | None:
    drive = get_drive_service()
    file = find_file_by_keyword(
        drive, folder_id, keyword,
        mime_type="application/vnd.google-apps.spreadsheet"
    )
    return file["id"] if file else None


# ═══════════════════════════════════════
# ① 建立期別資料夾與檔案（透過 GAS Web App）
# ═══════════════════════════════════════

GAS_WEB_APP_URL = "https://script.google.com/macros/s/AKfycbxe5u28GjYc_MejSTLdiivIikuTwQkwlgGjPrWMzzhqqv3G5M58mXOK-B-AUpnDs3P0/exec"


def create_period(root_folder_id: str, period: str, region_name: str, log_fn=None) -> dict:
    """
    呼叫 GAS Web App 建立期別資料夾與檔案
    GAS 用 jenny@lemonclean.com.tw 執行，複製的檔案空間算在該帳號
    """
    import requests

    def log(msg):
        if log_fn:
            log_fn(msg)

    log(f"🔄 呼叫 GAS 建立期別：{period}")

    params = {
        "period": period,
        "region": region_name,
        "rootFolderId": root_folder_id,
    }

    try:
        response = requests.get(GAS_WEB_APP_URL, params=params, timeout=120)
        result = response.json()
    except Exception as e:
        raise Exception(f"呼叫 GAS 失敗：{e}")

    # 顯示 GAS 回傳的每一條 log
    for entry in result.get("logs", []):
        log(entry)

    if not result.get("success"):
        raise Exception(f"GAS 執行失敗：{result.get('message', '未知錯誤')}")

    return result


# ═══════════════════════════════════════
# ② 期別訂單轉檔
# ═══════════════════════════════════════

def convert_order_file(root_folder_id: str, period: str, region_name: str, log_fn=None) -> str:
    """
    轉換 {期別}訂單-{地區}.xlsx → Google Sheet
    存在同一資料夾，同名蓋舊檔
    """
    return convert_period_order_file(root_folder_id, period, region_name, log_fn)


# ═══════════════════════════════════════
# ② 金流對帳轉檔（下半月）
# ═══════════════════════════════════════

def convert_payment_file(root_folder_id: str, period: str, region_name: str, log_fn=None) -> dict:
    """
    轉換下半月金流檔案：
    已退款全部加收/退款.xlsx、預收.xlsx、發票.zip、藍新收款/退款.csv
    """
    return convert_payment_files(root_folder_id, period, region_name, log_fn)


# ═══════════════════════════════════════
# ③ 訂單搬運到範本
# ═══════════════════════════════════════

def copy_orders_to_template(
    root_folder_id: str, period: str, region_name: str, log_fn=None
) -> int:
    """
    來源：{期別}訂單-{地區}（Google Sheet 第一個工作表，A2:BJ）
    目標：{期別}金流對帳-{地區} 的「範本」工作表
    上半月：清空再貼
    下半月：接在最後一筆後面
    注意：C/D/H 欄為日期，Y/Z/AA/AB 為數值，不轉換格式
    """
    def log(msg):
        if log_fn:
            log_fn(msg)

    drive = get_drive_service()
    folder_id = _get_period_folder_id(root_folder_id, period)

    # 找訂單 Google Sheet
    order_name = f"{period}訂單-{region_name}"
    order_file = find_file_in_folder(drive, folder_id, order_name)
    if not order_file:
        raise Exception(f"找不到訂單 Google Sheet：{order_name}，請先執行「期別訂單轉檔」")

    log(f"📂 來源：{order_name}")

    # 找金流對帳試算表
    reconciliation_id = _get_period_file_id(root_folder_id, period, "金流對帳", region_name)

    ss_order = open_spreadsheet(order_file["id"])
    ss_rec = open_spreadsheet(reconciliation_id)

    source_sheet = ss_order.worksheets()[0]
    template_sheet = ss_rec.worksheet("範本")

    # 讀取資料（不轉換日期和數值）
    data = get_all_data(source_sheet, "A2", "BJ")
    if not data:
        raise Exception("訂單無資料")

    log(f"📋 讀取 {len(data)} 筆資料")

    first_half = is_first_half(period)
    start_row = get_paste_row(template_sheet, first_half)
    count = paste_data(template_sheet, start_row, data)

    log(f"✅ 搬運完成：{count} 筆（{'上半月清空後貼入' if first_half else '下半月接續貼入'}）")
    return count


# ═══════════════════════════════════════
# ④ 範本加工
# ═══════════════════════════════════════

ABNORMAL_KEYWORDS = ["異動", "請假", "補做", "加時", "減時", "遲到", "薪資", "未服務", "加洗"]
EXPANDABLE_TYPES = ["水洗", "家電", "座椅", "收納", "地毯", "其他"]


def process_template(
    root_folder_id: str, period: str, region_name: str, log_fn=None
) -> dict:
    """
    範本加工：
    1. 排序（E欄→H欄→M欄客戶姓名）
    2. 異常標記（AP/AY欄含關鍵字 → 寫入K欄）
    3. 水洗類別文字去重
    4. 儲值金標記（E欄含VIP券/儲值金 → A欄寫「儲值金」）
    5. F/G欄服務項目拆解（有多個項目時拆成多列）
    """
    def log(msg):
        if log_fn:
            log_fn(msg)

    reconciliation_id = _get_period_file_id(root_folder_id, period, "金流對帳", region_name)
    ss = open_spreadsheet(reconciliation_id)
    sheet = ss.worksheet("範本")

    data = get_all_data(sheet, "A2", "BJ")
    if not data:
        return {"sort_count": 0, "mark_count": 0, "expand_count": 0, "warnings": []}

    max_cols = 62
    data = [row + [""] * (max_cols - len(row)) for row in data]
    df = pd.DataFrame(data)

    # ── 1. 排序：E(4) → H(7) → M(12) ──
    df = df.sort_values(by=[4, 7, 12], ascending=True).reset_index(drop=True)
    sort_count = len(df)
    log(f"🔵 排序完成：{sort_count} 筆")

    # ── 2. 異常標記 ──
    mark_count = 0
    for idx, row in df.iterrows():
        ap = str(row[41]) if pd.notna(row[41]) else ""
        ay = str(row[50]) if pd.notna(row[50]) else ""
        combined = (ap + " " + ay).strip()
        if any(kw in combined for kw in ABNORMAL_KEYWORDS):
            df.at[idx, 10] = combined
            mark_count += 1
    log(f"🔵 異常標記：{mark_count} 筆")

    # ── 3. 水洗類別去重 ──
    for idx, row in df.iterrows():
        e_text = str(row[4])
        if "3水洗：" in e_text:
            df.at[idx, 4] = _dedupe_wash_text(e_text)

    # ── 4. 儲值金標記 ──
    for idx, row in df.iterrows():
        e_text = str(row[4])
        if "VIP券" in e_text or "儲值金" in e_text:
            df.at[idx, 0] = "儲值金"

    # ── 5. F/G 欄拆解 ──
    log("🔵 F/G 欄服務項目拆解中...")
    expanded_data, expand_count, warnings = _expand_fg_rows(df)

    if warnings:
        for w in warnings:
            log(f"⚠️ {w}")

    log(f"🔵 拆解完成：新增 {expand_count} 列")

    # ── 寫回 ──
    sheet.batch_clear([f"A2:BJ{len(data) + expand_count + 10}"])
    if expanded_data:
        sheet.update("A2", expanded_data, value_input_option="USER_ENTERED")

    log(f"✅ 範本加工完成：排序 {sort_count} 筆，異常 {mark_count} 筆，拆解新增 {expand_count} 列")

    return {
        "sort_count": sort_count,
        "mark_count": mark_count,
        "expand_count": expand_count,
        "warnings": warnings,
    }


def _dedupe_wash_text(text: str) -> str:
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
    解析 F 欄服務項目，回傳 [{name, qty, has_qty}]
    支援換行、頓號、逗號分隔
    X 後的數字為數量，沒有數字則 has_qty=False
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
            items.append({
                "name": match.group(1).strip(),
                "qty": match.group(2),
                "has_qty": True
            })
        else:
            items.append({"name": line, "qty": "", "has_qty": False})
    return items


def _expand_fg_rows(df: pd.DataFrame) -> tuple[list, int, list]:
    """
    F/G 欄拆解：
    - F欄有 N 個服務項目 → 共 N 列（主單 + 子單）
    - 原本有子單的，補足到 N 列
    - G欄 = X 後的數字，沒有數字記錄 warning
    回傳：(輸出資料, 新增列數, warnings)
    """
    output = []
    expand_count = 0
    warnings = []

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
            # 單一項目，直接放
            if items and not items[0]["has_qty"]:
                warnings.append(f"訂單 {order_id}：F欄無數量（X後無數字），請確認")
            output.append(row.tolist())
            continue

        # 多個服務項目，需要拆解
        target_count = len(items)

        # 找出已存在的子單（訂單編號含 -N 的）
        # 這裡簡化：主單固定是第一列，後續是新增的子單
        for i, item in enumerate(items):
            new_row = row.tolist().copy()
            new_row[5] = item["name"]  # F欄：服務項目名稱
            new_row[6] = item["qty"]   # G欄：數量

            if i == 0:
                # 主單：保留原訂單編號
                pass
            else:
                # 子單：訂單編號加 -i
                new_row[1] = f"{order_id}-{i}"
                expand_count += 1

            if not item["has_qty"]:
                warnings.append(f"訂單 {order_id} 項目「{item['name']}」：無數量（X後無數字），請確認")

            output.append(new_row)

    return output, expand_count, warnings


# ═══════════════════════════════════════
# ⑤ 分類搬運
# ═══════════════════════════════════════

# 其他承攬類別（先分，避免含「清潔」字的被誤分）
OTHER_CONTRACT_MAP = {
    "水洗": "水洗營收明細",
    "收納": "收納營收明細",
    "家電": "家電營收明細",
    "座椅": "座椅營收明細",
    "地毯": "地毯營收明細",
}

CLEANING_KEYWORDS = ["清潔", "1專業清潔"]


def copy_classified_data(
    root_folder_id: str, period: str, region_name: str, log_fn=None
) -> dict:
    """
    分類搬運：
    1. 先分其他承攬（水洗/收納/家電/座椅/地毯）
    2. 再分清潔承攬
    3. 無法分類的資料跳出警告視窗
    4. 記錄清潔搬運筆數（供後續薪資表使用）
    """
    def log(msg):
        if log_fn:
            log_fn(msg)

    reconciliation_id = _get_period_file_id(root_folder_id, period, "金流對帳", region_name)
    cleaning_id = _get_period_file_id(root_folder_id, period, "清潔承攬", region_name)
    other_id = _get_period_file_id(root_folder_id, period, "其他承攬", region_name)

    ss_rec = open_spreadsheet(reconciliation_id)
    template = ss_rec.worksheet("範本")
    data = get_all_data(template, "A2", "BJ")

    if not data:
        raise Exception("範本無資料，請先執行搬運和加工")

    log(f"📋 範本共 {len(data)} 筆，開始分類")

    # 分類
    other_buckets = {k: [] for k in OTHER_CONTRACT_MAP}
    cleaning_rows = []
    unclassified = []

    for row in data:
        e_text = str(row[4]) if len(row) > 4 else ""
        classified = False

        # 先判斷其他承攬
        for label in OTHER_CONTRACT_MAP:
            if label in e_text:
                other_buckets[label].append(row)
                classified = True
                break

        if not classified:
            # 再判斷清潔
            if any(kw in e_text for kw in CLEANING_KEYWORDS):
                cleaning_rows.append(row)
                classified = True

        if not classified:
            unclassified.append(e_text)

    # 無法分類的資料警告
    if unclassified:
        unique_unclassified = list(set(unclassified))
        warning_msg = f"以下 {len(unique_unclassified)} 種類別無法分類，請確認：\n" + "\n".join(unique_unclassified[:10])
        st.warning(warning_msg)
        log(f"⚠️ 無法分類：{len(unclassified)} 筆")

    first_half = is_first_half(period)
    ss_clean = open_spreadsheet(cleaning_id)
    ss_other = open_spreadsheet(other_id)
    counts = {}

    # 先搬其他承攬
    for label, sheet_name in OTHER_CONTRACT_MAP.items():
        rows = other_buckets[label]
        if not rows:
            counts[label] = 0
            continue
        try:
            target = ss_other.worksheet(sheet_name)
            start_row = get_paste_row(target, first_half)
            paste_data(target, start_row, rows)
            counts[label] = len(rows)
            log(f"✅ {label}：{len(rows)} 筆 → {sheet_name}")
        except Exception as e:
            st.warning(f"⚠️ {sheet_name} 寫入失敗：{e}")
            counts[label] = 0

    # 再搬清潔承攬
    if cleaning_rows:
        try:
            clean_sheet = ss_clean.worksheet("清潔營收明細")
            start_row = get_paste_row(clean_sheet, first_half)
            paste_data(clean_sheet, start_row, cleaning_rows)
            counts["清潔"] = len(cleaning_rows)
            log(f"✅ 清潔：{len(cleaning_rows)} 筆 → 清潔營收明細")

            # 記錄清潔搬運筆數到 session state（供薪資表使用）
            st.session_state[f"cleaning_count_{period}_{region_name}"] = len(cleaning_rows)

        except Exception as e:
            st.warning(f"⚠️ 清潔營收明細寫入失敗：{e}")
            counts["清潔"] = 0
    else:
        counts["清潔"] = 0

    counts["無法分類"] = len(unclassified)
    return counts


# ═══════════════════════════════════════
# ⑥ 搬運退款＋預收
# ═══════════════════════════════════════

def move_refund_and_prepaid(
    root_folder_id: str, period: str, region_name: str, log_fn=None
) -> dict:
    """
    1. 搬運已退款全部加收
    2. 搬運已退款全部退款
    3. 對兩者去重（KEY：A+B+Y欄）
    4. 搬運預收（不去重）
    """
    def log(msg):
        if log_fn:
            log_fn(msg)

    reconciliation_id = _get_period_file_id(root_folder_id, period, "金流對帳", region_name)
    folder_id = _get_period_folder_id(root_folder_id, period)

    ss = open_spreadsheet(reconciliation_id)
    template = ss.worksheet("範本")
    counts = {}

    # 搬運退款（加收 + 退款）
    refund_keywords = ["已退款全部加收", "已退款全部退款"]
    refund_start_row = None
    total_refund_rows = 0

    for keyword in refund_keywords:
        file_id = _find_sheet_by_keyword(folder_id, keyword)
        if not file_id:
            log(f"⚠️ 找不到 {keyword}，略過")
            counts[keyword] = 0
            continue

        src_ss = open_spreadsheet(file_id)
        src_sheet = src_ss.worksheets()[0]
        rows = get_all_data(src_sheet, "A2", "BJ")

        if not rows:
            counts[keyword] = 0
            log(f"⚠️ {keyword} 無資料")
            continue

        start_row = find_last_non_empty_row(template, 2) + 1
        if refund_start_row is None:
            refund_start_row = start_row

        paste_data(template, start_row, rows)
        counts[keyword] = len(rows)
        total_refund_rows += len(rows)
        log(f"✅ {keyword}：{len(rows)} 筆")

    # 去重（A+B+Y欄）
    if total_refund_rows > 0 and refund_start_row:
        log("🔵 退款資料去重中（KEY：A+B+Y欄）...")
        deduped = _deduplicate_by_aby(template, refund_start_row, total_refund_rows)
        removed = total_refund_rows - deduped
        counts["去重後"] = deduped
        log(f"✅ 去重完成：{deduped} 筆（移除 {removed} 筆重複）")

    # 搬運預收（不去重）
    prepaid_id = _find_sheet_by_keyword(folder_id, "預收")
    if not prepaid_id:
        log("⚠️ 找不到預收，略過")
        counts["預收"] = 0
    else:
        src_ss = open_spreadsheet(prepaid_id)
        src_sheet = src_ss.worksheets()[0]
        rows = get_all_data(src_sheet, "A2", "BJ")

        if rows:
            start_row = find_last_non_empty_row(template, 2) + 1
            paste_data(template, start_row, rows)
            counts["預收"] = len(rows)
            log(f"✅ 預收：{len(rows)} 筆")
        else:
            counts["預收"] = 0
            log("⚠️ 預收無資料")

    return counts


def _deduplicate_by_aby(sheet, start_row: int, row_count: int) -> int:
    """依 A+B+Y 欄去重，回傳去重後筆數"""
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
    {"sheet_name": "00發票",    "keyword": "發票",    "range_end": "R"},
    {"sheet_name": "01藍新收款", "keyword": "藍新收款", "range_end": "U"},
    {"sheet_name": "02藍新退款", "keyword": "藍新退款", "range_end": "W"},
]


def move_invoice_and_bluenew(
    root_folder_id: str, period: str, region_name: str, log_fn=None
) -> dict:
    """
    發票：A2:R，藍新收款：A2:U，藍新退款：A2:W
    每次清空再貼
    """
    def log(msg):
        if log_fn:
            log_fn(msg)

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
            log(f"⚠️ 找不到 {keyword}，略過")
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
            log(f"✅ {keyword}：{len(rows)} 筆 → {sheet_name}")
        except Exception as e:
            st.warning(f"⚠️ {sheet_name} 寫入失敗：{e}")
            counts[keyword] = 0

    return counts
