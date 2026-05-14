"""
modules/payment_reconciliation.py
金流對帳模組  v2026-05c

流程：
上半月 / 下半月：
  ① 建立期別資料夾與檔案（GAS）
  ② 期別訂單轉檔（GAS）
  ③ 訂單搬運到範本
  ④ 範本加工
  ⑤ 分類搬運（含底色 / 字型 / 列高 21px）

下半月額外：
  ⑥ 金流對帳轉檔（GAS）
  ⑦ 搬運退款＋預收
  ⑧ 搬運發票＋藍新
"""

from __future__ import annotations

import re

import pandas as pd
import streamlit as st

from modules.auth import get_drive_service, get_credentials
from modules.period_utils import get_file_name, is_first_half
from modules.drive_helper import (
    get_folder_by_name,
    find_file_in_folder,
    find_file_by_keyword,
)
from modules.sheet_helper import (
    open_spreadsheet,
    get_all_data,
    get_paste_row,
    paste_data,
    find_last_non_empty_row,
)


# ═══════════════════════════════════════════════════════════════
# 共用：找期別資料夾和檔案
# ═══════════════════════════════════════════════════════════════

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


# ═══════════════════════════════════════════════════════════════
# GAS 呼叫
# ═══════════════════════════════════════════════════════════════

GAS_WEB_APP_URL = (
    "https://script.google.com/macros/s/"
    "AKfycbxD1ek5H5uLT2HgGUZzgoPqy6xDrF03Dqc1LXMeUQpDfACdoLCn4WGhx3p_ufbkxIa4/exec"
)


def _call_gas(action: str, root_folder_id: str, period: str, region_name: str,
              log_fn=None) -> dict:
    import requests as _requests

    def log(msg):
        if log_fn:
            log_fn(msg)

    params = {
        "action": action,
        "period": period,
        "region": region_name,
        "rootFolderId": root_folder_id,
    }
    try:
        response = _requests.get(GAS_WEB_APP_URL, params=params, timeout=180)
        result = response.json()
    except Exception as e:
        raise Exception(f"呼叫 GAS 失敗：{e}")

    for entry in result.get("logs", []):
        log(entry)

    if not result.get("success"):
        raise Exception(f"GAS 執行失敗：{result.get('message', '未知錯誤')}")

    return result


# ═══════════════════════════════════════════════════════════════
# ① 建立期別資料夾與檔案（GAS）
# ═══════════════════════════════════════════════════════════════

def create_period(root_folder_id: str, period: str, region_name: str, log_fn=None) -> dict:
    if log_fn:
        log_fn(f"🔄 呼叫 GAS 建立期別：{period}")
    return _call_gas("createPeriod", root_folder_id, period, region_name, log_fn)


# ═══════════════════════════════════════════════════════════════
# ② 期別訂單轉檔（GAS）
# ═══════════════════════════════════════════════════════════════

def convert_order_file(root_folder_id: str, period: str, region_name: str, log_fn=None) -> dict:
    if log_fn:
        log_fn(f"🔄 呼叫 GAS 轉檔：{period}訂單-{region_name}")
    return _call_gas("convertOrder", root_folder_id, period, region_name, log_fn)


# ═══════════════════════════════════════════════════════════════
# ⑥ 金流對帳轉檔（GAS）
# ═══════════════════════════════════════════════════════════════

def convert_payment_file(root_folder_id: str, period: str, region_name: str, log_fn=None) -> dict:
    if log_fn:
        log_fn(f"🔄 呼叫 GAS 金流對帳轉檔：{period}")
    return _call_gas("convertPayment", root_folder_id, period, region_name, log_fn)


# ═══════════════════════════════════════════════════════════════
# ③ 訂單搬運到範本
# ═══════════════════════════════════════════════════════════════

def copy_orders_to_template(
    root_folder_id: str, period: str, region_name: str, log_fn=None
) -> dict:
    """
    來源：{期別}訂單-{地區}（Google Sheet 第一個工作表，A2:BJ）
    目標：{期別}金流對帳-{地區} 的「範本」工作表
    上半月：清空再貼；下半月：接在最後一筆後面
    回傳：{"count": 筆數, "start_row": 起始列號}
    """
    def log(msg):
        if log_fn:
            log_fn(msg)

    drive = get_drive_service()
    folder_id = _get_period_folder_id(root_folder_id, period)

    order_name = f"{period}訂單-{region_name}"
    order_file = find_file_in_folder(drive, folder_id, order_name)
    if not order_file:
        raise Exception(f"找不到訂單 Google Sheet：{order_name}，請先執行「期別訂單轉檔」")

    log(f"📂 來源：{order_name}")

    reconciliation_id = _get_period_file_id(root_folder_id, period, "金流對帳", region_name)
    ss_order = open_spreadsheet(order_file["id"])
    ss_rec   = open_spreadsheet(reconciliation_id)

    source_sheet   = ss_order.worksheets()[0]
    template_sheet = ss_rec.worksheet("範本")

    data = get_all_data(source_sheet, "A2", "BJ")
    if not data:
        raise Exception("訂單無資料")

    log(f"📋 讀取 {len(data)} 筆資料")

    first_half = is_first_half(period)
    start_row  = get_paste_row(template_sheet, first_half)
    count      = paste_data(template_sheet, start_row, data)

    log(f"✅ 搬運完成：{count} 筆（起始列：{start_row}，"
        f"{'上半月清空後貼入' if first_half else '下半月接續貼入'}）")
    return {"count": count, "start_row": start_row}


# ═══════════════════════════════════════════════════════════════
# ④ 範本加工
# ═══════════════════════════════════════════════════════════════

ABNORMAL_KEYWORDS = ["異動", "請假", "補做", "加時", "減時", "遲到", "薪資", "未服務", "加洗"]
EXPANDABLE_TYPES  = ["水洗", "家電", "座椅", "收納", "地毯", "其他"]

SERVICE_KEYWORDS = {
    "清潔": ["1專業清潔", "2居家清潔"],
    "水洗": ["3水洗"],
    "家電": ["4家電"],
    "收納": ["5收納"],
    "座椅": ["6座椅"],
    "地毯": ["7地毯"],
}


def process_template(
    root_folder_id: str, period: str, region_name: str,
    start_row: int = None, log_fn=None
) -> dict:
    """
    範本加工：只針對 start_row 起的資料列做加工。
    Double check：加工前主單數 = 加工後主單數（B欄不含-1/-2）。
    """
    def log(msg):
        if log_fn:
            log_fn(msg)

    reconciliation_id = _get_period_file_id(root_folder_id, period, "金流對帳", region_name)
    ss    = open_spreadsheet(reconciliation_id)
    sheet = ss.worksheet("範本")

    all_data = get_all_data(sheet, "A2", "BJ")
    if not all_data:
        return {"sort_count": 0, "mark_count": 0, "expand_count": 0, "warnings": []}

    max_cols = 62
    all_data = [row + [""] * (max_cols - len(row)) for row in all_data]

    if start_row is None or start_row <= 2:
        process_start_idx = 0
        log(f"🔵 上半月模式：加工全部 {len(all_data)} 筆")
    else:
        process_start_idx = start_row - 2
        if process_start_idx >= len(all_data):
            log("⚠️ 起始列超出資料範圍，無新資料需要加工")
            return {"sort_count": 0, "mark_count": 0, "expand_count": 0, "warnings": []}
        log(f"🔵 下半月模式：從第 {start_row} 列開始，"
            f"加工 {len(all_data) - process_start_idx} 筆新資料")

    old_rows = all_data[:process_start_idx]
    new_rows = all_data[process_start_idx:]

    # ── 加工前主單數 ──────────────────────────────────────────
    before_main      = _count_main_by_service(new_rows)
    main_count_before = sum(before_main.values())
    log(f"🔵 加工前主單數：{main_count_before} 筆 "
        f"（清潔:{before_main['清潔']} 水洗:{before_main['水洗']} "
        f"家電:{before_main['家電']} 收納:{before_main['收納']} "
        f"座椅:{before_main['座椅']} 地毯:{before_main['地毯']}）")

    df_new = pd.DataFrame(new_rows)

    # 1. 排序
    df_new     = df_new.sort_values(by=[4, 7, 12], ascending=True).reset_index(drop=True)
    sort_count = len(df_new)
    log(f"🔵 排序完成：{sort_count} 筆")

    # 2. 異常標記
    mark_count = 0
    for idx, row in df_new.iterrows():
        ap       = str(row[41]) if pd.notna(row[41]) else ""
        ay       = str(row[50]) if pd.notna(row[50]) else ""
        combined = (ap + " " + ay).strip()
        if any(kw in combined for kw in ABNORMAL_KEYWORDS):
            df_new.at[idx, 10] = combined
            mark_count += 1
    log(f"🔵 異常標記：{mark_count} 筆")

    # 3. 水洗類別去重
    for idx, row in df_new.iterrows():
        e_text = str(row[4])
        if "3水洗：" in e_text:
            df_new.at[idx, 4] = _dedupe_wash_text(e_text)

    # 4. 儲值金標記
    for idx, row in df_new.iterrows():
        e_text = str(row[4])
        if "VIP券" in e_text or "儲值金" in e_text:
            df_new.at[idx, 0] = "儲值金"

    # 5. F/G 欄拆解
    log("🔵 F/G 欄服務項目拆解中...")
    expanded_new, expand_count, warnings, category_counts, new_row_indices = _expand_fg_rows(df_new)
    for w in warnings:
        log(f"⚠️ {w}")
    log(f"🔵 拆解完成：新增 {expand_count} 列")

    # ── 加工後主單數 double check ─────────────────────────────
    after_main       = _count_main_by_service(expanded_new)
    after_rows_count = _count_rows_by_service(expanded_new)
    main_count_after = sum(after_main.values())

    if main_count_after != main_count_before:
        log(f"⚠️ Double check 警告：加工前主單 {main_count_before} 筆，"
            f"加工後主單 {main_count_after} 筆，數量不一致！")
    else:
        log(f"🔵 Double check 主單數：{main_count_after} 筆 ✅")

    for svc in ["清潔", "水洗", "家電", "收納", "座椅", "地毯"]:
        b = before_main.get(svc, 0)
        a = after_main.get(svc, 0)
        if b != a:
            log(f"⚠️ Double check [{svc}] 主單數不一致：加工前={b}，加工後={a}")
        else:
            log(f"🔵 Double check [{svc}]：主單 {a} ✅，"
                f"加工後總列數={after_rows_count.get(svc, 0)}")

    log(f"🔵 儲值金列數：{after_rows_count.get('儲值金', 0)}")

    # ── 寫回範本 ──────────────────────────────────────────────
    final_data = old_rows + expanded_new
    total_rows = len(final_data)

    sheet.batch_clear([f"A2:BJ{total_rows + expand_count + 10}"])
    if final_data:
        sheet.update("A2", final_data, value_input_option="USER_ENTERED")

    ss_rec          = sheet.spreadsheet
    format_requests = []

    # 橘色底（K欄有值）
    if mark_count > 0:
        try:
            orange_bg = {"red": 1.0, "green": 0.6, "blue": 0.2}
            all_k = sheet.get("K2:K")
            for i, row_val in enumerate(all_k):
                if row_val and row_val[0].strip():
                    row_num = i + 2
                    format_requests.append({
                        "repeatCell": {
                            "range": {
                                "sheetId": sheet.id,
                                "startRowIndex": row_num - 1, "endRowIndex": row_num,
                                "startColumnIndex": 0, "endColumnIndex": 62,
                            },
                            "cell": {"userEnteredFormat": {"backgroundColor": orange_bg}},
                            "fields": "userEnteredFormat.backgroundColor",
                        }
                    })
        except Exception as e:
            log(f"⚠️ 橘色標記失敗：{e}")

    # 淺綠色底（拆解新增列）
    if new_row_indices:
        try:
            green_bg = {"red": 0.85, "green": 0.96, "blue": 0.85}
            for new_idx in new_row_indices:
                final_idx = len(old_rows) + new_idx
                row_num   = 2 + final_idx
                format_requests.append({
                    "repeatCell": {
                        "range": {
                            "sheetId": sheet.id,
                            "startRowIndex": row_num - 1, "endRowIndex": row_num,
                            "startColumnIndex": 0, "endColumnIndex": 62,
                        },
                        "cell": {"userEnteredFormat": {"backgroundColor": green_bg}},
                        "fields": "userEnteredFormat.backgroundColor",
                    }
                })
        except Exception as e:
            log(f"⚠️ 淺綠色標記失敗：{e}")

    if format_requests:
        try:
            ss_rec.batch_update({"requests": format_requests})
            log(f"🔵 格式標記完成：橘色 {mark_count} 列，淺綠色 {len(new_row_indices)} 列")
        except Exception as e:
            log(f"⚠️ 格式標記失敗：{e}")

    log(f"✅ 範本加工完成：排序 {sort_count} 筆，異常 {mark_count} 筆，"
        f"拆解新增 {expand_count} 列")

    return {
        "sort_count":      sort_count,
        "mark_count":      mark_count,
        "expand_count":    expand_count,
        "warnings":        warnings,
        "category_counts": category_counts,
        "before_main":     before_main,
        "after_main":      after_main,
        "after_rows":      after_rows_count,
    }


def _count_main_by_service(rows) -> dict:
    """各服務主單數（B欄不含 -1/-2）"""
    counts = {k: 0 for k in SERVICE_KEYWORDS}
    counts["其他"] = 0
    for row in rows:
        b_val = str(row[1]) if len(row) > 1 else ""
        if re.search(r"-\d+$", b_val):
            continue
        e_val   = str(row[4]) if len(row) > 4 else ""
        matched = False
        for svc, keywords in SERVICE_KEYWORDS.items():
            if any(kw in e_val for kw in keywords):
                counts[svc] += 1
                matched = True
                break
        if not matched:
            counts["其他"] += 1
    return counts


def _count_rows_by_service(rows) -> dict:
    """各服務總列數（含子單）"""
    counts = {k: 0 for k in SERVICE_KEYWORDS}
    counts["儲值金"] = 0
    counts["其他"]   = 0
    for row in rows:
        a_val = str(row[0]) if len(row) > 0 else ""
        e_val = str(row[4]) if len(row) > 4 else ""
        if a_val == "儲值金":
            counts["儲值金"] += 1
            continue
        matched = False
        for svc, keywords in SERVICE_KEYWORDS.items():
            if any(kw in e_val for kw in keywords):
                counts[svc] += 1
                matched = True
                break
        if not matched:
            counts["其他"] += 1
    return counts


def _dedupe_wash_text(text: str) -> str:
    prefix = "3水洗："
    if prefix not in text:
        return text
    idx  = text.index(prefix)
    head = text[:idx + len(prefix)]
    tail = text[idx + len(prefix):].strip()
    half = len(tail) // 2
    if half > 0 and tail[:half] == tail[half:]:
        return head + tail[:half]
    return text.replace("噴抽水洗＋除蟎噴抽水洗＋除蟎", "噴抽水洗＋除蟎")


def _parse_service_items(text: str) -> list[dict]:
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
            items.append({"name": match.group(1).strip(), "qty": match.group(2), "has_qty": True})
        else:
            items.append({"name": line, "qty": "", "has_qty": False})
    return items


def _expand_fg_rows(df: pd.DataFrame) -> tuple[list, int, list, dict, list]:
    output          = []
    expand_count    = 0
    warnings        = []
    category_counts = {}
    new_row_indices = []

    for idx, row in df.iterrows():
        e_text   = str(row[4])
        f_text   = str(row[5])
        order_id = str(row[1])

        is_expandable = any(t in e_text for t in EXPANDABLE_TYPES)
        if not is_expandable or not f_text.strip():
            output.append(row.tolist())
            continue

        items = _parse_service_items(f_text)
        if not items:
            output.append(row.tolist())
            continue

        category = next((cat for cat in EXPANDABLE_TYPES if cat in e_text), None)

        if len(items) == 1:
            item    = items[0]
            new_row = row.tolist().copy()
            new_row[5] = item["name"]
            new_row[6] = item["qty"]
            if not item["has_qty"]:
                warnings.append(f"訂單 {order_id}：F欄無數量（X後無數字），請確認")
            output.append(new_row)
            if category:
                category_counts[category] = category_counts.get(category, 0) + 1
        else:
            for i, item in enumerate(items):
                new_row    = row.tolist().copy()
                new_row[5] = item["name"]
                new_row[6] = item["qty"]
                if i > 0:
                    new_row[1] = f"{order_id}-{i}"
                    expand_count += 1
                    new_row_indices.append(len(output))
                    for col_idx in range(24, 28):
                        if col_idx < len(new_row):
                            new_row[col_idx] = ""
                if not item["has_qty"]:
                    warnings.append(f"訂單 {order_id} 項目「{item['name']}」：無數量，請確認")
                output.append(new_row)
            if category:
                category_counts[category] = category_counts.get(category, 0) + len(items)

    return output, expand_count, warnings, category_counts, new_row_indices


# ═══════════════════════════════════════════════════════════════
# ⑤ 分類搬運
# ═══════════════════════════════════════════════════════════════

OTHER_CONTRACT_MAP = {
    "水洗": "水洗營收明細",
    "收納": "收納營收明細",
    "家電": "家電營收明細",
    "座椅": "座椅營收明細",
    "地毯": "地毯營收明細",
}
CLEANING_KEYWORDS = ["清潔", "1專業清潔"]

# 白色背景（不記錄）
_WHITE_BG = {"red": 1.0, "green": 1.0, "blue": 1.0}
# 目標列高（pixels）
_ROW_HEIGHT_PX = 21


def _build_sheets_service():
    """用 get_credentials() 建立 Google Sheets API v4 client。"""
    import googleapiclient.discovery
    import google.auth.transport.requests

    creds = get_credentials()
    if not getattr(creds, "token", None) or not creds.valid:
        try:
            creds.refresh(google.auth.transport.requests.Request())
        except Exception:
            pass
    return googleapiclient.discovery.build("sheets", "v4", credentials=creds,
                                           cache_discovery=False)


def _fetch_row_fmts(spreadsheet_id: str, sheet_title: str,
                    row_nums: list[int]) -> dict[int, dict]:
    """
    批次讀取多列的格式（背景色＋字型）。
    row_nums: 1-based 列號清單
    回傳: {row_num: {"bg": dict|None, "font": dict|None}}
    """
    if not row_nums:
        return {}

    svc    = _build_sheets_service()
    ranges = [f"'{sheet_title}'!A{r}:BJ{r}" for r in row_nums]

    try:
        result = svc.spreadsheets().get(
            spreadsheetId   = spreadsheet_id,
            ranges          = ranges,
            fields          = "sheets.data.rowData.values.effectiveFormat",
            includeGridData = True,
        ).execute()
    except Exception:
        return {r: {"bg": None, "font": None} for r in row_nums}

    def _color_or_none(c: dict | None) -> dict | None:
        if not c:
            return None
        r = c.get("red",   0.0)
        g = c.get("green", 0.0)
        b = c.get("blue",  0.0)
        # 白色（誤差容許 0.01）不記錄
        if abs(r - 1) < 0.01 and abs(g - 1) < 0.01 and abs(b - 1) < 0.01:
            return None
        return {"red": r, "green": g, "blue": b}

    fmt_map = {}
    sheets_data = result.get("sheets", [])

    for i, row_num in enumerate(row_nums):
        try:
            row_data = sheets_data[i]["data"][0]["rowData"][0]["values"]
            ef       = row_data[0].get("effectiveFormat", {})
            bg       = _color_or_none(ef.get("backgroundColor"))
            tf       = ef.get("textFormat", {})
            font     = {
                "fontFamily":      tf.get("fontFamily"),
                "fontSize":        tf.get("fontSize"),
                "bold":            tf.get("bold"),
                "italic":          tf.get("italic"),
                "foregroundColor": _color_or_none(tf.get("foregroundColor")),
            } if tf else None
            fmt_map[row_num] = {"bg": bg, "font": font}
        except (IndexError, KeyError, TypeError):
            fmt_map[row_num] = {"bg": None, "font": None}

    return fmt_map


def _apply_fmts(target_sheet, paste_start: int, fmts: list[dict | None]):
    """
    套用格式到目標工作表。
    每列：背景色 + 字型（各自只在有值時寫入）+ 列高 21px。
    """
    if not fmts:
        return

    requests = []
    for i, fmt in enumerate(fmts):
        row_num  = paste_start + i
        fmt      = fmt or {}
        cell_fmt = {}
        fields   = []

        bg = fmt.get("bg")
        if bg:
            cell_fmt["backgroundColor"] = bg
            fields.append("userEnteredFormat.backgroundColor")

        font = fmt.get("font") or {}
        tf   = {}
        if font.get("fontFamily"):
            tf["fontFamily"] = font["fontFamily"]
        if font.get("fontSize") is not None:
            tf["fontSize"] = font["fontSize"]
        if font.get("bold") is not None:
            tf["bold"] = font["bold"]
        if font.get("italic") is not None:
            tf["italic"] = font["italic"]
        fg = font.get("foregroundColor")
        if fg:
            tf["foregroundColor"] = fg
        if tf:
            cell_fmt["textFormat"] = tf
            fields.append("userEnteredFormat.textFormat")

        if cell_fmt and fields:
            requests.append({
                "repeatCell": {
                    "range": {
                        "sheetId":          target_sheet.id,
                        "startRowIndex":    row_num - 1,
                        "endRowIndex":      row_num,
                        "startColumnIndex": 0,
                        "endColumnIndex":   62,
                    },
                    "cell":   {"userEnteredFormat": cell_fmt},
                    "fields": ",".join(fields),
                }
            })

        # 列高固定 21px（不論有無格式都設定）
        requests.append({
            "updateDimensionProperties": {
                "range": {
                    "sheetId":    target_sheet.id,
                    "dimension":  "ROWS",
                    "startIndex": row_num - 1,
                    "endIndex":   row_num,
                },
                "properties": {"pixelSize": _ROW_HEIGHT_PX},
                "fields":     "pixelSize",
            }
        })

    if requests:
        target_sheet.spreadsheet.batch_update({"requests": requests})


def copy_classified_data(
    root_folder_id: str, period: str, region_name: str,
    template_start_row: int = None,
    category_counts: dict = None,
    log_fn=None
) -> dict:
    """
    分類搬運：只分類 template_start_row 起的新資料。
    搬運時同步搬移底色、字型，並設定目標列高 21px。
    1. 先分其他承攬（水洗/收納/家電/座椅/地毯）
    2. 再分清潔承攬
    3. 無法分類的資料跳出警告
    """
    def log(msg):
        if log_fn:
            log_fn(msg)

    reconciliation_id = _get_period_file_id(root_folder_id, period, "金流對帳", region_name)
    cleaning_id       = _get_period_file_id(root_folder_id, period, "清潔承攬", region_name)
    other_id          = _get_period_file_id(root_folder_id, period, "其他承攬", region_name)

    ss_rec   = open_spreadsheet(reconciliation_id)
    template = ss_rec.worksheet("範本")
    all_data = get_all_data(template, "A2", "BJ")

    if not all_data:
        raise Exception("範本無資料，請先執行搬運和加工")

    if template_start_row and template_start_row > 2:
        process_start_idx = template_start_row - 2
        data = all_data[process_start_idx:]
        log(f"📋 範本共 {len(all_data)} 筆，分類第 {template_start_row} 列起的 {len(data)} 筆")
    else:
        data = all_data
        log(f"📋 範本共 {len(data)} 筆，開始分類")

    # ── 分類 ──────────────────────────────────────────────────
    other_buckets      = {k: [] for k in OTHER_CONTRACT_MAP}
    other_row_indices  = {k: [] for k in OTHER_CONTRACT_MAP}
    cleaning_rows      = []
    cleaning_row_indices = []
    unclassified       = []

    for orig_idx, row in enumerate(data):
        e_text     = str(row[4]) if len(row) > 4 else ""
        classified = False

        for label in OTHER_CONTRACT_MAP:
            if label in e_text:
                other_buckets[label].append(row)
                other_row_indices[label].append(orig_idx)
                classified = True
                break

        if not classified:
            if any(kw in e_text for kw in CLEANING_KEYWORDS):
                cleaning_rows.append(row)
                cleaning_row_indices.append(orig_idx)
                classified = True

        if not classified:
            unclassified.append(e_text)

    if unclassified:
        unique_unc = list(set(unclassified))
        st.warning(f"以下 {len(unique_unc)} 種類別無法分類：\n" + "\n".join(unique_unc[:10]))
        log(f"⚠️ 無法分類：{len(unclassified)} 筆")

    if category_counts:
        for cat, expected in category_counts.items():
            actual = len(other_buckets.get(cat, []))
            if actual != expected:
                log(f"⚠️ Double check [{cat}]：④加工={expected} 列，⑤分類={actual} 列，請確認")
            else:
                log(f"🔵 Double check [{cat}]：{actual} 列 ✅")

    first_half     = is_first_half(period)
    template_sheet = ss_rec.worksheet("範本")
    ss_clean       = open_spreadsheet(cleaning_id)
    ss_other       = open_spreadsheet(other_id)
    counts         = {}

    # ── 共用：計算來源列號 ────────────────────────────────────
    def _sheet_row(orig_idx: int) -> int:
        """data 中的 0-based index → 範本工作表 1-based 列號"""
        if template_start_row and template_start_row > 2:
            return template_start_row + orig_idx
        return 2 + orig_idx

    # ── 先搬其他承攬 ──────────────────────────────────────────
    for label, sheet_name in OTHER_CONTRACT_MAP.items():
        rows        = other_buckets[label]
        row_indices = other_row_indices[label]

        if not rows:
            counts[label] = 0
            continue

        try:
            target      = ss_other.worksheet(sheet_name)
            paste_start = get_paste_row(target, first_half)
            paste_data(target, paste_start, rows)
            counts[label] = len(rows)
            log(f"✅ {label}：{len(rows)} 筆 → {sheet_name}")

            # 批次讀取格式
            src_rows = [_sheet_row(i) for i in row_indices]
            fmt_map  = _fetch_row_fmts(
                spreadsheet_id = reconciliation_id,
                sheet_title    = template_sheet.title,
                row_nums       = src_rows,
            )
            fmts = [fmt_map.get(r) for r in src_rows]
            _apply_fmts(target, paste_start, fmts)

        except Exception as e:
            st.warning(f"⚠️ {sheet_name} 寫入失敗：{e}")
            counts[label] = 0

    # ── 再搬清潔承攬 ──────────────────────────────────────────
    if cleaning_rows:
        try:
            clean_sheet = ss_clean.worksheet("清潔營收明細")
            paste_start = get_paste_row(clean_sheet, first_half)
            paste_data(clean_sheet, paste_start, cleaning_rows)
            counts["清潔"] = len(cleaning_rows)
            log(f"✅ 清潔：{len(cleaning_rows)} 筆 → 清潔營收明細")

            src_rows = [_sheet_row(i) for i in cleaning_row_indices]
            fmt_map  = _fetch_row_fmts(
                spreadsheet_id = reconciliation_id,
                sheet_title    = template_sheet.title,
                row_nums       = src_rows,
            )
            fmts = [fmt_map.get(r) for r in src_rows]
            _apply_fmts(clean_sheet, paste_start, fmts)

            st.session_state[f"cleaning_count_{period}_{region_name}"] = len(cleaning_rows)

        except Exception as e:
            st.warning(f"⚠️ 清潔營收明細寫入失敗：{e}")
            counts["清潔"] = 0
    else:
        counts["清潔"] = 0

    counts["無法分類"] = len(unclassified)
    return counts


# ═══════════════════════════════════════════════════════════════
# ⑦ 搬運退款＋預收
# ═══════════════════════════════════════════════════════════════

def move_refund_and_prepaid(
    root_folder_id: str, period: str, region_name: str, log_fn=None
) -> dict:
    """
    1. 搬運已退款全部加收
    2. 搬運已退款全部退款
    3. 去重（KEY：A+B+Y欄）
    4. 搬運預收（不去重）
    """
    def log(msg):
        if log_fn:
            log_fn(msg)

    reconciliation_id = _get_period_file_id(root_folder_id, period, "金流對帳", region_name)
    folder_id         = _get_period_folder_id(root_folder_id, period)

    ss       = open_spreadsheet(reconciliation_id)
    template = ss.worksheet("範本")
    counts   = {}

    refund_keywords   = ["已退款全部加收", "已退款全部退款"]
    refund_start_row  = None
    total_refund_rows = 0

    for keyword in refund_keywords:
        file_id = _find_sheet_by_keyword(folder_id, keyword)
        if not file_id:
            log(f"⚠️ 找不到 {keyword}，略過")
            counts[keyword] = 0
            continue

        src_ss    = open_spreadsheet(file_id)
        src_sheet = src_ss.worksheets()[0]
        rows      = get_all_data(src_sheet, "A2", "BJ")

        if not rows:
            counts[keyword] = 0
            log(f"⚠️ {keyword} 無資料")
            continue

        start_row = find_last_non_empty_row(template, 2) + 1
        if refund_start_row is None:
            refund_start_row = start_row

        paste_data(template, start_row, rows)
        counts[keyword]    = len(rows)
        total_refund_rows += len(rows)
        log(f"✅ {keyword}：{len(rows)} 筆")

    if total_refund_rows > 0 and refund_start_row:
        log("🔵 退款資料去重中（KEY：A+B+Y欄）...")
        deduped = _deduplicate_by_aby(template, refund_start_row, total_refund_rows)
        removed = total_refund_rows - deduped
        counts["去重後"] = deduped
        log(f"✅ 去重完成：{deduped} 筆（移除 {removed} 筆重複）")

    prepaid_id = _find_sheet_by_keyword(folder_id, "預收")
    if not prepaid_id:
        log("⚠️ 找不到預收，略過")
        counts["預收"] = 0
    else:
        src_ss    = open_spreadsheet(prepaid_id)
        src_sheet = src_ss.worksheets()[0]
        rows      = get_all_data(src_sheet, "A2", "BJ")
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
    all_data = sheet.get(f"A{start_row}:BJ{start_row + row_count - 1}")
    if not all_data:
        return 0

    seen   = set()
    unique = []
    for row in all_data:
        a   = str(row[0])  if len(row) > 0  else ""
        b   = str(row[1])  if len(row) > 1  else ""
        y   = str(row[24]) if len(row) > 24 else ""
        key = f"{a}|{b}|{y}"
        if key not in seen:
            seen.add(key)
            unique.append(row)

    if len(unique) < len(all_data):
        sheet.batch_clear([f"A{start_row}:BJ{start_row + row_count - 1}"])
        if unique:
            sheet.update(f"A{start_row}", unique, value_input_option="USER_ENTERED")

    return len(unique)


# ═══════════════════════════════════════════════════════════════
# ⑧ 搬運發票＋藍新
# ═══════════════════════════════════════════════════════════════

INVOICE_BLUENEW_MAP = [
    {"sheet_name": "00發票",     "keyword": "發票",     "range_end": "R"},
    {"sheet_name": "01藍新收款", "keyword": "藍新收款", "range_end": "U"},
    {"sheet_name": "02藍新退款", "keyword": "藍新退款", "range_end": "W"},
]


def move_invoice_and_bluenew(
    root_folder_id: str, period: str, region_name: str, log_fn=None
) -> dict:
    """
    發票 A2:R、藍新收款 A2:U、藍新退款 A2:W
    每次清空再貼
    """
    def log(msg):
        if log_fn:
            log_fn(msg)

    reconciliation_id = _get_period_file_id(root_folder_id, period, "金流對帳", region_name)
    folder_id         = _get_period_folder_id(root_folder_id, period)
    ss                = open_spreadsheet(reconciliation_id)
    counts            = {}

    for target in INVOICE_BLUENEW_MAP:
        sheet_name = target["sheet_name"]
        keyword    = target["keyword"]
        range_end  = target["range_end"]

        file_id = _find_sheet_by_keyword(folder_id, keyword)
        if not file_id:
            log(f"⚠️ 找不到 {keyword}，略過")
            counts[keyword] = 0
            continue

        src_ss    = open_spreadsheet(file_id)
        src_sheet = src_ss.worksheets()[0]
        rows      = get_all_data(src_sheet, "A2", range_end)

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
