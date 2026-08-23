"""
modules/payment_reconciliation.py
金流對帳模組  v2026-08c-oauth

流程：
上半月 / 下半月：
  ① 建立期別資料夾與檔案（Python + OAuth）
  ② 期別訂單轉檔（Python + OAuth）
  ③ 訂單搬運到範本
  ④ 範本加工
  ⑤ 分類搬運（含底色 / 字型 / 列高 21px）

下半月額外：
  ⑥ 金流對帳轉檔（Python + OAuth）
  ⑦ 搬運退款＋預收
  ⑧ 搬運發票＋藍新
"""

from __future__ import annotations

import re
import copy
import unicodedata
from datetime import datetime

import pandas as pd
import streamlit as st

from modules.auth import get_drive_service, get_credentials
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
# Python + OAuth：建立與轉檔
# ═══════════════════════════════════════════════════════════════

def create_period(root_folder_id: str, period: str, region_name: str, log_fn=None) -> dict:
    """使用 auth.py 提供的使用者 OAuth 建立期別資料夾與檔案。"""
    if log_fn:
        log_fn(f"🔄 Python + OAuth 建立期別：{period}")

    return create_period_folder_and_files(
        root_folder_id,
        period,
        region_name,
        log_fn=log_fn,
    )


# ═══════════════════════════════════════════════════════════════
# ② 期別訂單轉檔（Python + OAuth）
# ═══════════════════════════════════════════════════════════════

def convert_order_file(root_folder_id: str, period: str, region_name: str, log_fn=None) -> dict:
    """使用 auth.py 提供的使用者 OAuth 將訂單檔轉成 Google 試算表。"""
    if log_fn:
        log_fn(f"🔄 Python + OAuth 轉檔：{period}訂單-{region_name}")

    return convert_period_order_file(
        root_folder_id,
        period,
        region_name,
        log_fn=log_fn,
    )


# ═══════════════════════════════════════════════════════════════
# ⑥ 金流對帳轉檔（Python + OAuth）
# ═══════════════════════════════════════════════════════════════

def convert_payment_file(root_folder_id: str, period: str, region_name: str, log_fn=None) -> dict:
    """使用 auth.py 提供的使用者 OAuth 轉換金流相關檔案。"""
    if log_fn:
        log_fn(f"🔄 Python + OAuth 金流對帳轉檔：{period}")

    return convert_payment_files(
        root_folder_id,
        period,
        region_name,
        log_fn=log_fn,
    )


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
    start_row  = _get_period_paste_row(template_sheet, first_half, log_fn=log)
    count = len(data)

    # 下半月重跑時，若目標尾端已是同一批資料，只補格式，不再重複追加。
    retry_start = start_row - count
    is_retry = False
    if not first_half and retry_start >= 2:
        existing = template_sheet.get(f"A{retry_start}:BJ{start_row - 1}")

        def normalized(rows):
            return [[str(cell) for cell in row] for row in rows]

        is_retry = normalized(existing) == normalized(data)

    if is_retry:
        start_row = retry_start
        log(f"✅ 資料先前已搬運：{count} 筆（起始列：{start_row}），本次只補格式")
    else:
        count = paste_data(template_sheet, start_row, data)
        log(f"✅ 搬運完成：{count} 筆（起始列：{start_row}，"
            f"{'上半月清空後貼入' if first_half else '下半月接續貼入'}）")

    # 搬移格式（底色 + 字型 + 列高 21px）
    # 來源：訂單工作表第 2 列起（共 count 列）
    # 目標：範本工作表 start_row 起
    try:
        import traceback
        src_row_nums = list(range(2, 2 + count))
        used_cols = max((len(row) for row in data), default=1)
        log(f"🔵 讀取格式中（訂單工作表第 2–{1 + count} 列）...")
        fmt_map = _fetch_row_fmts(
            spreadsheet_id = order_file["id"],
            sheet_title    = source_sheet.title,
            row_nums       = src_row_nums,
            max_cols       = used_cols,
        )
        log(f"🔵 格式讀取完成，套用中...")
        fmts = [fmt_map.get(r) for r in src_row_nums]
        _apply_fmts(template_sheet, start_row, fmts)
        log(f"🔵 格式搬移完成（{count} 列，列高 21px）")
    except Exception as e:
        log(f"⚠️ 資料已完成搬運；僅格式搬移失敗：{e}")
        log(f"⚠️ 詳細：{traceback.format_exc()[:300]}")

    return {"count": count, "start_row": start_row}


# ═══════════════════════════════════════════════════════════════
# ④ 範本加工
# ═══════════════════════════════════════════════════════════════

def _text_sort_key(value):
    """
    文字排序 key：
    - 全半形正規化 NFKC
    - 全形空白轉半形空白
    - 去頭尾空白
    - 英文大小寫不敏感
    """
    if value is None or pd.isna(value):
        return ""
    text = unicodedata.normalize("NFKC", str(value))
    text = text.replace("\u3000", " ").strip()
    return text.casefold()




# 常用中文姓名筆劃表：用於 M 欄客戶姓名排序。
# 說明：Python 內建排序不會依繁中筆劃排序，因此這裡用常見姓氏/姓名用字做筆劃 key。
# 若遇到未收錄中文字，會排在已知筆劃字後方，再以 Unicode 保持穩定排序。
CJK_STROKE_OVERRIDES = {
    # 常見姓氏 / 截圖中出現姓氏
    "丁": 2, "七": 2, "卜": 2, "刁": 2,
    "于": 3, "士": 3, "土": 3, "大": 3, "小": 3, "尤": 4, "尹": 4, "孔": 4, "王": 4, "方": 4, "毛": 4, "文": 4, "牛": 4,
    "司": 5, "白": 5, "古": 5, "石": 5, "田": 5, "甘": 5, "申": 5, "史": 5, "左": 5, "丘": 5, "平": 5, "包": 5,
    "任": 6, "伍": 6, "朱": 6, "江": 6, "池": 6, "何": 7, "吳": 7, "呂": 7, "宋": 7, "李": 7, "杜": 7, "沈": 7, "阮": 7, "辛": 7,
    "周": 8, "林": 8, "邱": 8, "金": 8, "姚": 9, "施": 9, "洪": 9, "胡": 9, "柯": 9, "范": 9, "侯": 9,
    "高": 10, "夏": 10, "孫": 10, "徐": 10, "唐": 10, "翁": 10, "馬": 10, "袁": 10,
    "郭": 11, "陳": 11, "曹": 11, "許": 11, "張": 11, "梁": 11, "莊": 11, "連": 11,
    "黃": 12, "曾": 12, "彭": 12, "傅": 12, "馮": 12, "游": 12, "程": 12,
    "楊": 13, "葉": 13, "董": 13, "詹": 13, "賈": 13, "葛": 13, "溫": 13, "廖": 14, "劉": 15, "蔡": 17, "謝": 17, "簡": 18, "羅": 19, "蕭": 16,
    # 常見名字用字 / 截圖中出現用字
    "一": 1, "乙": 1, "二": 2, "人": 2, "力": 2, "又": 2, "三": 3, "子": 3, "女": 3, "凡": 3, "千": 3,
    "仁": 4, "允": 4, "元": 4, "天": 4, "心": 4, "月": 4, "中": 4, "予": 4,
    "可": 5, "平": 5, "弘": 5, "正": 5, "民": 5, "玉": 5, "生": 5,
    "任": 6, "宇": 6, "安": 6, "妤": 7, "伶": 7, "妍": 7, "希": 7, "廷": 7, "志": 7, "良": 7, "均": 7,
    "佳": 8, "依": 8, "佩": 8, "欣": 8, "承": 8, "明": 8, "宜": 8, "奇": 8, "玫": 8,
    "品": 9, "思": 9, "怡": 9, "柔": 9, "珊": 9, "玲": 9, "盈": 9, "科": 9, "美": 9, "芬": 10, "芳": 10, "芸": 8,
    "倫": 10, "偉": 11, "敏": 11, "淑": 11, "涵": 11, "琪": 12, "瑀": 13, "鈺": 13,
    "婷": 12, "雅": 12, "嘉": 14, "榮": 14, "慧": 15, "穎": 16, "潔": 15, "儀": 15,
    "麗": 19, "寶": 20, "蓉": 16, "霖": 16, "忠": 8, "德": 15, "超": 12, "省": 9,
}


def _is_cjk_char(ch: str) -> bool:
    return "\u3400" <= ch <= "\u9fff"


def _stroke_sort_key(value):
    """
    中文姓名筆劃排序 key。
    - 英文 / 數字仍用文字排序。
    - 中文依每個字的筆劃數排序，再用字本身做穩定 tie-breaker。
    - 未收錄中文字給 99 劃，避免亂插在已知筆劃字中。
    """
    text = _text_sort_key(value)
    if not text:
        return ((999, ""),)

    key = []
    for ch in text:
        if _is_cjk_char(ch):
            key.append((CJK_STROKE_OVERRIDES.get(ch, 99), ch))
        else:
            # 非中文放在中文字前，維持英文/數字排序。
            key.append((0, ch))
    return tuple(key)


def _date_sort_key(value):
    """
    H 欄日期排序 key：可接受 2026/5/1、2026/05/01、2026-5-1。
    無法轉日期者排最後。
    """
    if value is None or pd.isna(value):
        return pd.Timestamp.max
    text = unicodedata.normalize("NFKC", str(value)).replace("\u3000", " ").strip()
    if not text:
        return pd.Timestamp.max
    dt = pd.to_datetime(text, errors="coerce")
    if pd.isna(dt):
        return pd.Timestamp.max
    return dt


ABNORMAL_KEYWORDS = ["異動", "加時", "減時", "請假", "補做", "遲到", "薪資", "未服務", "加洗", "未洗", "加收", "退款", "颱風", "停班", "停課"]
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

    修正版重點：
    1. 排序為 E → H日期 → M文字。
    2. 排序時「資料列 + A:BJ 逐格格式」綁在一起排序。
    3. F/G 拆解新增列會繼承母列格式。
    4. 寫回時先清空 A2:BJ 內容與格式，再寫回資料與原格式。
    5. 最後再套加工產生的底色：橘色異常列、淺綠色拆解新增列。
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

    # 加工前先抓取目前範本 A:BJ 的逐格格式，之後會跟著資料列一起排序。
    all_row_nums = list(range(2, 2 + len(all_data)))
    try:
        fmt_map = _fetch_row_fmts(
            spreadsheet_id = reconciliation_id,
            sheet_title    = sheet.title,
            row_nums       = all_row_nums,
        )
        all_fmts = [fmt_map.get(r, {"cells": [{} for _ in range(max_cols)]}) for r in all_row_nums]
        log(f"🔵 已讀取範本格式：{len(all_fmts)} 列（A:BJ 逐格格式）")
    except Exception as e:
        log(f"⚠️ 範本格式讀取失敗，將只加工資料：{e}")
        all_fmts = [{"cells": [{} for _ in range(max_cols)]} for _ in all_data]

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
    old_fmts = all_fmts[:process_start_idx]
    new_rows = all_data[process_start_idx:]
    new_fmts = all_fmts[process_start_idx:]

    # ── 加工前主單數 ──────────────────────────────────────────
    before_main       = _count_main_by_service(new_rows)
    main_count_before = sum(before_main.values())
    log(f"🔵 加工前主單數：{main_count_before} 筆 "
        f"（清潔:{before_main['清潔']} 水洗:{before_main['水洗']} "
        f"家電:{before_main['家電']} 收納:{before_main['收納']} "
        f"座椅:{before_main['座椅']} 地毯:{before_main['地毯']}）")

    # 1. 排序：資料列 + 格式列一起排序，避免底色 / 字型錯位。
    rows_with_fmt = [
        {"row": row, "fmt": fmt, "orig_index": idx}
        for idx, (row, fmt) in enumerate(zip(new_rows, new_fmts))
    ]
    rows_with_fmt.sort(
        key=lambda x: (
            _text_sort_key(x["row"][4] if len(x["row"]) > 4 else ""),   # E 購買項目
            _date_sort_key(x["row"][7] if len(x["row"]) > 7 else ""),   # H 服務日期
            _text_sort_key(x["row"][12] if len(x["row"]) > 12 else ""),   # M 客戶姓名：文字排序（對齊 GAS localeCompare）
            x["orig_index"],                                             # 穩定排序保底
        )
    )
    sorted_rows = [x["row"] for x in rows_with_fmt]
    sorted_fmts = [x["fmt"] for x in rows_with_fmt]
    sort_count = len(sorted_rows)
    log(f"🔵 排序完成：{sort_count} 筆（E → H日期 → M客戶姓名文字排序；格式跟著原列排序）")

    # 2. 異常標記：只改資料，原格式仍保留；最後再套橘色底。
    mark_count = 0
    for idx, row in enumerate(sorted_rows):
        ap       = str(row[41]) if len(row) > 41 and row[41] is not None else ""
        ay       = str(row[50]) if len(row) > 50 and row[50] is not None else ""
        combined = (ap + " " + ay).strip()
        if any(kw in combined for kw in ABNORMAL_KEYWORDS):
            row[10] = combined
            mark_count += 1
    log(f"🔵 異常標記：{mark_count} 筆")

    # 3. 水洗類別去重
    for row in sorted_rows:
        e_text = str(row[4]) if len(row) > 4 else ""
        if "3水洗：" in e_text:
            row[4] = _dedupe_wash_text(e_text)

    # 4. 儲值金標記
    for row in sorted_rows:
        e_text = str(row[4]) if len(row) > 4 else ""
        if "VIP券" in e_text or "儲值金" in e_text:
            row[0] = "儲值金"

    # 5. F/G 欄拆解：新增列繼承母列格式。
    log("🔵 F/G 欄服務項目拆解中...")
    expanded_new, expanded_fmts, expand_count, warnings, category_counts, new_row_indices = (
        _expand_fg_rows_with_fmts(sorted_rows, sorted_fmts)
    )
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
    final_fmts = old_fmts + expanded_fmts
    total_rows = len(final_data)

    # 先清空 A2:BJ 的內容與格式，避免排序 / 拆解後殘留舊格式。
    _clear_a2_bj_contents_and_formats(sheet, log_fn=log)

    if final_data:
        sheet.update("A2", final_data, value_input_option="USER_ENTERED")
        _apply_fmts(sheet, 2, final_fmts)
        log(f"🔵 已寫回資料與原列格式：{len(final_data)} 列")

    ss_rec          = sheet.spreadsheet
    format_requests = []

    # 橘色底（K欄有值）：在原格式寫回後才套用，讓加工底色覆蓋原底色。
    if mark_count > 0:
        try:
            orange_bg = {"red": 1.0, "green": 0.6, "blue": 0.2}
            all_k = sheet.get(f"K2:K{total_rows + 1}")
            for i, row_val in enumerate(all_k):
                if row_val and str(row_val[0]).strip():
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

    # 淺綠色底（拆解新增列）：在原格式寫回後才套用，讓加工底色覆蓋原底色。
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
        # ⑤分類搬運需要的是加工後「所有服務的實際列數」；
        # category_counts 只包含需拆解的類別，會讓讀取範圍嚴重不足。
        "category_counts": after_rows_count,
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
                    # 新切出的子單不繼承 V:AD（發票及收款金額計算欄）。
                    # Python 索引 21:30 對應試算表 V:AD。
                    for col_idx in range(21, 30):
                        if col_idx < len(new_row):
                            new_row[col_idx] = ""
                if not item["has_qty"]:
                    warnings.append(f"訂單 {order_id} 項目「{item['name']}」：無數量，請確認")
                output.append(new_row)
            if category:
                category_counts[category] = category_counts.get(category, 0) + len(items)

    return output, expand_count, warnings, category_counts, new_row_indices


def _expand_fg_rows_with_fmts(rows, fmts):
    existing_ids = {str(row[1]) for row in rows if len(row) > 1 and str(row[1]).strip()}
    
    # 先掃描所有主列，建立子單應填入的內容
    child_item_map = {}  # {child_id: {"name": ..., "qty": ...}}
    for row in rows:
        order_id = str(row[1]) if len(row) > 1 else ""
        if re.search(r"-\d+$", order_id):
            continue
        e_text = str(row[4]) if len(row) > 4 else ""
        f_text = str(row[5]) if len(row) > 5 else ""
        if not any(t in e_text for t in EXPANDABLE_TYPES) or not f_text.strip():
            continue
        items = _parse_service_items(f_text)
        for i, item in enumerate(items):
            if i > 0:
                child_item_map[f"{order_id}-{i}"] = item

    output_rows = []
    output_fmts = []
    expand_count = 0
    warnings = []
    category_counts = {}
    new_row_indices = []

    for row, fmt in zip(rows, fmts):
        row = list(row)
        parent_fmt = copy.deepcopy(fmt or {"cells": [{} for _ in range(62)]})
        order_id = str(row[1]) if len(row) > 1 else ""

        # 子列：用 child_item_map 更新 F/G，否則原樣保留
        if re.search(r"-\d+$", order_id):
            if order_id in child_item_map:
                item = child_item_map[order_id]
                row[5] = item["name"]
                row[6] = item["qty"]
            output_rows.append(row)
            output_fmts.append(parent_fmt)
            continue

        e_text = str(row[4]) if len(row) > 4 else ""
        f_text = str(row[5]) if len(row) > 5 else ""

        is_expandable = any(t in e_text for t in EXPANDABLE_TYPES)
        if not is_expandable or not f_text.strip():
            output_rows.append(row)
            output_fmts.append(parent_fmt)
            continue

        items = _parse_service_items(f_text)
        if not items:
            output_rows.append(row)
            output_fmts.append(parent_fmt)
            continue

        category = next((cat for cat in EXPANDABLE_TYPES if cat in e_text), None)

        for i, item in enumerate(items):
            new_row = row.copy()
            new_row[5] = item["name"]
            new_row[6] = item["qty"]
            if i > 0:
                child_id = f"{order_id}-{i}"
                if child_id in existing_ids:
                    continue  # 已存在，子列自己跑到時會處理
                new_row[1] = child_id
                expand_count += 1
                new_row_indices.append(len(output_rows))
                # 只清空本次新建立的子單；原本已存在的子單會在上方
                # 「子列」分支原樣保留，因此不受此規則影響。
                # Python 索引 21:30 對應試算表 V:AD。
                for col_idx in range(21, 30):
                    if col_idx < len(new_row):
                        new_row[col_idx] = ""
            if not item["has_qty"]:
                warnings.append(f"訂單 {order_id} 項目「{item['name']}」：無數量，請確認")
            output_rows.append(new_row)
            output_fmts.append(copy.deepcopy(parent_fmt))

        if category:
            category_counts[category] = category_counts.get(category, 0) + len(items)

    return output_rows, output_fmts, expand_count, warnings, category_counts, new_row_indices


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


def _clear_a2_bj_contents_and_formats(sheet, log_fn=None) -> None:
    """
    清空 A2:BJ 的內容與格式。
    注意：Google Sheets API 的 repeatCell 清格式需用 batch_update；
    gspread 的 batch_clear 只清內容，不會清格式。
    """
    def log(msg):
        if log_fn:
            log_fn(msg)

    max_rows = max(sheet.row_count, 2)

    # 1) 清內容
    sheet.batch_clear([f"A2:BJ{max_rows}"])

    # 2) 清格式：A2:BJ 最後列
    requests = [{
        "repeatCell": {
            "range": {
                "sheetId": sheet.id,
                "startRowIndex": 1,       # 第 2 列，0-based
                "endRowIndex": max_rows,
                "startColumnIndex": 0,    # A
                "endColumnIndex": 62,     # BJ
            },
            "cell": {"userEnteredFormat": {}},
            "fields": "userEnteredFormat",
        }
    }]
    sheet.spreadsheet.batch_update({"requests": requests})
    log("🧹 已清空 A2:BJ 的內容與格式")


def _get_append_row_by_col_b(sheet) -> int:
    """
    依 B 欄最後一個非空白列，回傳下一列作為貼入起始列。
    若 B 欄沒有資料，回傳 2。
    """
    vals = sheet.get("B2:B") or []
    last_offset = -1
    for i, row in enumerate(vals):
        if row and str(row[0]).strip():
            last_offset = i
    return 2 if last_offset < 0 else 2 + last_offset + 1


def _get_period_paste_row(sheet, first_half: bool, log_fn=None) -> int:
    """
    上半月：清空 A2:BJ 內容與格式後，從第 2 列貼入。
    下半月：依 B 欄最後非空白列的下一列貼入。
    """
    if first_half:
        _clear_a2_bj_contents_and_formats(sheet, log_fn=log_fn)
        return 2
    return _get_append_row_by_col_b(sheet)


def _color_or_none(c: dict | None) -> dict | None:
    """白色或空值回傳 None，其他回傳 RGB dict。"""
    if not c:
        return None
    r = c.get("red",   0.0)
    g = c.get("green", 0.0)
    b = c.get("blue",  0.0)
    if abs(r - 1) < 0.01 and abs(g - 1) < 0.01 and abs(b - 1) < 0.01:
        return None
    return {"red": r, "green": g, "blue": b}


def _cell_format_from_effective(ef: dict | None) -> dict:
    """
    將 effectiveFormat 轉成可寫入 userEnteredFormat 的格式。
    目前搬運 A:BJ 逐格常用格式：
    - 背景色
    - 字型 / 字級 / 粗體 / 斜體 / 字色
    - 水平 / 垂直對齊
    - 換行
    - 數字格式
    """
    ef = ef or {}
    out = {}

    bg = _color_or_none(ef.get("backgroundColor"))
    if bg:
        out["backgroundColor"] = bg

    tf = ef.get("textFormat") or {}
    text_format = {}
    for key in ["fontFamily", "fontSize", "bold", "italic", "strikethrough", "underline"]:
        if tf.get(key) is not None:
            text_format[key] = tf.get(key)

    fg = _color_or_none(tf.get("foregroundColor"))
    if fg:
        text_format["foregroundColor"] = fg

    if text_format:
        out["textFormat"] = text_format

    for key in ["horizontalAlignment", "verticalAlignment", "wrapStrategy"]:
        if ef.get(key) is not None:
            out[key] = ef.get(key)

    if ef.get("numberFormat"):
        out["numberFormat"] = ef.get("numberFormat")

    if ef.get("textRotation"):
        out["textRotation"] = ef.get("textRotation")

    return out


def _column_letter(number: int) -> str:
    letters = ""
    while number:
        number, remainder = divmod(number - 1, 26)
        letters = chr(65 + remainder) + letters
    return letters


def _fetch_row_fmts(spreadsheet_id: str, sheet_title: str,
                    row_nums: list[int], max_cols: int = 62) -> dict[int, dict]:
    """
    批次讀取多列 A:BJ 逐格格式。
    回傳：
      {
        row_num: {
          "cells": [A欄格式, B欄格式, ..., BJ欄格式]
        }
      }
    """
    if not row_nums:
        return {}

    fmt_map = {}
    svc = _build_sheets_service()
    max_cols = max(1, min(int(max_cols), 62))
    end_col = _column_letter(max_cols)

    # Highly repetitive Sheets formatting compresses beyond httplib2's safety
    # ratio (100x). Request identity encoding and keep each response bounded.
    sorted_rows = sorted(set(row_nums))
    for offset in range(0, len(sorted_rows), 100):
        chunk = sorted_rows[offset:offset + 100]
        min_row, max_row = min(chunk), max(chunk)
        request = svc.spreadsheets().get(
            spreadsheetId=spreadsheet_id,
            ranges=[f"'{sheet_title}'!A{min_row}:{end_col}{max_row}"],
            fields="sheets.data.rowData.values.effectiveFormat",
            includeGridData=True,
        )
        request.headers["Accept-Encoding"] = "identity"
        result = request.execute()

        try:
            all_row_data = result["sheets"][0]["data"][0].get("rowData", [])
        except (IndexError, KeyError):
            all_row_data = []

        for row_num in chunk:
            idx = row_num - min_row
            cells = []
            try:
                values = all_row_data[idx].get("values", [])
            except (IndexError, KeyError, TypeError):
                values = []

            for col_idx in range(max_cols):
                try:
                    ef = values[col_idx].get("effectiveFormat", {}) if col_idx < len(values) else {}
                except (KeyError, TypeError):
                    ef = {}
                cells.append(_cell_format_from_effective(ef))

            fmt_map[row_num] = {"cells": cells}

    return fmt_map


def _apply_fmts(target_sheet, paste_start: int, fmts: list[dict | None]):
    """
    套用 A:BJ 逐格格式到目標工作表，並將列高固定為 21px。
    """
    if not fmts:
        return

    requests = []
    for i, fmt in enumerate(fmts):
        row_num = paste_start + i
        cells = (fmt or {}).get("cells") or [{} for _ in range(62)]

        for col_idx, cell_fmt in enumerate(cells[:62]):
            if not cell_fmt:
                continue

            fields = []
            for key in [
                "backgroundColor",
                "textFormat",
                "horizontalAlignment",
                "verticalAlignment",
                "wrapStrategy",
                "numberFormat",
                "textRotation",
            ]:
                if key in cell_fmt:
                    fields.append(f"userEnteredFormat.{key}")

            if not fields:
                continue

            requests.append({
                "repeatCell": {
                    "range": {
                        "sheetId":          target_sheet.id,
                        "startRowIndex":    row_num - 1,
                        "endRowIndex":      row_num,
                        "startColumnIndex": col_idx,
                        "endColumnIndex":   col_idx + 1,
                    },
                    "cell":   {"userEnteredFormat": cell_fmt},
                    "fields": ",".join(fields),
                }
            })

        # 列高固定 21px
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

    if template_start_row and template_start_row > 2 and category_counts:
        # 下半月：用 ④ 加工後各服務列數加總，計算精確結束列號
        total_new = sum(category_counts.values())
        end_row   = template_start_row + total_new - 1
        log(f"📋 從範本第 {template_start_row} 至第 {end_row} 列，讀取 {total_new} 筆")
        # 確保工作表有足夠列數
        if template.row_count < end_row:
            template.add_rows(end_row - template.row_count + 10)
        raw  = template.get(
            f"A{template_start_row}:BJ{end_row}",
            value_render_option="UNFORMATTED_VALUE",
        ) or []
        data = raw
    elif template_start_row and template_start_row > 2:
        # 下半月但無 category_counts：用工作表實際最後列
        last = template.row_count
        raw  = template.get(
            f"A{template_start_row}:BJ{last}",
            value_render_option="UNFORMATTED_VALUE",
        ) or []
        while raw and not any(str(c).strip() for c in raw[-1]):
            raw.pop()
        data = raw
        log(f"📋 分類第 {template_start_row} 列起的 {len(data)} 筆新資料")
    else:
        data = get_all_data(template, "A2", "BJ")
        log(f"📋 範本共 {len(data)} 筆，開始分類")

    if not data:
        raise Exception("無資料可分類，請先執行搬運和加工")

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
            if cat == "清潔":
                actual = len(cleaning_rows)
            elif cat in other_buckets:
                actual = len(other_buckets[cat])
            else:
                continue
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
        """
        data 中的 0-based index → 範本工作表 1-based 列號。
        下半月：data 從 template_start_row 開始讀，所以 index 0 = template_start_row。
        上半月：data 從第 2 列開始，所以 index 0 = 第 2 列。
        """
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
            paste_start = _get_period_paste_row(target, first_half, log_fn=log)
            paste_data(target, paste_start, rows)
            counts[label] = len(rows)
            log(f"✅ {label}：{len(rows)} 筆 → {sheet_name}")

            # 搬移格式（底色 + 字型 + 列高 21px）
            try:
                import traceback
                src_rows = [_sheet_row(i) for i in row_indices]
                log(f"🔵 {label} 讀取格式（{len(src_rows)} 列）...")
                fmt_map  = _fetch_row_fmts(
                    spreadsheet_id = reconciliation_id,
                    sheet_title    = template_sheet.title,
                    row_nums       = src_rows,
                )
                fmts = [fmt_map.get(r) for r in src_rows]
                _apply_fmts(target, paste_start, fmts)
                log(f"🔵 {label} 格式搬移完成")
            except Exception as fe:
                log(f"⚠️ {label} 格式搬移失敗：{fe}")
                log(f"⚠️ 詳細：{traceback.format_exc()[:300]}")

        except Exception as e:
            st.warning(f"⚠️ {sheet_name} 寫入失敗：{e}")
            counts[label] = 0

    # ── 再搬清潔承攬 ──────────────────────────────────────────
    if cleaning_rows:
        try:
            clean_sheet = ss_clean.worksheet("清潔營收明細")
            paste_start = _get_period_paste_row(clean_sheet, first_half, log_fn=log)
            paste_data(clean_sheet, paste_start, cleaning_rows)
            counts["清潔"] = len(cleaning_rows)
            log(f"✅ 清潔：{len(cleaning_rows)} 筆 → 清潔營收明細")

            # 搬移格式（底色 + 字型 + 列高 21px）
            try:
                import traceback
                src_rows = [_sheet_row(i) for i in cleaning_row_indices]
                log(f"🔵 清潔讀取格式（{len(src_rows)} 列）...")
                fmt_map  = _fetch_row_fmts(
                    spreadsheet_id = reconciliation_id,
                    sheet_title    = template_sheet.title,
                    row_nums       = src_rows,
                )
                fmts = [fmt_map.get(r) for r in src_rows]
                _apply_fmts(clean_sheet, paste_start, fmts)
                log(f"🔵 清潔格式搬移完成")
            except Exception as fe:
                log(f"⚠️ 清潔格式搬移失敗：{fe}")
                log(f"⚠️ 詳細：{traceback.format_exc()[:300]}")

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


# ═══════════════════════════════════════════════════════════════
# ⑨ 搬運ATM
#
# ATM資料的來源不是像發票/藍新那樣每期轉檔出來的檔案，而是各地區
# 「請款」試算表（地區設定的 allowance_id）裡一份持續累積、跨期別的
# 「ATM」工作表。AA:AG 這組鏡射公式（AA=A、AB=I、AC=J、AD=K、AE=L、
# AF=N、AG=O）只有零星示範列有填，大部分列是空的，所以要：
#   1. 用 A 欄（ATM日期）篩出屬於本期別（年/月）的列
#   2. 幫這些列補上 AA:AG 公式（沒有就新增，已有就覆蓋，結果一樣）
#   3. 讀回算出來的值，貼進「金流對帳」03ATM工作表的 A:G
#      （一樣先清空再貼，不累加）
# ═══════════════════════════════════════════════════════════════

ATM_ALLOWANCE_SHEET_NAME = "ATM"


def move_atm_from_allowance(
    allowance_id: str, root_folder_id: str, period: str, region_name: str, log_fn=None
) -> dict:
    """
    ⑨ 搬運ATM：從地區「請款」試算表（allowance_id）的「ATM」工作表，
    篩出A欄日期屬於本期別（年/月）的列，幫這些列補上 AA:AG 公式
    （AA=A、AB=I、AC=J、AD=K、AE=L、AF=N、AG=O——大部分列本來沒有這組
    公式，只有零星示範列有），再把算出來的值貼進「金流對帳」03ATM
    工作表的 A:G（清空後重新貼入）。
    """
    def log(msg):
        if log_fn:
            log_fn(msg)

    if not allowance_id:
        raise Exception("這個地區的地區設定缺少 allowance_id（請款 ID）")

    reconciliation_id = _get_period_file_id(root_folder_id, period, "金流對帳", region_name)
    ss_recon = open_spreadsheet(reconciliation_id)
    atm_sheet = ss_recon.worksheet("03ATM")

    ss_allowance = open_spreadsheet(allowance_id)
    allowance_ws = ss_allowance.worksheet(ATM_ALLOWANCE_SHEET_NAME)
    a_col = allowance_ws.get("A2:A") or []
    log(f"📋 讀取請款試算表「{ATM_ALLOWANCE_SHEET_NAME}」{len(a_col)} 列")

    month_text = _month_text(period)
    matched_row_nums = [
        offset + 2 for offset, row in enumerate(a_col)
        if _extract_year_month(row[0] if row else "") == month_text
    ]
    log(f"🔵 本期（{month_text}）共 {len(matched_row_nums)} 列")

    matched_rows: list[list] = []
    if matched_row_nums:
        formula_updates = [
            {
                "range": f"AA{r}:AG{r}",
                "values": [[f"=A{r}", f"=I{r}", f"=J{r}", f"=K{r}", f"=L{r}", f"=N{r}", f"=O{r}"]],
            }
            for r in matched_row_nums
        ]
        allowance_ws.batch_update(formula_updates, value_input_option="USER_ENTERED")
        log(f"🔵 已補上 AA:AG 公式：{len(matched_row_nums)} 列")

        min_r, max_r = min(matched_row_nums), max(matched_row_nums)
        block = allowance_ws.get(f"AA{min_r}:AG{max_r}") or []
        wanted = set(matched_row_nums)
        for i, row_num in enumerate(range(min_r, max_r + 1)):
            if row_num not in wanted:
                continue
            vals = block[i] if i < len(block) else []
            matched_rows.append(vals + [""] * (7 - len(vals)))

    atm_sheet.batch_clear([f"A2:G{max(atm_sheet.row_count, 2)}"])
    count = paste_data(atm_sheet, 2, matched_rows) if matched_rows else 0
    log(f"✅ 已清空「03ATM」A2:G 後重新貼入：{count} 筆")

    atm_sheet.batch_clear([f"AA2:AA{max(atm_sheet.row_count, 2)}"])
    _mark_atm_non_service_rows(atm_sheet, log_fn=log)

    return {"count": count}


# ═══════════════════════════════════════════════════════════════
# ⑨ 金流對帳彙總與檢核
#
# 「金流對帳」工作表 A:BJ 跟「範本」同一份配置，緊接著 BK:BX 是核對
# 用的欄位（BK1＝本期別月份，供 BR:BU 既有公式的 $BK$1 參照；BL:BX
# 是從範本欄位整理出來、逐列下拉的公式，包含 BR～BU 分別對 00發票／
# 01藍新收款／02藍新退款／03ATM 四張工作表的 VLOOKUP 核對結果）：
#
#   BJ=62 VIP券的訂單編號　BK=63 期別月份　BL=64 付款日期
#   BM=65 訂單編號　BN=66 付款方式　BO=67 發票號碼　BP=68 金額
#   BQ=69 檢核　BR=70 發票核對　BS=71 藍新收款　BT=72 藍新退款
#   BU=73 ATM　BV=74 專員收現　BW=75 異動費用　BX=76 消毒服務
#
# ⑨-1 只搬 A2:BJ（跟其他步驟一致），BK1 固定設月份公式；BL:BQ 用
# 「複製第2列公式」的方式延伸到 B 欄最後一筆，不動 BK2:BK 人工確認欄。
# ═══════════════════════════════════════════════════════════════

RECONCILIATION_SHEET_NAME = "金流對帳"

COL_REC_VIP_ORDER   = 62  # BJ VIP券的訂單編號
COL_REC_MONTH       = 63  # BK 期別月份（例如 "2026/07"）
COL_REC_PAID_DATE   = 64  # BL 付款日期
COL_REC_ORDER_NO    = 65  # BM 訂單編號
COL_REC_PAY_METHOD  = 66  # BN 付款方式
COL_REC_INVOICE_NO  = 67  # BO 發票號碼
COL_REC_AMOUNT      = 68  # BP 金額
COL_REC_CHECK       = 69  # BQ 檢核
COL_REC_INVOICE_CHK = 70  # BR 發票核對（比對 00發票）
COL_REC_NEWEBPAY_IN = 71  # BS 藍新收款（比對 01藍新收款）
COL_REC_NEWEBPAY_RE = 72  # BT 藍新退款（比對 02藍新退款）
COL_REC_ATM_CHK     = 73  # BU ATM（比對 03ATM）
REC_LAST_COL        = 76  # BX 消毒服務，BK:BX 公式區塊的最後一欄

# 四張來源工作表：訂單編號欄（0-based）、日期欄（0-based，用來篩本期）、
# 以及「金流對帳」對應的核對欄。02藍新退款／03ATM 另有 alt_key_idx：
# 拆退款/異動費等子單在來源表會補上 "-1" 後綴的鏡射欄，跟「金流對帳」
# 拆出來的子單訂單編號（LCxxxx-1）對得上。

# 四張來源表的欄位排布已經統一（使用者在表上手動整理過）：AA＝手動加註的
# "-1" 等子單後綴標記（預設空白），AB＝訂單編號（已經把AA的後綴併進去的
# 最終比對鍵值，金流對帳BR:BU公式查的就是這欄開始的 AB:AC），AC＝比對
# 結果值。四張表只有各自原本的日期欄位不同。
_SOURCE_KEY_IDX = 27    # AB（0-based）：四張表統一的訂單編號比對鍵值欄
_SOURCE_MARKER_IDX = 26  # AA（0-based）：手動加註的子單後綴標記欄

SOURCE_SHEETS = {
    "00發票": {
        "rec_col": COL_REC_INVOICE_CHK,
        "key_idx": _SOURCE_KEY_IDX,
        "date_idx": 3,      # D 訂單日期
        "read_end_col": "AG",
    },
    "01藍新收款": {
        "rec_col": COL_REC_NEWEBPAY_IN,
        "key_idx": _SOURCE_KEY_IDX,
        "date_idx": 1,      # B 訂單交易日期
        "read_end_col": "AG",
    },
    "02藍新退款": {
        "rec_col": COL_REC_NEWEBPAY_RE,
        "key_idx": _SOURCE_KEY_IDX,
        "date_idx": 8,      # I 商店執行日期
        "read_end_col": "AG",
    },
    "03ATM": {
        "rec_col": COL_REC_ATM_CHK,
        "key_idx": _SOURCE_KEY_IDX,
        "date_idx": 0,      # A ATM日期
        "read_end_col": "AG",
    },
}


def _month_text(period: str) -> str:
    """期別 "202607-2" → "2026/07"，對應「金流對帳」BK1 的格式。"""
    return f"{period[:4]}/{period[4:6]}"


def _extract_year_month(value) -> str | None:
    """從各種日期字串（"2026/6/22"、"2026-07-31 21:45:24" 等）取出
    "YYYY/MM"；取不到就回傳 None。"""
    text = str(value or "").strip()
    m = re.match(r"^(\d{4})[/-](\d{1,2})", text)
    if not m:
        return None
    y, mo = m.groups()
    return f"{int(y):04d}/{int(mo):02d}"


def _clean_order_key(value) -> str:
    return str(value or "").strip()


def _is_blank_or_error(value) -> bool:
    text = str(value or "").strip()
    return text == "" or text.startswith("#")


PAY_METHOD_CREDIT_CARD = "信用卡"
PAY_METHOD_NEWEBPAY_ATM = "藍新ATM"
PAY_METHOD_ATM = "ATM"


def _row_needs_check(sheet_name: str, pay_method: str, amount: float, invoice_no: str) -> bool:
    """複製「金流對帳」BR:BU 既有公式各自的判斷條件（不含月份，月份在
    呼叫端先篩過），決定這一列該不該對這張來源表有值。"""
    if sheet_name == "00發票":
        return bool(invoice_no) and amount > 0
    if sheet_name == "01藍新收款":
        return pay_method in (PAY_METHOD_CREDIT_CARD, PAY_METHOD_NEWEBPAY_ATM) and amount > 0
    if sheet_name == "02藍新退款":
        return pay_method == PAY_METHOD_CREDIT_CARD and amount < 0
    if sheet_name == "03ATM":
        return pay_method == PAY_METHOD_ATM
    return False


def _to_number(value) -> float:
    if isinstance(value, (int, float)):
        return float(value)
    text = str(value or "").replace(",", "").strip()
    if not text:
        return 0.0
    try:
        return float(text)
    except ValueError:
        return 0.0


def copy_template_to_reconciliation(
    root_folder_id: str, period: str, region_name: str, log_fn=None
) -> dict:
    """
    ⑨-1 範本彙總，固定順序：
      1. 清空「金流對帳」A2:BJ。
      2. 把「範本」A2:BJ 搬到「金流對帳」A2:BJ。
      3. 設定 BK1 月份公式，再將 BL:BQ 下拉到 B 欄最後一筆。

    完成以上搬運後，才由 setup_reconciliation_marks 清空核對欄與底色，
    然後進行比對、異常標記及篩選。

    「金流對帳」是每次執行都完整反映「範本」目前內容的快照，不是像
    範本本身那樣分上/下半月累加——所以每次都先清空 A2:BJ，再從第2列
    重新貼入，不會一直往下新增列數。

    貼入後，把「金流對帳」BL:BQ 從第2列往下複製到 B 欄最後一筆；
    BR:BU 原有公式會依 BK1 的月份判斷是否比對四張來源表。
    """
    def log(msg):
        if log_fn:
            log_fn(msg)

    reconciliation_id = _get_period_file_id(root_folder_id, period, "金流對帳", region_name)
    ss       = open_spreadsheet(reconciliation_id)
    template = ss.worksheet("範本")
    recon    = ss.worksheet(RECONCILIATION_SHEET_NAME)

    data = get_all_data(template, "A2", "BJ")
    if not data:
        raise Exception("「範本」無資料，請先完成③～⑤搬運/加工/分類")

    log(f"📋 讀取「範本」{len(data)} 筆")

    _clear_a2_bj_contents_and_formats(recon, log_fn=log)
    count = paste_data(recon, 2, data)
    log(f"✅ 已將「範本」A2:BJ 搬到「{RECONCILIATION_SHEET_NAME}」A2:BJ：{count} 筆")

    # BK 是人工確認欄，只有 BK1 放本期月份公式，不可把 BK1/BK2 往下拉。
    recon.update_acell(f"{_column_letter(COL_REC_MONTH)}1", '=TEXT(H2,"YYYY/MM")')
    log(f"🔵 「{RECONCILIATION_SHEET_NAME}」BK1 已設為 =TEXT(H2,\"YYYY/MM\")")

    # 公式只拉到 B 欄最後一筆非空白資料，避免空白列也進入對帳。
    formula_count = max(
        (idx for idx, row in enumerate(data, start=1)
         if len(row) >= 2 and str(row[1] or "").strip()),
        default=0,
    )
    if formula_count > 1:
        try:
            requests = [{
                "copyPaste": {
                    "source": {
                        "sheetId": recon.id,
                        "startRowIndex": 1,
                        "endRowIndex": 2,
                        "startColumnIndex": COL_REC_PAID_DATE - 1,
                        "endColumnIndex": COL_REC_CHECK,
                    },
                    "destination": {
                        "sheetId": recon.id,
                        "startRowIndex": 1,
                        "endRowIndex": 1 + formula_count,
                        "startColumnIndex": COL_REC_PAID_DATE - 1,
                        "endColumnIndex": COL_REC_CHECK,
                    },
                    "pasteType": "PASTE_FORMULA",
                    "pasteOrientation": "NORMAL",
                }
            }]
            ss.batch_update({"requests": requests})
            log(f"🔵 已將 BL:BQ 公式從第 2 列複製到 B 欄最後一筆（共 {formula_count} 列）")
        except Exception as e:
            log(f"⚠️ BL:BQ 公式複製失敗，請手動下拉公式：{e}")

    return {"count": count, "start_row": 2}


def _load_source_rows(ss, sheet_name: str, meta: dict) -> list[list]:
    ws = ss.worksheet(sheet_name)
    return get_all_data(ws, "A2", meta["read_end_col"])


def _strip_order_suffix(order_no: str) -> str:
    """"LC00211258-1" → "LC00211258"；沒有 "-數字" 後綴就原樣回傳。"""
    return re.sub(r"-\d+$", "", order_no)


def _source_key_set(rows: list[list], meta: dict) -> set[str]:
    keys = set()
    for row in rows:
        key = _clean_order_key(row[meta["key_idx"]] if len(row) > meta["key_idx"] else "")
        if key:
            keys.add(key)
    return keys


def _source_keys_in_period(rows: list[list], meta: dict, month_text: str) -> set[str]:
    keys = set()
    for row in rows:
        date_val = row[meta["date_idx"]] if len(row) > meta["date_idx"] else ""
        if _extract_year_month(date_val) != month_text:
            continue
        key = _clean_order_key(row[meta["key_idx"]] if len(row) > meta["key_idx"] else "")
        if key:
            keys.add(key)
    return keys


def check_reconciliation(
    root_folder_id: str, period: str, region_name: str, log_fn=None
) -> dict:
    """
    ⑨-2 對帳檢核：只要「金流對帳」BL欄（付款日期）的月份＝本期別
    （BK1），這一列就需要檢查 BR（發票核對）、BS（藍新收款）、BT（藍新
    退款）、BU（ATM）——但四欄各自何時「應該」有值，是照工作表本身
    既有公式的判斷條件來（不是每一列四欄都要有值）：

      BR：發票號碼(BO)<>0 且 金額(BP)>0 → 應比對 00發票
      BS：付款方式(BN)＝信用卡或藍新ATM 且 金額(BP)>0 → 應比對 01藍新收款
      BT：付款方式(BN)＝信用卡 且 金額(BP)<0（退款是負數）→ 應比對 02藍新退款
      BU：付款方式(BN)＝ATM → 應比對 03ATM

    只有「這一列照上面條件本來就應該對得上」但欄位仍空白/錯誤時，才
    算一筆缺漏；不適用的欄位（例如儲值金列，四欄都不適用）不會列入，
    也不會去查來源表。

    對不上的欄位，直接查對應來源工作表的訂單編號欄，區分兩種最常見
    原因：
      1. 來源工作表查無此訂單編號
         → 通常是還沒執行⑥～⑧搬運，或訂單編號格式不一致
      2. 來源工作表查得到此訂單編號，但金流對帳欄位仍空白/錯誤
         → 通常是該列 BK:BX 公式還沒往下拉，或日期不在 BK1 期別內
    """
    def log(msg):
        if log_fn:
            log_fn(msg)

    reconciliation_id = _get_period_file_id(root_folder_id, period, "金流對帳", region_name)
    ss    = open_spreadsheet(reconciliation_id)
    recon = ss.worksheet(RECONCILIATION_SHEET_NAME)

    month_text = _month_text(period)
    all_rows = recon.get(f"A2:{_column_letter(REC_LAST_COL)}")
    log(f"📋 讀取「{RECONCILIATION_SHEET_NAME}」{len(all_rows)} 列，篩選本期（{month_text}）")

    source_key_sets = {}
    for name, meta in SOURCE_SHEETS.items():
        rows = _load_source_rows(ss, name, meta)
        source_key_sets[name] = _source_key_set(rows, meta)
        log(f"🔵 {name}：{len(rows)} 筆（全表）")

    issues: list[dict] = []
    checked = 0
    for offset, row in enumerate(all_rows):
        row_num = offset + 2

        def cell(idx_1based, _row=row):
            idx = idx_1based - 1
            return _row[idx] if idx < len(_row) else ""

        if _extract_year_month(cell(COL_REC_PAID_DATE)) != month_text:
            continue

        order_no = _clean_order_key(cell(COL_REC_ORDER_NO))
        if not order_no:
            continue

        invoice_no = str(cell(COL_REC_INVOICE_NO) or "").strip()
        pay_method = str(cell(COL_REC_PAY_METHOD) or "").strip()
        amount     = _to_number(cell(COL_REC_AMOUNT))

        row_checked = False
        for name, meta in SOURCE_SHEETS.items():
            if not _row_needs_check(name, pay_method, amount, invoice_no):
                continue  # 這一列照公式條件本來就不需要比對這張來源表
            row_checked = True
            if not _is_blank_or_error(cell(meta["rec_col"])):
                continue
            col_letter = _column_letter(meta["rec_col"])
            if order_no in source_key_sets[name]:
                reason_type = "來源查得到但金流對帳仍空白"
                reason = (
                    f"{name} 查得到訂單 {order_no}，但「{RECONCILIATION_SHEET_NAME}」"
                    f"{col_letter}{row_num} 仍空白/錯誤，請檢查該列 BK:BX 公式是否已"
                    f"下拉，或付款日期是否落在 {month_text}"
                )
            else:
                base_order = _strip_order_suffix(order_no)
                if base_order != order_no and base_order in source_key_sets[name]:
                    reason_type = "子單需在來源表加註後綴"
                    reason = (
                        f"{name} 查無子單訂單 {order_no}，但查得到母單 {base_order}——"
                        f"子單有時無法帶入發票/第三方金流系統，請到 {name} 找到 {base_order} "
                        f"那一列，在 AA 欄加註「{order_no[len(base_order):]}」（例如 -1），"
                        f"讓 AB 欄自動併出 {order_no} 後即可核對"
                    )
                else:
                    reason_type = "來源查無此訂單"
                    reason = f"{name} 查無訂單 {order_no}，請確認是否已執行⑥～⑧搬運，或訂單編號格式是否一致"
            issues.append({
                "row": row_num, "order_no": order_no, "sheet": name,
                "column": col_letter, "reason_type": reason_type, "reason": reason,
            })

        if row_checked:
            checked += 1

    # 相同問題（同一張來源表＋同一種原因）排在一起，方便一次看完同類問題，
    # 不用在上千筆裡跳著找。
    issues.sort(key=lambda it: (it["sheet"], it["reason_type"], it["row"]))

    # 只記錄「來源工作表／原因類型」的彙總筆數到執行日誌，逐筆明細改由
    # 呼叫端（Streamlit UI）用表格呈現，避免上千筆訊息洗版執行日誌。
    summary: dict[tuple[str, str], int] = {}
    for issue in issues:
        key = (issue["sheet"], issue["reason_type"])
        summary[key] = summary.get(key, 0) + 1
    for (sheet_name, reason_type), count in sorted(summary.items(), key=lambda kv: -kv[1]):
        log(f"❌ {sheet_name}／{reason_type}：{count} 筆")

    log(f"===== 對帳檢核完成：本期需對帳 {checked} 筆，缺漏 {len(issues)} 筆 =====")
    return {"checked": checked, "issues": issues}


def reverse_check_sources(
    root_folder_id: str, period: str, region_name: str, log_fn=None
) -> dict:
    """
    ⑨-3 反向比對：分別檢查 00發票／01藍新收款／02藍新退款／03ATM 本期
    （依各自的日期欄篩選月份＝本期別）的每一筆訂單編號，是否都能在
    「金流對帳」BM欄（訂單編號）找到；找不到的視為金流對帳漏收/漏搬運，
    逐筆列出。

    「金流對帳」這邊比對用的訂單編號集合不依月份篩選（用整張表），
    因為同一份金流對帳檔案本來就會跨月（例如7月檔案裡也有6月才收款
    的ATM訂單），只依「金流對帳」是否找得到這筆訂單編號來判斷，比較
    不會誤判成漏收。
    """
    def log(msg):
        if log_fn:
            log_fn(msg)

    reconciliation_id = _get_period_file_id(root_folder_id, period, "金流對帳", region_name)
    ss    = open_spreadsheet(reconciliation_id)
    recon = ss.worksheet(RECONCILIATION_SHEET_NAME)

    month_text = _month_text(period)
    recon_orders = {
        _clean_order_key(v)
        for v in recon.col_values(COL_REC_ORDER_NO)[1:]
        if _clean_order_key(v)
    }
    log(f"📋 「{RECONCILIATION_SHEET_NAME}」訂單編號共 {len(recon_orders)} 筆（全表，不分月份）")

    result: dict[str, dict] = {}
    for name, meta in SOURCE_SHEETS.items():
        rows = _load_source_rows(ss, name, meta)
        period_keys = _source_keys_in_period(rows, meta, month_text)
        missing = sorted(k for k in period_keys if k not in recon_orders)
        result[name] = {"period_count": len(period_keys), "missing": missing}
        log(f"🔵 {name}：本期（{month_text}）{len(period_keys)} 筆，"
            f"「{RECONCILIATION_SHEET_NAME}」查無 {len(missing)} 筆")
        # 逐筆訂單編號改由呼叫端（Streamlit UI）用表格呈現，不逐筆寫進執行日誌。

    return result


# ═══════════════════════════════════════════════════════════════
# 比對前清空＋異常淺青色2標註（人工確認後自動消色）
#
# 執行對帳前先清空「確認欄」的值與比對欄底色，避免看到上次殘留的標註
# 或舊確認值；比對欄空白/錯誤視為異常，用 Google Sheets 原生「條件式
# 格式」標淺青色2——只要設定一次規則就會持續生效，之後人工在確認欄
# （金流對帳BK／00發票AH／01藍新收款AF／02藍新退款AF／03ATM AF）填值，
# Sheets 會自動依規則重新判斷並把底色消掉，不用重跑程式。
#
# 條件式格式完成後，再用 BasicFilter 依「淺青色2」可見底色篩選。
# ═══════════════════════════════════════════════════════════════

LIGHT_CYAN_2 = {"red": 162 / 255, "green": 196 / 255, "blue": 198 / 255}

# 各工作表：比對前要清空的確認/標記欄（1-based欄號）、判斷是否消色的
# 確認欄，以及要重設底色＋加註異常標註的比對欄範圍（起訖皆1-based）。
_ABNORMAL_FORMULA_REC = (
    'IF(OR($BO2="儲值金",$BO2="不開立發票"),FALSE,'
    'IFERROR(OR($BQ2<>0,ISNA($BR2),ISNA($BS2),ISNA($BT2),ISNA($BU2),'
    'AND($BR2<>"",$BR2<>$BO2),AND($BS2<>"",$BS2<>$BP2),'
    'AND($BT2<>"",$BP2<>$BT2),AND($BU2<>"",$BU2<>$BP2)),TRUE))'
)
# 比較運算遇到 #N/A 會直接傳播錯誤，條件格式就不會生效。
# 外層 IFERROR(...,TRUE) 保證 AD/AE/AF/AG 任一錯誤都會被視為異常；
# 各張表的免異常條件放在最外層 IF，優先於通用異常條件。
_ABNORMAL_FORMULA_INVOICE = (
    'IF(AND($AA2="-1",IFERROR($AG2=0,FALSE)),FALSE,'
    'IFERROR(OR(ISNA($AD2),ISNA($AF2),ISNA($AG2),$AG2<>0),TRUE))'
)
_ABNORMAL_FORMULA_NEWEBPAY_IN = (
    'IFERROR(OR(ISNA($AD2),ISNA($AE2),$AE2<>0),TRUE)'
)
_ABNORMAL_FORMULA_NEWEBPAY_REFUND = (
    'IF(IFERROR(VLOOKUP($AB2,INDIRECT("\'金流對帳\'!$BM:$BP"),4,FALSE)'
    '+$AC2=0,FALSE),FALSE,'
    'IFERROR(OR(ISNA($AD2),ISNA($AE2),$AE2<>0),TRUE))'
)
_ABNORMAL_FORMULA_ATM = (
    'IF(OR($G2="新訓費",$G2="車馬費",'
    'AND($G2="減時",IFERROR($AC2+$AD2=0,FALSE))),FALSE,'
    'IFERROR(OR(ISNA($AD2),ISNA($AE2),$AE2<>0),TRUE))'
)

MARK_SHEETS = {
    RECONCILIATION_SHEET_NAME: {
        "clear_cols": [COL_REC_MONTH],       # BK2:BK
        "confirm_col": COL_REC_MONTH,        # BK 非空白 → 消色
        "mark_start": COL_REC_CHECK,         # BQ
        "mark_end": COL_REC_ATM_CHK,         # BU
        "abnormal_formula": _ABNORMAL_FORMULA_REC,
    },
    "00發票": {
        "clear_cols": [27, 34],              # AA2:AA、AH2:AH
        "confirm_col": 34,                   # AH
        "mark_start": 28,                    # AB
        "mark_end": 33,                      # AG
        "abnormal_formula": _ABNORMAL_FORMULA_INVOICE,
    },
    "01藍新收款": {
        "clear_cols": [27, 32],              # AA2:AA、AF2:AF
        "confirm_col": 32,                   # AF
        "mark_start": 28,                    # AB
        "mark_end": 31,                      # AE
        "abnormal_formula": _ABNORMAL_FORMULA_NEWEBPAY_IN,
    },
    "02藍新退款": {
        "clear_cols": [32],                  # AF2:AF（無AA）
        "confirm_col": 32,
        "mark_start": 28,
        "mark_end": 31,
        "abnormal_formula": _ABNORMAL_FORMULA_NEWEBPAY_REFUND,
    },
    "03ATM": {
        "clear_cols": [27, 32],              # AA2:AA、AF2:AF
        "confirm_col": 32,
        "mark_start": 28,
        "mark_end": 31,
        "abnormal_formula": _ABNORMAL_FORMULA_ATM,
    },
}


def _mark_atm_non_service_rows(ws, log_fn=None) -> int:
    """在03ATM的AA欄加註子單後綴：只處理 A:G 有資料，且 G 欄
    不是「服務費用」的列。呼叫端需先清空 AA2:AA，避免舊標記殘留。
    """
    def log(msg):
        if log_fn:
            log_fn(msg)

    max_rows = max(ws.row_count, 2)
    rows = ws.get(f"A2:G{max_rows}") or []
    marker_updates = []
    for offset, row in enumerate(rows):
        if not any(str(value or "").strip() for value in row):
            continue
        category = str(row[6] if len(row) > 6 else "").strip()
        if category != "服務費用":
            marker_updates.append({
                "range": f"AA{offset + 2}",
                "values": [["-1"]],
            })

    if marker_updates:
        ws.batch_update(marker_updates, value_input_option="USER_ENTERED")
    log(f"🔵 「03ATM」AA欄已加註 -1：{len(marker_updates)} 列（G欄非服務費用）")
    return len(marker_updates)


def _clear_marks_and_backgrounds(ws, cfg: dict, log_fn=None) -> None:
    """比對前：清空確認/標記欄的值，並把比對欄範圍底色重設為無色。"""
    def log(msg):
        if log_fn:
            log_fn(msg)

    max_rows = max(ws.row_count, 2)

    clear_ranges = [f"{_column_letter(c)}2:{_column_letter(c)}{max_rows}" for c in cfg["clear_cols"]]
    ws.batch_clear(clear_ranges)

    requests = [{
        "repeatCell": {
            "range": {
                "sheetId": ws.id,
                "startRowIndex": 1,
                "endRowIndex": max_rows,
                "startColumnIndex": cfg["mark_start"] - 1,
                "endColumnIndex": cfg["mark_end"],
            },
            # backgroundColor={} 會被 Sheets 當成 RGB(0,0,0)，就是截圖的黑底。
            # 格式物件不帶 backgroundColor，再指定 fields，才是真正「移除底色」。
            "cell": {"userEnteredFormat": {}},
            "fields": "userEnteredFormat.backgroundColor",
        }
    }]
    ws.spreadsheet.batch_update({"requests": requests})
    log(f"🧹 「{ws.title}」已清空 {'、'.join(_column_letter(c) for c in cfg['clear_cols'])} 欄並重設底色")


def _remove_basic_filter(ws, log_fn=None) -> None:
    """清空及重建異常篩選前，先移除舊 BasicFilter，讓所有列恢復顯示。"""
    def log(msg):
        if log_fn:
            log_fn(msg)

    ws.spreadsheet.batch_update({"requests": [{
        "clearBasicFilter": {"sheetId": ws.id}
    }]})
    log(f"🧹 「{ws.title}」已移除舊篩選結果")


def _replace_abnormal_highlight_rule(ws, cfg: dict, log_fn=None) -> None:
    """
    設定條件式格式：mark_start:mark_end 範圍內，cfg["abnormal_formula"]
    判定異常且確認欄未填時標淺青色2；確認欄一填值，Sheets 自動重新判斷
    並消色。每次都先移除同範圍舊規則再新增，避免重跑後規則越疊越多。
    """
    def log(msg):
        if log_fn:
            log_fn(msg)

    start_idx = cfg["mark_start"] - 1
    end_idx = cfg["mark_end"]

    meta = ws.spreadsheet.fetch_sheet_metadata()
    for s in meta.get("sheets", []):
        if s["properties"]["sheetId"] != ws.id:
            continue
        old_indexes = [
            idx for idx, rule in enumerate(s.get("conditionalFormats", []))
            if any(rng.get("sheetId") == ws.id
                   and rng.get("startColumnIndex") == start_idx
                   and rng.get("endColumnIndex") == end_idx
                   for rng in rule.get("ranges", []))
        ]
        for idx in sorted(old_indexes, reverse=True):
            ws.spreadsheet.batch_update({"requests": [{
                "deleteConditionalFormatRule": {"sheetId": ws.id, "index": idx}
            }]})
        break

    max_rows = max(ws.row_count, 2)
    confirm_letter = _column_letter(cfg["confirm_col"])
    first_col_letter = _column_letter(cfg["mark_start"])

    extra_conditions = [f'${confirm_letter}2=""']
    if ws.title == RECONCILIATION_SHEET_NAME:
        # 只比對 BL 付款日期屬於 BK1 月份的列；其他月份不著色。
        extra_conditions.insert(0, 'IFERROR(TEXT($BL2,"yyyy/mm")=$BK$1,FALSE)')

    condition_formula = (
        f'=AND({",".join(extra_conditions)},{cfg["abnormal_formula"]})'
    )

    ws.spreadsheet.batch_update({"requests": [{
        "addConditionalFormatRule": {
            "rule": {
                "ranges": [{
                    "sheetId": ws.id,
                    "startRowIndex": 1,
                    "endRowIndex": max_rows,
                    "startColumnIndex": start_idx,
                    "endColumnIndex": end_idx,
                }],
                "booleanRule": {
                    "condition": {
                        "type": "CUSTOM_FORMULA",
                        "values": [{
                            "userEnteredValue": condition_formula
                        }],
                    },
                    "format": {"backgroundColor": LIGHT_CYAN_2},
                },
            },
            "index": 0,
        }
    }]})
    log(f"🔵 「{ws.title}」已設定 {first_col_letter}:{_column_letter(cfg['mark_end'])} "
        f"淺青色2異常標註（{confirm_letter} 非空白自動消色）")


def _set_abnormal_filter(ws, cfg: dict, log_fn=None) -> None:
    """只顯示淺青色2異常列。「金流對帳」再叠加 BL 月份＝BK1，
    確保例如 202607-2 只會顯示 2026/07 的異常資料。
    """
    def log(msg):
        if log_fn:
            log_fn(msg)

    max_rows = max(ws.row_count, 2)
    filter_specs = [{
        "columnIndex": cfg["mark_start"] - 1,
        "filterCriteria": {
            "visibleBackgroundColorStyle": {"rgbColor": LIGHT_CYAN_2}
        }
    }]
    if ws.title == RECONCILIATION_SHEET_NAME:
        filter_specs.append({
            "columnIndex": COL_REC_PAID_DATE - 1,
            "filterCriteria": {
                "condition": {
                    "type": "CUSTOM_FORMULA",
                    "values": [{
                        "userEnteredValue": '=IFERROR(TEXT($BL2,"yyyy/mm")=$BK$1,FALSE)'
                    }],
                }
            }
        })

    ws.spreadsheet.batch_update({"requests": [{
        "setBasicFilter": {
            "filter": {
                "range": {
                    "sheetId": ws.id,
                    "startRowIndex": 0,
                    "endRowIndex": max_rows,
                    "startColumnIndex": 0,
                    "endColumnIndex": cfg["mark_end"],
                },
                "filterSpecs": filter_specs,
            }
        }
    }]})
    if ws.title == RECONCILIATION_SHEET_NAME:
        log("🔎 「金流對帳」已篩選 BL 為 BK1 月份，並只顯示淺青色2異常列")
    else:
        log(f"🔎 「{ws.title}」已只顯示淺青色2異常列")


PURCHASE_SEARCH_URL = (
    "https://backend.lemonclean.com.tw/purchase?keyword=&name=&phone=&orderNo={order_no}"
    "&date_s=&date_e=&clean_date_s=&clean_date_e=&paid_at_s=&paid_at_e="
    "&refundDateS=&refundDateE=&buy=&area_id=&isCharge=&isRefund=&payway="
    "&purchase_status=&progress_status=&invoiceStatus=&otherFee=&orderBy="
)


def _hyperlink_formula(order_no: str, service_date) -> str:
    """以服務日期為顯示文字，連到後台訂單查詢；訂單編號先移除 -1/-2。"""
    base_order = _strip_order_suffix(_clean_order_key(order_no))
    date_text = str(service_date or "").strip().replace('"', '""')
    url = PURCHASE_SEARCH_URL.format(order_no=base_order).replace('"', '""')
    return f'=HYPERLINK("{url}","{date_text}")'


def _batch_formula_updates(ws, updates: list[dict], chunk_size: int = 500) -> None:
    for start in range(0, len(updates), chunk_size):
        ws.batch_update(
            updates[start:start + chunk_size],
            value_input_option="USER_ENTERED",
        )


def _add_service_date_order_links(ss, log_fn=None) -> dict[str, int]:
    """
    在異常規則與篩選建立後加上訂單連結：
      - 金流對帳：BV（服務日期來自本列 H，訂單來自 BM）
      - 00發票：AI（AH 保留為人工確認欄）
      - 01藍新收款／02藍新退款／03ATM：AG（AF 保留為人工確認欄）
        （以 AB 訂單編號回查金流對帳 BM 對應的 H 服務日期）。
    """
    def log(msg):
        if log_fn:
            log_fn(msg)

    recon = ss.worksheet(RECONCILIATION_SHEET_NAME)
    recon_rows = recon.get(f"A2:{_column_letter(COL_REC_ORDER_NO)}") or []

    service_date_by_order: dict[str, object] = {}
    recon_updates: list[dict] = []
    for offset, row in enumerate(recon_rows):
        order_no = _clean_order_key(
            row[COL_REC_ORDER_NO - 1] if len(row) >= COL_REC_ORDER_NO else ""
        )
        service_date = row[7] if len(row) > 7 else ""  # H 服務日期
        if not order_no or not str(service_date or "").strip():
            continue
        base_order = _strip_order_suffix(order_no)
        service_date_by_order.setdefault(order_no, service_date)
        service_date_by_order.setdefault(base_order, service_date)
        recon_updates.append({
            "range": f"BV{offset + 2}",
            "values": [[_hyperlink_formula(order_no, service_date)]],
        })

    recon.batch_clear([f"BV2:BV{max(recon.row_count, 2)}"])
    _batch_formula_updates(recon, recon_updates)
    counts = {RECONCILIATION_SHEET_NAME: len(recon_updates)}
    log(f"🔗 「{RECONCILIATION_SHEET_NAME}」BV 已加入服務日期訂單連結：{len(recon_updates)} 列")

    source_link_cols = {
        "00發票": "AI",
        "01藍新收款": "AG",
        "02藍新退款": "AG",
        "03ATM": "AG",
    }
    for sheet_name, link_col in source_link_cols.items():
        ws = ss.worksheet(sheet_name)
        rows = ws.get(f"AB2:AB{max(ws.row_count, 2)}") or []
        updates: list[dict] = []
        for offset, row in enumerate(rows):
            order_no = _clean_order_key(row[0] if row else "")
            if not order_no:
                continue
            service_date = (
                service_date_by_order.get(order_no)
                or service_date_by_order.get(_strip_order_suffix(order_no))
            )
            if not service_date:
                continue
            updates.append({
                "range": f"{link_col}{offset + 2}",
                "values": [[_hyperlink_formula(order_no, service_date)]],
            })
        ws.batch_clear([f"{link_col}2:{link_col}{max(ws.row_count, 2)}"])
        _batch_formula_updates(ws, updates)
        counts[sheet_name] = len(updates)
        log(f"🔗 「{sheet_name}」{link_col} 已加入服務日期訂單連結：{len(updates)} 列")

    return counts


def setup_reconciliation_marks(
    root_folder_id: str, period: str, region_name: str, log_fn=None
) -> None:
    """
    比對前執行：清空各表確認/標記欄與底色，並設定淺青色2異常標註的條件式
    格式。規則只需設定一次即持續生效，之後人工填確認欄會自動消色，不用
    重跑本函式；但重跑也安全（會先移除同範圍舊規則再重建，不會疊加）。

    條件式格式建立後自動依淺青色2篩選；金流對帳另同時限定
    BL 付款日期的月份等於 BK1。
    """
    def log(msg):
        if log_fn:
            log_fn(msg)

    reconciliation_id = _get_period_file_id(root_folder_id, period, "金流對帳", region_name)
    ss = open_spreadsheet(reconciliation_id)

    for sheet_name, cfg in MARK_SHEETS.items():
        ws = ss.worksheet(sheet_name)
        _remove_basic_filter(ws, log_fn=log)
        _clear_marks_and_backgrounds(ws, cfg, log_fn=log)
        if sheet_name == "03ATM":
            # 先清空 AA，再依 G 欄重建 -1；完成後才進行異常比對。
            _mark_atm_non_service_rows(ws, log_fn=log)
        _replace_abnormal_highlight_rule(ws, cfg, log_fn=log)
        _set_abnormal_filter(ws, cfg, log_fn=log)

    _add_service_date_order_links(ss, log_fn=log)

    log("===== 比對前清空、異常標註與篩選設定完成 =====")
