"""
主控試算表模組
LemonSalarySystem
ID：1GdW3FSZ0s3TGeYiNx3JtYvED_RRfJjiFYwLFeYHZ1hA

欄位結構：
  第1行：作業名稱 | 202601-1 | （空） | 202601-2 | （空） | ...
  第2行：（空）   | ID/筆數  | 完成時間 | ID/筆數 | 完成時間 | ...
  第3行起：作業資料

設計原則：
  - 打卡時用 A 欄比對作業名稱找行號，不依賴固定行號
  - 新增作業時插入整列，舊資料自動往下移
  - 區塊標題（清潔承攬）只作標記用，不打卡
"""

import pytz
from datetime import datetime
from modules.auth import open_spreadsheet

MASTER_SHEET_ID = "1GdW3FSZ0s3TGeYiNx3JtYvED_RRfJjiFYwLFeYHZ1hA"
TAIPEI_TZ = pytz.timezone("Asia/Taipei")

START_YEAR = 2026
START_MONTH = 1
YEARS = 3
DATA_START_ROW = 3


# ═══════════════════════════════════════
# 作業清單（定義打卡表的列順序）
# "__TITLE__:xxx" = 區塊標題列（A欄顯示 xxx，不打卡）
# "__BLANK__"     = 空白列
# ═══════════════════════════════════════

PAYMENT_TASKS = [
    "__TITLE__:排程期別資料夾",
    "排程期別資料夾",
    "排程期別金流對帳",
    "排程期別專員薪資表",
    "排程期別服務分潤表",
    "排程期別元大帳戶",
    "__TITLE__:排程手動資料夾",
    "手動期別資料夾",
    "手動期別金流對帳",
    "手動期別清潔承攬",
    "手動期別其他承攬",
    "手動期別元大帳戶",
    "期別訂單轉檔",
    "訂單起始列",
    "複製期別訂單",
    "加工-排序",
    "加工-K欄標註異常標橘底",
    "加工前-清潔主單數",
    "加工前-水洗主單數",
    "加工前-家電主單數",
    "加工前-收納主單數",
    "加工前-座椅主單數",
    "加工前-地毯主單數",
    "加工後-清潔主單數",
    "加工後-水洗主單數",
    "加工後-家電主單數",
    "加工後-收納主單數",
    "加工後-座椅主單數",
    "加工後-地毯主單數",
    "加工-清潔加工列數",
    "加工-水洗加工列數",
    "加工-家電加工列數",
    "加工-收納加工列數",
    "加工-座椅加工列數",
    "加工-地毯加工列數",
    "加工-儲值金列數",
    "複製清潔訂單列數",
    "複製水洗訂單列數",
    "複製家電訂單列數",
    "複製收納訂單列數",
    "複製座椅訂單列數",
    "複製地毯訂單列數",
    "期別發票解壓縮",
    "期別發票轉檔",
    "期別已退款全部加收轉檔",
    "期別已退款全部退款轉檔",
    "期別預收轉檔",
    "期別藍新收款轉檔",
    "期別藍新退款轉檔",
    "複製已退款全部加收",
    "複製已退款全部退款",
    "複製預收",
    "複製發票",
    "複製藍新收款",
    "複製藍新退款",
]

# ★ 對應主控表圖片（第59-72列）★
CLEANING_TASKS = [
    "__TITLE__:清潔承攬",
    "前置作業",
    "00調薪",
    "01專員請款",
    "02儲值獎金",
    "03新人實境",
    "04新人實習",
    "05組長津貼",
    "06季獎金",
    "結算作業",
    "一鍵執行",
    "新人實境實習期別",
    "工具包押金",
    "元大帳戶",
]

ALL_TASKS = PAYMENT_TASKS + ["__BLANK__"] + CLEANING_TASKS


def _display_name(task: str) -> str:
    """取得 A 欄顯示名稱"""
    if task.startswith("__TITLE__:"):
        return task[10:]
    if task == "__BLANK__":
        return ""
    return task


def _is_data_row(task: str) -> bool:
    """是否為可打卡的資料列"""
    return not task.startswith("__TITLE__") and task != "__BLANK__"


# ═══════════════════════════════════════
# 欄號計算
# ═══════════════════════════════════════

def period_to_col(period: str) -> int:
    year  = int(period[:4])
    month = int(period[4:6])
    half  = int(period[7])
    months_from_start = (year - START_YEAR) * 12 + (month - START_MONTH)
    return 2 + months_from_start * 4 + (half - 1) * 2


def col_to_letter(n: int) -> str:
    result = ""
    while n > 0:
        n, r = divmod(n - 1, 26)
        result = chr(65 + r) + result
    return result


def _build_header_rows():
    row1 = ["作業名稱"]
    row2 = [""]
    for y in range(YEARS):
        year = START_YEAR + y
        for month in range(1, 13):
            for half in [1, 2]:
                period = f"{year}{str(month).zfill(2)}-{half}"
                row1.extend([period, ""])
                row2.extend(["ID/筆數", "完成時間"])
    return row1, row2


# ═══════════════════════════════════════
# 行號查找（A 欄比對）
# ═══════════════════════════════════════

def _find_row(sheet, task_name: str) -> int | None:
    """在 A 欄找作業名稱，回傳行號（1-based）或 None"""
    a_col = sheet.col_values(1)
    for i, val in enumerate(a_col):
        if val and val.strip() == task_name.strip():
            return i + 1
    return None


def _get_all_a_col(sheet) -> list:
    return [v.strip() if v else "" for v in sheet.col_values(1)]


# ═══════════════════════════════════════
# 初始化 / 更新地區工作表
# ═══════════════════════════════════════

def init_region_sheet(region_name: str) -> bool:
    """
    建立或更新地區工作表。
    - 新建：填入標題行和所有作業名稱
    - 已存在：只更新標題行（第1、2行），不插入任何列
      （避免重複執行時不斷插入作業列破壞現有打卡資料）
    回傳 True=新建，False=已存在
    """
    ss = open_spreadsheet(MASTER_SHEET_ID)

    is_new = False
    try:
        sheet = ss.worksheet(region_name)
    except Exception:
        sheet = ss.add_worksheet(title=region_name, rows=200, cols=400)
        is_new = True

    # 更新標題行（只改第1、2行，不影響資料）
    row1, row2 = _build_header_rows()
    sheet.update("A1", [row1])
    sheet.update("A2", [row2])

    if is_new:
        # 全新建立：直接寫入所有作業名稱
        task_rows = [[_display_name(t)] for t in ALL_TASKS]
        sheet.update(f"A{DATA_START_ROW}", task_rows)

    # 已存在的工作表不做任何插入，避免破壞現有打卡資料
    return is_new


# ═══════════════════════════════════════
# 打卡
# ═══════════════════════════════════════

def record_execution(
    region_name: str,
    period: str,
    task_key: str,
    count=None,
) -> bool:
    """
    記錄執行結果。
    task_key：作業名稱（直接對應 A 欄）
    count：ID 或筆數（None 只記時間）
    """
    try:
        ss    = open_spreadsheet(MASTER_SHEET_ID)
        sheet = ss.worksheet(region_name)
        row   = _find_row(sheet, task_key)
        if row is None:
            import streamlit as st
            st.warning(f"⚠️ 打卡找不到作業：{task_key}")
            return False

        col       = period_to_col(period)
        count_col = col_to_letter(col)
        time_col  = col_to_letter(col + 1)
        time_str  = datetime.now(TAIPEI_TZ).strftime("%Y/%m/%d %H:%M:%S")

        updates = []
        if count is not None:
            updates.append({"range": f"{count_col}{row}", "values": [[count]]})
        updates.append({"range": f"{time_col}{row}", "values": [[time_str]]})
        sheet.batch_update(updates)
        return True
    except Exception as e:
        import streamlit as st
        st.warning(f"⚠️ 打卡失敗 [{task_key}]：{e}")
        return False


def record_batch(region_name: str, period: str, records: list) -> None:
    """
    批次打卡。
    records = [{"task_key": "複製清潔訂單列數", "count": 153}, ...]
    count 可省略（None = 只記完成時間）
    """
    try:
        ss    = open_spreadsheet(MASTER_SHEET_ID)
        sheet = ss.worksheet(region_name)
        time_str = datetime.now(TAIPEI_TZ).strftime("%Y/%m/%d %H:%M:%S")

        a_col = _get_all_a_col(sheet)

        def find_row_from_cache(task_name: str) -> int | None:
            for i, val in enumerate(a_col):
                if val == task_name.strip():
                    return i + 1
            return None

        updates   = []
        not_found = []
        for record in records:
            task_key = record.get("task_key", "")
            count    = record.get("count")
            row      = find_row_from_cache(task_key)
            if row is None:
                not_found.append(task_key)
                continue
            col       = period_to_col(period)
            count_col = col_to_letter(col)
            time_col  = col_to_letter(col + 1)
            if count is not None:
                updates.append({"range": f"{count_col}{row}", "values": [[count]]})
            updates.append({"range": f"{time_col}{row}", "values": [[time_str]]})

        if not_found:
            import streamlit as st
            st.warning(f"⚠️ 打卡找不到作業：{not_found}")

        if updates:
            sheet.batch_update(updates)
    except Exception as e:
        import streamlit as st
        st.warning(f"⚠️ 打卡失敗：{e}")


def get_recorded_value(region_name: str, period: str, task_key: str):
    """
    從打卡表讀取某作業的 ID/筆數欄值（供 double check 用）。
    """
    ss    = open_spreadsheet(MASTER_SHEET_ID)
    sheet = ss.worksheet(region_name)
    row   = _find_row(sheet, task_key)
    if row is None:
        return None
    col       = period_to_col(period)
    count_col = col_to_letter(col)
    val       = sheet.acell(f"{count_col}{row}").value
    return val if val else None
