"""
Google Sheets 讀寫共用模組
包含：
- 開啟試算表（依 ID）
- 找第一個空白列（上下半月貼上邏輯）
- 日期標準化
- 通用讀寫
"""

import re
from datetime import datetime
from modules.auth import get_gspread_client


# ═══════════════════════════════════════
# 開啟試算表
# ═══════════════════════════════════════

def open_spreadsheet(file_id: str):
    """用 ID 開啟試算表"""
    client = get_gspread_client()
    return client.open_by_key(file_id)


def get_worksheet(file_id: str, sheet_name: str):
    """用試算表 ID 和工作表名稱取得工作表"""
    ss = open_spreadsheet(file_id)
    return ss.worksheet(sheet_name)


# ═══════════════════════════════════════
# 上下半月貼上位置
# ═══════════════════════════════════════

def get_paste_row(sheet, is_first_half: bool, check_col: int = 2) -> int:
    """
    依上下半月決定貼上起始列
    上半月：清空 A2: 後從第 2 列開始
    下半月：找 check_col 欄最後一筆非空白的下一列
    check_col：用來判斷資料結束的欄（預設 B 欄 = 2）
    """
    if is_first_half:
        # 清空 A2 以下所有資料
        last_row = sheet.row_count
        if last_row >= 2:
            sheet.batch_clear([f"A2:BJ{last_row}"])
        return 2
    else:
        # 找最後一筆非空白列
        col_values = sheet.col_values(check_col)
        last_non_empty = 1
        for i, val in enumerate(col_values):
            if val and val.strip():
                last_non_empty = i + 1
        return last_non_empty + 1


def paste_data(sheet, start_row: int, data: list[list]) -> int:
    """
    從 start_row 開始貼入資料
    回傳貼入的筆數
    """
    if not data:
        return 0

    end_row = start_row + len(data) - 1
    range_notation = f"A{start_row}:A{end_row}"
    sheet.update(f"A{start_row}", data, value_input_option="USER_ENTERED")
    return len(data)


def col_num_to_letter(n: int) -> str:
    """
    欄位數字轉字母，支援超過 26 欄
    1→A, 26→Z, 27→AA, 28→AB, ...
    """
    result = ""
    while n > 0:
        n, remainder = divmod(n - 1, 26)
        result = chr(65 + remainder) + result
    return result


# ═══════════════════════════════════════
# 日期標準化（對應 GAS normalizeDateOnly_）
# ═══════════════════════════════════════

DATE_COLS = [2, 3, 7]  # C, D, H（0-based index）


def normalize_date(value) -> str:
    """
    將各種日期格式統一為 yyyy/M/d
    支援：datetime、Excel 數字日期、yyyy/MM/dd、yyyy-MM-dd
    """
    if not value or value == "":
        return value

    if isinstance(value, datetime):
        return value.strftime("%-Y/%-m/%-d")

    if isinstance(value, (int, float)):
        # Excel 數字日期轉換
        if 30000 < value < 60000:
            import datetime as dt
            delta = dt.timedelta(days=int(value) - 25569)
            date = dt.datetime(1970, 1, 1) + delta
            return f"{date.year}/{date.month}/{date.day}"

    if isinstance(value, str):
        text = value.strip()
        match = re.match(r"^(\d{4})[\/\-](\d{1,2})[\/\-](\d{1,2})$", text)
        if match:
            y, m, d = match.groups()
            return f"{int(y)}/{int(m)}/{int(d)}"

    return value


def normalize_row_dates(row: list) -> list:
    """
    將 row 中 C/D/H 欄（index 2/3/7）的日期統一格式
    對應 GAS normalizeSafeColumns_
    """
    row = list(row)
    for i in DATE_COLS:
        if i < len(row):
            row[i] = normalize_date(row[i])
    return row


def normalize_all_rows(rows: list[list]) -> list[list]:
    """批次處理所有資料列的日期欄"""
    return [normalize_row_dates(row) for row in rows]


# ═══════════════════════════════════════
# 通用讀取
# ═══════════════════════════════════════

def get_all_data(sheet, start: str = "A2", end: str = "BJ") -> list[list]:
    """
    讀取工作表資料，過濾空行
    """
    all_values = sheet.get(f"{start}:{end}")
    if not all_values:
        return []
    return [row for row in all_values if any(str(c).strip() for c in row)]


def find_last_non_empty_row(sheet, col: int = 2) -> int:
    """
    找指定欄的最後一筆非空白列號
    col：欄位索引（1-based）
    """
    col_values = sheet.col_values(col)
    last = 1
    for i, val in enumerate(col_values):
        if val and str(val).strip():
            last = i + 1
    return last
