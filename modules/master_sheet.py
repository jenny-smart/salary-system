"""
主控試算表模組
LemonSalarySystem
ID：1GdW3FSZ0s3TGeYiNx3JtYvED_RRfJjiFYwLFeYHZ1hA

欄位結構：
  第1行：作業名稱 | 202601-1 | （空） | 202601-2 | （空） | ...
  第2行：（空）   | 筆數     | 完成時間 | 筆數    | 完成時間 | ...
  第3行起：作業資料（金流對帳）
  空白行
  清潔承攬標題
  清潔承攬作業
"""

import pytz
from datetime import datetime
from modules.auth import open_spreadsheet

MASTER_SHEET_ID = "1GdW3FSZ0s3TGeYiNx3JtYvED_RRfJjiFYwLFeYHZ1hA"
TAIPEI_TZ = pytz.timezone("Asia/Taipei")

START_YEAR = 2026
START_MONTH = 1
YEARS = 3

DATA_START_ROW = 3  # 作業資料從第3行開始


# ═══════════════════════════════════════
# 作業清單
# ═══════════════════════════════════════

PAYMENT_TASKS = [
    "排程期別資料夾",
    "排程期別資料夾",
    "排程期別金流對帳",
    "排程期別專員薪資表",
    "排程期別服務分潤表",
    "排程期別元大帳戶",
    "排程手動資料夾",
    "手動期別資料夾",
    "手動期別金流對帳",
    "手動期別清潔承攬",
    "手動期別其他承攬",
    "手動期別元大帳戶",
    "期別訂單轉檔",
    "複製期別訂單",
    "加工-排序",
    "加工-K欄標註異常標橘底",
    "加工-水洗加工",
    "加工-家電加工",
    "加工-收納加工",
    "加工-座椅加工",
    "加工-地毯加工",
    "複製清潔訂單",
    "複製水洗訂單",
    "複製家電訂單",
    "複製收納訂單",
    "複製座椅訂單",
    "複製地毯訂單",
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

CLEANING_TASKS = [
    "薪資表整理",
    "00調薪",
    "01專員請款",
    "02儲值獎金",
    "03新人實境",
    "04新人實習",
    "05組長津貼",
    "06工具包押金",
    "07介紹獎金",
    "08季獎金",
    "09薪資結算整理",
    "一鍵執行",
    "新人實境期別標註",
    "元大帳戶",
]

CLEANING_TITLE_ROW = DATA_START_ROW + len(PAYMENT_TASKS) + 1
CLEANING_DATA_START = CLEANING_TITLE_ROW + 1

# task_key → 行號對照
TASK_ROW_MAP = {}
for i, name in enumerate(PAYMENT_TASKS):
    TASK_ROW_MAP[f"金流-{name}"] = DATA_START_ROW + i
for i, name in enumerate(CLEANING_TASKS):
    TASK_ROW_MAP[f"清潔-{name}"] = CLEANING_DATA_START + i


# ═══════════════════════════════════════
# 欄號計算
# ═══════════════════════════════════════

def period_to_col(period: str) -> int:
    """期別 → 筆數欄號（B=2 起）"""
    year = int(period[:4])
    month = int(period[4:6])
    half = int(period[7])
    months_from_start = (year - START_YEAR) * 12 + (month - START_MONTH)
    return 2 + months_from_start * 4 + (half - 1) * 2


def col_to_letter(n: int) -> str:
    """欄號轉字母"""
    result = ""
    while n > 0:
        n, r = divmod(n - 1, 26)
        result = chr(65 + r) + result
    return result


def _build_header_rows() -> tuple[list, list]:
    """產生第1行（期別）和第2行（筆數/完成時間）"""
    row1 = ["作業名稱"]
    row2 = [""]
    for y in range(YEARS):
        year = START_YEAR + y
        for month in range(1, 13):
            for half in [1, 2]:
                period = f"{year}{str(month).zfill(2)}-{half}"
                row1.extend([period, ""])
                row2.extend(["筆數", "完成時間"])
    return row1, row2


# ═══════════════════════════════════════
# 初始化 / 更新地區工作表
# ═══════════════════════════════════════

def init_region_sheet(region_name: str) -> bool:
    """
    建立或更新地區工作表
    - 工作表不存在：全新建立
    - 工作表已存在：
        只更新標題行（第1、2行）和 A 欄作業名稱
        已有的筆數和完成時間資料完全保留
    回傳 True=新建，False=更新
    """
    ss = open_spreadsheet(MASTER_SHEET_ID)

    is_new = False
    try:
        sheet = ss.worksheet(region_name)
    except Exception:
        sheet = ss.add_worksheet(title=region_name, rows=200, cols=400)
        is_new = True

    row1, row2 = _build_header_rows()

    # 更新標題行（不影響資料欄）
    sheet.update("A1", [row1])
    sheet.update("A2", [row2])

    # 更新 A 欄作業名稱（只寫 A 欄，不動 B 欄以後的資料）
    payment_a = [[name] for name in PAYMENT_TASKS]
    sheet.update(f"A{DATA_START_ROW}", payment_a)

    # 清潔承攬標題
    sheet.update(f"A{CLEANING_TITLE_ROW}", [["清潔承攬"]])

    # 清潔承攬 A 欄
    cleaning_a = [[name] for name in CLEANING_TASKS]
    sheet.update(f"A{CLEANING_DATA_START}", cleaning_a)

    return is_new


# ═══════════════════════════════════════
# 打卡
# ═══════════════════════════════════════

def record_execution(
    region_name: str,
    period: str,
    task_key: str,
    count: int = None,
) -> bool:
    """
    記錄執行結果
    task_key：「金流-作業名稱」或「清潔-作業名稱」
    count：筆數（None 只記時間）
    """
    row = TASK_ROW_MAP.get(task_key)
    if row is None:
        return False

    col = period_to_col(period)
    count_col = col_to_letter(col)
    time_col = col_to_letter(col + 1)
    time_str = datetime.now(TAIPEI_TZ).strftime("%Y/%m/%d %H:%M:%S")

    ss = open_spreadsheet(MASTER_SHEET_ID)
    sheet = ss.worksheet(region_name)

    updates = []
    if count is not None:
        updates.append({"range": f"{count_col}{row}", "values": [[count]]})
    updates.append({"range": f"{time_col}{row}", "values": [[time_str]]})
    sheet.batch_update(updates)

    return True


def record_batch(region_name: str, period: str, records: list) -> None:
    """
    批次打卡
    records = [{"task_key": "金流-期別訂單轉檔", "count": 427}, ...]
    """
    ss = open_spreadsheet(MASTER_SHEET_ID)
    sheet = ss.worksheet(region_name)
    time_str = datetime.now(TAIPEI_TZ).strftime("%Y/%m/%d %H:%M:%S")

    updates = []
    for record in records:
        task_key = record.get("task_key")
        count = record.get("count")
        row = TASK_ROW_MAP.get(task_key)
        if row is None:
            continue
        col = period_to_col(period)
        count_col = col_to_letter(col)
        time_col = col_to_letter(col + 1)
        if count is not None:
            updates.append({"range": f"{count_col}{row}", "values": [[count]]})
        updates.append({"range": f"{time_col}{row}", "values": [[time_str]]})

    if updates:
        sheet.batch_update(updates)
