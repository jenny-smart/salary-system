"""
主控試算表模組
用於記錄各地區各期別的執行狀況

試算表：LemonSalarySystem
ID：1GdW3FSZ0s3TGeYiNx3JtYvED_RRfJjiFYwLFeYHZ1hA

工作表結構：每個地區一個工作表（例如「新北」）
欄位：
  A欄：作業名稱
  B欄：202601-1 筆數
  C欄：202601-1 完成時間
  D欄：202601-2 筆數
  E欄：202601-2 完成時間
  F欄：202602-1 筆數
  ...以此類推（每月4欄，從202601開始）
"""

import pytz
from datetime import datetime
from modules.auth import open_spreadsheet

MASTER_SHEET_ID = "1GdW3FSZ0s3TGeYiNx3JtYvED_RRfJjiFYwLFeYHZ1hA"
TAIPEI_TZ = pytz.timezone("Asia/Taipei")

# 起始年月
START_YEAR = 2026
START_MONTH = 1
# 產生幾年的欄位（預設3年）
YEARS = 3


# ═══════════════════════════════════════
# 金流對帳作業清單（行號對應）
# ═══════════════════════════════════════

PAYMENT_TASKS = [
    # (作業名稱, 是否為區塊標題)
    ("排程期別資料夾", True),   # 紅色標題
    ("排程期別資料夾", False),
    ("排程期別金流對帳", False),
    ("排程期別專員薪資表", False),
    ("排程期別服務分潤表", False),
    ("排程期別元大帳戶", False),
    ("排程手動資料夾", True),   # 紅色標題
    ("手動期別資料夾", False),
    ("手動期別金流對帳", False),
    ("手動期別清潔承攬", False),
    ("手動期別其他承攬", False),
    ("手動期別元大帳戶", False),
    ("期別訂單轉檔", False),
    ("複製期別訂單", False),
    ("加工-排序", False),
    ("加工-K欄標註異常標橘底", False),
    ("加工-水洗加工", False),
    ("加工-家電加工", False),
    ("加工-收納加工", False),
    ("加工-座椅加工", False),
    ("加工-地毯加工", False),
    ("複製清潔訂單", False),
    ("複製水洗訂單", False),
    ("複製家電訂單", False),
    ("複製收納訂單", False),
    ("複製座椅訂單", False),
    ("複製地毯訂單", False),
    ("期別發票解壓縮", False),
    ("期別發票轉檔", False),
    ("期別已退款全部加收轉檔", False),
    ("期別已退款全部退款轉檔", False),
    ("期別預收轉檔", False),
    ("期別藍新收款轉檔", False),
    ("期別藍新退款轉檔", False),
    ("複製已退款全部加收", False),
    ("複製已退款全部退款", False),
    ("複製預收", False),
    ("複製發票", False),
    ("複製藍新收款", False),
    ("複製藍新退款", False),
]

CLEANING_TASKS = [
    ("薪資表整理", False),
    ("00調薪", False),
    ("01專員請款", False),
    ("02儲值獎金", False),
    ("03新人實境", False),
    ("04新人實習", False),
    ("05組長津貼", False),
    ("06工具包押金", False),
    ("07介紹獎金", False),
    ("08季獎金", False),
    ("09薪資結算整理", False),
    ("一鍵執行", False),
    ("新人實境期別標註", False),
    ("元大帳戶", False),
]

# 各作業名稱對應的行號（1-based）
# 第1行是標題行，金流對帳從第2行開始
PAYMENT_START_ROW = 2
CLEANING_SECTION_TITLE_ROW = PAYMENT_START_ROW + len(PAYMENT_TASKS) + 1  # 空一行
CLEANING_START_ROW = CLEANING_SECTION_TITLE_ROW + 1

# 建立作業名稱 → 行號的對應表
TASK_ROW_MAP = {}
for i, (name, _) in enumerate(PAYMENT_TASKS):
    TASK_ROW_MAP[f"金流-{name}"] = PAYMENT_START_ROW + i
for i, (name, _) in enumerate(CLEANING_TASKS):
    TASK_ROW_MAP[f"清潔-{name}"] = CLEANING_START_ROW + i


# ═══════════════════════════════════════
# 欄號計算
# ═══════════════════════════════════════

def period_to_col(period: str) -> int:
    """
    將期別轉換為欄號（1-based）
    202601-1 → 2（B欄）
    202601-2 → 4（D欄）
    202602-1 → 6（F欄）
    ...
    A欄=1（作業名稱），B欄起為期別資料
    """
    year = int(period[:4])
    month = int(period[4:6])
    half = int(period[7])

    months_from_start = (year - START_YEAR) * 12 + (month - START_MONTH)
    col = 2 + months_from_start * 4 + (half - 1) * 2
    return col  # 筆數欄，+1 是完成時間欄


def col_to_letter(n: int) -> str:
    """欄號轉字母（1→A, 27→AA...）"""
    result = ""
    while n > 0:
        n, r = divmod(n - 1, 26)
        result = chr(65 + r) + result
    return result


def get_period_headers() -> list:
    """產生所有期別的標題列（第1行）"""
    headers = ["作業名稱"]
    for y in range(YEARS):
        year = START_YEAR + y
        for month in range(1, 13):
            for half in [1, 2]:
                period = f"{year}{str(month).zfill(2)}-{half}"
                headers.append(f"{period} 筆數")
                headers.append(f"{period} 完成時間")
    return headers


# ═══════════════════════════════════════
# 初始化地區工作表
# ═══════════════════════════════════════

def init_region_sheet(region_name: str) -> bool:
    """
    在主控試算表建立地區工作表
    填入標題行和所有作業名稱
    若工作表已存在則跳過
    回傳 True=新建，False=已存在
    """
    ss = open_spreadsheet(MASTER_SHEET_ID)

    # 檢查是否已存在
    try:
        existing = ss.worksheet(region_name)
        return False  # 已存在
    except Exception:
        pass

    # 新增工作表
    sheet = ss.add_worksheet(title=region_name, rows=200, cols=300)

    # 第1行：標題
    headers = get_period_headers()
    sheet.update("A1", [headers])

    # 金流對帳區
    payment_rows = []
    for name, _ in PAYMENT_TASKS:
        payment_rows.append([name])
    sheet.update(f"A{PAYMENT_START_ROW}", payment_rows)

    # 清潔承攬區標題
    sheet.update(f"A{CLEANING_SECTION_TITLE_ROW}", [["清潔承攬"]])

    # 清潔承攬作業
    cleaning_rows = []
    for name, _ in CLEANING_TASKS:
        cleaning_rows.append([name])
    sheet.update(f"A{CLEANING_START_ROW}", cleaning_rows)

    return True


# ═══════════════════════════════════════
# 打卡（記錄執行結果）
# ═══════════════════════════════════════

def record_execution(
    region_name: str,
    period: str,
    task_key: str,
    count: int = None,
    timestamp: datetime = None
) -> bool:
    """
    記錄執行結果到主控試算表
    
    task_key 格式：「金流-作業名稱」或「清潔-作業名稱」
    例如：「金流-期別訂單轉檔」、「清潔-01專員請款」
    
    count：筆數（None 表示不記錄筆數，只記時間）
    """
    row = TASK_ROW_MAP.get(task_key)
    if row is None:
        return False

    col = period_to_col(period)
    count_col = col_to_letter(col)
    time_col = col_to_letter(col + 1)

    if timestamp is None:
        timestamp = datetime.now(TAIPEI_TZ)
    time_str = timestamp.strftime("%Y/%m/%d %H:%M:%S")

    ss = open_spreadsheet(MASTER_SHEET_ID)
    sheet = ss.worksheet(region_name)

    if count is not None:
        sheet.update(f"{count_col}{row}", [[count]])
    sheet.update(f"{time_col}{row}", [[time_str]])

    return True


def record_batch(region_name: str, period: str, records: list) -> None:
    """
    批次打卡（一次更新多個作業）
    records = [{"task_key": "金流-期別訂單轉檔", "count": 427}, ...]
    """
    ss = open_spreadsheet(MASTER_SHEET_ID)
    sheet = ss.worksheet(region_name)
    timestamp = datetime.now(TAIPEI_TZ)
    time_str = timestamp.strftime("%Y/%m/%d %H:%M:%S")

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
            updates.append({
                "range": f"{count_col}{row}",
                "values": [[count]]
            })
        updates.append({
            "range": f"{time_col}{row}",
            "values": [[time_str]]
        })

    if updates:
        sheet.batch_update(updates)
