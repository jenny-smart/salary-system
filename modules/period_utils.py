"""
期別工具模組
共用於金流對帳、清潔承攬、其他承攬

期別格式：YYYYMM-N
例如：202504-1（上半月）、202504-2（下半月）
"""

import re
from datetime import datetime
import pytz

TAIPEI_TZ = pytz.timezone("Asia/Taipei")

# 檔案類型名稱（建立期別時複製的四類檔案）
PERIOD_FILE_LABELS = ["金流對帳", "清潔承攬", "其他承攬", "元大帳戶"]


def is_valid_period(period: str) -> bool:
    """驗證期別格式是否正確"""
    return bool(re.match(r"^\d{6}-[12]$", period))


def is_first_half(period: str) -> bool:
    """判斷是否為上半月"""
    return period.endswith("-1")


def get_previous_period(period: str) -> str:
    """
    取得上一期別
    202505-2 → 202505-1
    202505-1 → 202504-2
    202501-1 → 202412-2（跨年）
    """
    year = int(period[:4])
    month = int(period[4:6])
    half = period[7]

    if half == "2":
        return f"{year}{str(month).zfill(2)}-1"
    else:
        month -= 1
        if month < 1:
            month = 12
            year -= 1
        return f"{year}{str(month).zfill(2)}-2"


def get_period_display(period: str) -> str:
    """
    取得期別顯示文字
    202504-1 → 2025年04月 上半月
    """
    year = period[:4]
    month = period[4:6]
    half = "上半月" if is_first_half(period) else "下半月"
    return f"{year}年{month}月 {half}"


def get_file_name(period: str, label: str, region_name: str) -> str:
    """
    依命名規則組出檔名
    例如：202504-1金流對帳-台北
    """
    return f"{period}{label}-{region_name}"


def get_current_taipei_time() -> datetime:
    """取得台北目前時間"""
    return datetime.now(TAIPEI_TZ)


def format_taipei_time(dt: datetime = None, fmt: str = "%Y/%m/%d %H:%M:%S") -> str:
    """格式化台北時間"""
    if dt is None:
        dt = get_current_taipei_time()
    if dt.tzinfo is None:
        dt = TAIPEI_TZ.localize(dt)
    return dt.strftime(fmt)


def get_auto_period() -> str:
    """
    依台北今天日期自動判斷期別
    1-15 日 → 上半月（-1）
    16-31 日 → 下半月（-2）
    """
    now = get_current_taipei_time()
    year = now.strftime("%Y")
    month = now.strftime("%m")
    half = "1" if now.day <= 15 else "2"
    return f"{year}{month}-{half}"
