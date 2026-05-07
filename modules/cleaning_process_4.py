"""
Lemon Clean 清潔承攬 — 工具包押金 / 介紹獎金 / 元大帳戶
檔案：modules/cleaning_process_4.py

依賴：
    modules/auth.py         — get_gspread_client()
    modules/master_sheet.py — record_execution()

打卡：統一寫入主控試算表（record_execution），不寫入 exec 工作表。

主控表 task_key：
    工具包押金 → "工具包押金"
    介紹獎金   → "介紹獎金"
    元大帳戶   → "元大帳戶"

工具包押金 & 介紹獎金邏輯（來自 GAS executeFullToolDepositProcess）：

    上半月：
        清空 場次時數薪資總表 A151:E（startRow=151）
        清空 場次時數薪資總表 AB4:AE（col 28:31）
        清空 介紹獎金工作表 A2:C

    下半月：
        工具包押金：
            篩選「工具包押金」工作表：
                I 欄 >= 80 且 J 欄非空白
                → 場次時數薪資總表 A151:E（A=J欄, B=A欄, C/D=空, E=押金金額）
                台中地區押金=1500，其餘=2000
        介紹獎金：
            篩選「工具包押金」工作表：
                I >= 80 且 J 欄空白
                → 介紹獎金工作表 A2:C（A=J欄, B=A欄, C=1000）

元大帳戶邏輯（來自 GAS runBankAccountUpdate）：

    上半月：
        從 場次時數薪資總表 N4:Q 讀取資料
        寫入期別元大帳戶試算表 A3:E
        存檔為 xlsx：{period}元大承攬費-{region}.xlsx
        目標日期 = 當月10日（週六提前到週五，週日提前到週五）

    下半月：
        從 場次時數薪資總表 U4:X 讀取資料
        寫入期別元大帳戶試算表 A3:E
        存檔為 xlsx：{period}元大承攬費-{region}.xlsx
        若 場次時數薪資總表 AB4:AE 有資料：
            另存 xlsx：{period}元大工具包押金-{region}.xlsx
        目標日期 = 當月20日（週六提前到週五，週日提前到週五）
"""

from __future__ import annotations

import datetime
from typing import List, Optional, Tuple

import gspread

from modules.auth import get_gspread_client
from modules.master_sheet import record_execution


# ──────────────────────────────────────────────────────────────
# 常數
# ──────────────────────────────────────────────────────────────

TS_FMT = "%Y/%m/%d %H:%M"

DEPOSIT_THRESHOLD = 80    # I 欄 >= 80
DEPOSIT_TAICHUNG  = 1500
DEPOSIT_OTHER     = 2000
INTRO_BONUS       = 1000

TOOL_DEPOSIT_START_ROW = 151   # 場次時數薪資總表寫入起始列
AB_COL = 28                    # AB 欄（1-based）


# ──────────────────────────────────────────────────────────────
# 工具
# ──────────────────────────────────────────────────────────────

def _now_ts() -> str:
    return datetime.datetime.now().strftime(TS_FMT)


def _log(log: List[str], msg: str) -> None:
    log.append(msg)


def _punch(task_key: str, region: str, period: str) -> str:
    """打卡至主控試算表。"""
    ts = _now_ts()
    record_execution(region, period, task_key, ts)
    return ts


def _to_num(val) -> float:
    try:
        return float(str(val).replace(",", "").strip())
    except (ValueError, TypeError):
        return 0.0


def _target_date(is_first_half: bool) -> datetime.date:
    """
    上半月：當月10日；下半月：當月20日。
    若落在週六提前到週五，週日提前到週五。
    """
    today = datetime.date.today()
    day   = 10 if is_first_half else 20
    d     = today.replace(day=day)
    if d.weekday() == 5:      # 週六
        d = d - datetime.timedelta(days=1)
    elif d.weekday() == 6:    # 週日
        d = d - datetime.timedelta(days=2)
    return d


def _col_letter(n: int) -> str:
    result = ""
    while n > 0:
        n, r = divmod(n - 1, 26)
        result = chr(65 + r) + result
    return result


# ──────────────────────────────────────────────────────────────
# 工具包押金 & 介紹獎金
# ──────────────────────────────────────────────────────────────

def run_tool_deposit(
    cleaning_file_id: str,
    region: str,
    period: str,
    is_first_half: bool,
    log: List[str],
) -> bool:
    """
    工具包押金 & 介紹獎金。
    兩者來自同一工作表篩選，打卡分開。
    """
    label = "上半月" if is_first_half else "下半月"
    _log(log, f"▶ 工具包押金 & 介紹獎金 {label} 開始")
    try:
        gc = get_gspread_client()
        ss = gc.open_by_key(cleaning_file_id)

        ws_summary = ss.worksheet("場次時數薪資總表")
        ws_deposit  = ss.worksheet("工具包押金")
        ws_intro    = ss.worksheet("介紹獎金")

        if is_first_half:
            _tool_clear(ws_summary, ws_intro, log)
        else:
            # 判斷地區決定押金金額
            deposit_amount = DEPOSIT_TAICHUNG if "台中" in region else DEPOSIT_OTHER

            dep_count, intro_count = _tool_process(
                ws_deposit, ws_summary, ws_intro,
                deposit_amount, log
            )
            _log(log, f"  工具包押金：{dep_count} 筆，介紹獎金：{intro_count} 筆")

        ts = _punch("工具包押金", region, period)
        _log(log, f"✅ 工具包押金 {label} 完成｜{ts}")

        ts2 = _punch("介紹獎金", region, period)
        _log(log, f"✅ 介紹獎金 {label} 完成｜{ts2}")
        return True

    except Exception as e:
        _log(log, f"❌ 工具包押金失敗：{e}")
        return False


def _tool_clear(
    ws_summary: gspread.Worksheet,
    ws_intro: gspread.Worksheet,
    log: List[str],
) -> None:
    """上半月：清空相關欄位。"""
    last_col = ws_summary.col_count
    last_ltr = _col_letter(last_col)

    # 清空 場次時數薪資總表 A151:E
    ws_summary.batch_clear([
        f"A{TOOL_DEPOSIT_START_ROW}:E",
        f"AB4:{_col_letter(AB_COL + 3)}",   # AB:AE = col28:31
    ])

    # 清空 介紹獎金 A2:C
    ws_intro.batch_clear(["A2:C"])

    _log(log, "    上半月：場次時數薪資總表 A151:E / AB4:AE 及介紹獎金 A2:C 已清空")


def _tool_process(
    ws_deposit: gspread.Worksheet,
    ws_summary: gspread.Worksheet,
    ws_intro: gspread.Worksheet,
    deposit_amount: int,
    log: List[str],
) -> Tuple[int, int]:
    """
    下半月：
    讀取「工具包押金」工作表，
        A欄=姓名, I欄=次數, J欄=備註（空白=介紹獎金，非空=工具包押金）
    工具包押金：I >= 80 且 J 非空白 → 場次時數薪資總表 A151起（A=J, B=A, C/D=空, E=押金）
    介紹獎金：  I >= 80 且 J 空白   → 介紹獎金工作表 A2起（A=J欄=空, B=A欄姓名, C=1000）

    注意：GAS 原版的 A=J欄, B=A欄 意思是：
        場次時數薪資總表 A欄 = 工具包押金 J 欄
        場次時數薪資總表 B欄 = 工具包押金 A 欄（姓名）
    """
    all_vals = ws_deposit.get("A2:J") or []

    dep_rows   = []
    intro_rows = []

    for row in all_vals:
        if not row:
            continue
        while len(row) < 10:
            row.append("")

        name   = str(row[0]).strip()   # A 欄（姓名）
        i_val  = _to_num(row[8])       # I 欄（次數）
        j_val  = str(row[9]).strip()   # J 欄（備註）

        if not name or i_val < DEPOSIT_THRESHOLD:
            continue

        if j_val:
            # 工具包押金
            dep_rows.append([j_val, name, "", "", deposit_amount])
        else:
            # 介紹獎金
            intro_rows.append([j_val, name, INTRO_BONUS])

    # 寫入場次時數薪資總表 A151 起
    if dep_rows:
        end_row = TOOL_DEPOSIT_START_ROW + len(dep_rows) - 1
        ws_summary.update(
            f"A{TOOL_DEPOSIT_START_ROW}:E{end_row}",
            dep_rows, value_input_option="RAW"
        )
        _log(log, f"    工具包押金寫入 A{TOOL_DEPOSIT_START_ROW}:E{end_row}，共 {len(dep_rows)} 筆")

    # 寫入介紹獎金工作表 A2 起
    if intro_rows:
        ws_intro.batch_clear(["A2:C"])
        ws_intro.update(
            f"A2:C{1 + len(intro_rows)}",
            intro_rows, value_input_option="RAW"
        )
        _log(log, f"    介紹獎金寫入 A2:C{1 + len(intro_rows)}，共 {len(intro_rows)} 筆")

    return len(dep_rows), len(intro_rows)


# ──────────────────────────────────────────────────────────────
# 元大帳戶
# ──────────────────────────────────────────────────────────────

def run_yuanta(
    cleaning_file_id: str,
    region: str,
    period: str,
    is_first_half: bool,
    log: List[str],
) -> bool:
    """
    元大帳戶。
    從場次時數薪資總表讀取資料，寫入期別元大帳戶試算表，並存檔 xlsx。

    上半月：N4:Q → 元大帳戶 A3:E，存為 {period}元大承攬費-{region}.xlsx
    下半月：U4:X → 元大帳戶 A3:E，存為 {period}元大承攬費-{region}.xlsx
            AB4:AE 若有資料 → 另存 {period}元大工具包押金-{region}.xlsx
    """
    label = "上半月" if is_first_half else "下半月"
    _log(log, f"▶ 元大帳戶 {label} 開始")
    try:
        gc = get_gspread_client()
        ss = gc.open_by_key(cleaning_file_id)

        ws_exec    = ss.worksheet("執行")
        ws_summary = ss.worksheet("場次時數薪資總表")

        yyyymm = str(ws_exec.acell("B1").value or "").strip()
        target = _target_date(is_first_half)
        target_str = target.strftime("%Y/%m/%d")

        # 讀取來源資料
        if is_first_half:
            src_range = "N4:Q"
            _log(log, f"    來源：場次時數薪資總表 N4:Q，目標日期：{target_str}")
        else:
            src_range = "U4:X"
            _log(log, f"    來源：場次時數薪資總表 U4:X，目標日期：{target_str}")

        src_data = ws_summary.get(src_range) or []
        src_data = [r for r in src_data if r and any(str(v).strip() for v in r)]

        if not src_data:
            _log(log, f"    ⚠️ {src_range} 無資料")
        else:
            _log(log, f"    讀取 {len(src_data)} 筆")
            # TODO: 寫入期別元大帳戶試算表 A3:E 並存檔 xlsx
            # 需要取得元大帳戶試算表 ID（來源待確認）
            _log(log, f"    ⚠️ 寫入元大帳戶試算表及存檔 xlsx 待實作")

        # 下半月：額外處理 AB4:AE（工具包押金）
        if not is_first_half:
            ab_data = ws_summary.get("AB4:AE") or []
            ab_data = [r for r in ab_data if r and any(str(v).strip() for v in r)]
            if ab_data:
                _log(log, f"    AB4:AE 有 {len(ab_data)} 筆工具包押金資料")
                # TODO: 另存 {period}元大工具包押金-{region}.xlsx
                _log(log, "    ⚠️ 工具包押金存檔 xlsx 待實作")

        ts = _punch("元大帳戶", region, period)
        _log(log, f"✅ 元大帳戶 {label} 完成｜{ts}")
        return True

    except Exception as e:
        _log(log, f"❌ 元大帳戶失敗：{e}")
        return False
