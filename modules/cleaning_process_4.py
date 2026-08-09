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
        清空 場次時數薪資總表 A121:E（startRow=121）
        清空 場次時數薪資總表 AB4:AE（col 28:31）
        清空 介紹獎金工作表 A2:C

    下半月：
        工具包押金：
            篩選「工具包押金」工作表：
                I 欄 >= 80 且 J 欄非空白
                → 場次時數薪資總表 A121:B（A=姓名, B=工具包押金 I欄場次）
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
from modules.period_utils import format_taipei_time, get_current_taipei_time


# ──────────────────────────────────────────────────────────────
# 常數
# ──────────────────────────────────────────────────────────────

TS_FMT = "%Y/%m/%d %H:%M"

DEPOSIT_THRESHOLD = 80    # I 欄 >= 80
DEPOSIT_TAICHUNG  = 1500
DEPOSIT_OTHER     = 2000
INTRO_BONUS       = 1000

TOOL_DEPOSIT_START_ROW = 121   # 場次時數薪資總表：工具包押金資料寫入起始列
AB_COL = 28                    # AB 欄（1-based）


# ──────────────────────────────────────────────────────────────
# 工具
# ──────────────────────────────────────────────────────────────

def _now_ts() -> str:
    return format_taipei_time(fmt=TS_FMT)


def _log(log: List[str], msg: str) -> None:
    log.append(msg)


def _punch(task_key: str, region: str, period: str) -> str:
    """打卡至主控試算表。"""
    ts = _now_ts()
    record_execution(region, period, task_key, None)
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
    today = get_current_taipei_time().date()
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
    region_cfg: dict = None,
    **kwargs,
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

        if is_first_half:
            ws_intro = ss.worksheet("介紹獎金")
            _tool_clear(ws_summary, ws_intro, log)
            dep_count = 0
        else:
            salary_id = str((region_cfg or {}).get("salary_id", "") or "").strip()
            if not salary_id:
                raise ValueError("地區設定缺少 salary_id")
            salary_ss = gc.open_by_key(salary_id)
            ws_deposit = salary_ss.worksheet("工具包押金")
            ws_counts = salary_ss.worksheet("場次和時數")
            ws_intro = ss.worksheet("介紹獎金")

            month = int(period[4:6])
            id_col = 5 + (month - 1) * 3  # 1月E、2月H、3月K...
            ws_counts.update_cell(1, id_col, cleaning_file_id)
            _log(log, f"  清潔承攬 ID 已寫入場次和時數 {_col_letter(id_col)}1")

            deposit_amount = DEPOSIT_TAICHUNG if "台中" in region else DEPOSIT_OTHER
            dep_count = _tool_process_v2(
                ws_deposit, ws_summary, ws_intro,
                deposit_amount, period, log
            )
            _log(log, f"  工具包押金：{dep_count} 筆")

        ts = _now_ts()
        record_execution(region, period, "工具包押金", dep_count)
        _log(log, f"✅ 工具包押金 {label} 完成｜{ts}")
        return True

    except Exception as e:
        detail = str(e).strip() or f"{type(e).__name__}: {e!r}"
        _log(log, f"❌ 工具包押金失敗：{detail}")
        return False


def _tool_process_v2(
    ws_deposit: gspread.Worksheet,
    ws_summary: gspread.Worksheet,
    ws_intro: gspread.Worksheet,
    deposit_amount: int,
    period: str,
    log: List[str],
) -> int:
    """
    依 salary_id 的「工具包押金」工作表處理下半月押金。

    規則：
    - 不動「場次時數薪資總表」第 1~120 列既有資料；工具包押金固定從第 121 列寫入。
    - 符合工具包押金資格者，自第 121 列起寫入：
        A 欄 = 姓名（薪資檔工具包押金 A 欄）
        B 欄 = 場次（薪資檔工具包押金 I 欄）
    - 不再把 G 欄提領日期寫到總表 B 欄。
    - G 欄提領日期仍依既有邏輯在薪資檔「工具包押金」內補寫，不影響上方總表資料。
    """
    rows = ws_deposit.get("A2:J") or []
    year, month = int(period[:4]), int(period[4:6])
    if month == 12:
        due = datetime.date(year + 1, 1, 10)
    else:
        due = datetime.date(year, month + 1, 10)
    due_text = due.strftime("%Y/%m/%d")

    # selected: (姓名, I欄場次)
    selected = []
    updates = []
    for offset, row in enumerate(rows, start=2):
        row += [""] * (10 - len(row))
        name = str(row[0]).strip()
        i_value = _to_num(row[8])
        current_due = str(row[6]).strip().replace("-", "/")
        try:
            current_due = datetime.datetime.strptime(
                current_due, "%Y/%m/%d"
            ).strftime("%Y/%m/%d")
        except ValueError:
            pass

        if (
            name
            and i_value >= DEPOSIT_THRESHOLD
            and (not current_due or current_due == due_text)
        ):
            # I 欄若為整數，寫入整數，避免 80.0 之類顯示。
            i_out = int(i_value) if float(i_value).is_integer() else i_value
            selected.append((name, i_out))
            if not current_due:
                updates.append({
                    "range": f"'{ws_deposit.title}'!G{offset}",
                    "values": [[due_text]],
                })

    if updates:
        ws_deposit.spreadsheet.values_batch_update({
            "valueInputOption": "USER_ENTERED", "data": updates
        })

    # 符合押金資格且 J 欄有介紹人：A=本人、B=介紹人、C=1000。
    intro_rows = []
    selected_names = {name for name, _i in selected}
    for row in rows:
        row += [""] * (10 - len(row))
        name = str(row[0]).strip()
        introducer = str(row[9]).strip()
        if name in selected_names and introducer:
            intro_rows.append([name, introducer, INTRO_BONUS])
    ws_intro.batch_clear(["A2:C"])
    if intro_rows:
        ws_intro.update(
            f"A2:C{len(intro_rows) + 1}",
            intro_rows,
            value_input_option="USER_ENTERED",
        )
    _log(log, f"  介紹獎金回填 {len(intro_rows)} 筆")

    # 工具包押金固定寫入「場次時數薪資總表」第 121 列起。
    # 嚴禁依姓名比對去清除、搬動或重排上方既有資料；A1:B120 完全不動。
    # 本次符合者依「工具包押金」工作表原列序寫入：A=姓名、B=I欄場次。
    append_start = 121

    if selected:
        end_row = append_start + len(selected) - 1
        ws_summary.update(
            f"A{append_start}:B{end_row}",
            [[name, i_value] for name, i_value in selected],
            value_input_option="USER_ENTERED",
        )
        _log(
            log,
            f"  工具包押金名單固定寫入 A{append_start}:B{end_row}："
            f"A=姓名、B=薪資檔 I 欄，共 {len(selected)} 筆；A1:B120 完全未變更",
        )
    else:
        _log(log, "  工具包押金無符合資料；A1:B120 完全不變更")

    # 元大工具包押金區：
    #   AD = 押金金額
    #   AE = 工具包押金姓名
    #   AB/AC = 以同列 AE 姓名去比對 H 欄姓名，取該列 I/J。
    # 例如 AE4=潘玟均，若 H73=潘玟均，則 AB4=I73、AC4=J73。
    # 這裡只處理 AB:AE，不改動 H:J 或其他既有資料。
    ws_summary.batch_clear(["AB4:AE120"])

    if selected:
        n = len(selected)
        end = 3 + n

        # 先固定寫入 AD/AE，之後 AB/AC 一律以 AE 的實際內容為比對依據。
        ws_summary.update(
            f"AD4:AE{end}",
            [[deposit_amount, name] for name, _i_value in selected],
            value_input_option="USER_ENTERED",
        )

        hij = ws_summary.get("H4:J120") or []
        account = {}
        for r in hij:
            if not r:
                continue
            h_name = str(r[0]).strip() if len(r) > 0 else ""
            if not h_name:
                continue
            i_val = r[1] if len(r) > 1 else ""
            j_val = r[2] if len(r) > 2 else ""
            account[h_name] = (i_val, j_val)

        ae_values = ws_summary.get(f"AE4:AE{end}") or []
        ab_ac = []
        missing = []
        for offset in range(n):
            ae_name = (
                str(ae_values[offset][0]).strip()
                if offset < len(ae_values) and ae_values[offset]
                else ""
            )
            i_val, j_val = account.get(ae_name, ("", ""))
            ab_ac.append([i_val, j_val])
            if ae_name and ae_name not in account:
                missing.append(ae_name)

        ws_summary.update(
            f"AB4:AC{end}", ab_ac, value_input_option="USER_ENTERED"
        )
        _log(log, f"  AB/AC 已依 AE 姓名比對 H 欄並帶入 I/J：{n} 筆")
        if missing:
            _log(log, "  ⚠️ H欄找不到姓名：" + "、".join(missing))

    _log(log, f"  工具包押金回填完成：{len(selected)} 筆，寫入起始列 A{append_start}")
    return len(selected)


def _tool_clear(
    ws_summary: gspread.Worksheet,
    ws_intro: gspread.Worksheet,
    log: List[str],
) -> None:
    """上半月：清空相關欄位。"""
    last_col = ws_summary.col_count
    last_ltr = _col_letter(last_col)

    # 清空 場次時數薪資總表 A121:E
    ws_summary.batch_clear([
        f"A{TOOL_DEPOSIT_START_ROW}:E",
        f"AB4:{_col_letter(AB_COL + 3)}",   # AB:AE = col28:31
    ])

    # 清空 介紹獎金 A2:C
    ws_intro.batch_clear(["A2:C"])

    _log(log, "    上半月：保留場次時數薪資總表 A:E；僅清空 AB4:AE 及介紹獎金 A2:C")


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
    工具包押金：I >= 80 且 J 非空白 → 場次時數薪資總表 A121起（A=J, B=A, C/D=空, E=押金）
    介紹獎金：  I >= 80 且 J 空白   → 介紹獎金工作表 A2起（A=J欄=空, B=A欄姓名, C=1000）

    注意：GAS 原版的 A=J欄, B=A欄 意思是：
        場次時數薪資總表 A欄 = 工具包押金 A欄（姓名）
        場次時數薪資總表 B欄 = 工具包押金 I欄（場次）
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
            dep_rows, value_input_option="USER_ENTERED"
        )
        _log(log, f"    工具包押金寫入 A{TOOL_DEPOSIT_START_ROW}:E{end_row}，共 {len(dep_rows)} 筆")

    # 寫入介紹獎金工作表 A2 起
    if intro_rows:
        ws_intro.batch_clear(["A2:C"])
        ws_intro.update(
            f"A2:C{1 + len(intro_rows)}",
            intro_rows, value_input_option="USER_ENTERED"
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
    region_cfg: dict = None,
    **kwargs,
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
        from modules.gas_pdf_client import run_yuanta as run_yuanta_gas
        gas_result = run_yuanta_gas(cleaning_file_id, region, period)
        if not gas_result.get("success"):
            raise RuntimeError(gas_result.get("message", "中控 GAS 元大帳戶執行失敗"))
        ts = _now_ts()
        record_execution(region, period, "元大帳戶", None)
        _log(log, f"✅ 中控 GAS 已完成元大承攬費／工具包押金 xlsx｜{ts}")
        return True

    except Exception as e:
        _log(log, f"❌ 元大帳戶失敗：{e}")
        return False
