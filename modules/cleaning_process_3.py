"""
Lemon Clean 清潔承攬 — 06季獎金 / 結算作業
檔案：modules/cleaning_process_3.py

打卡：統一寫入主控試算表（record_execution），不寫入 exec 工作表。
    06季獎金  → task_key = "06季獎金"（待實作）
    結算作業  → task_key = "結算作業"

結算作業步驟：
    步驟1：薪資表 L2048 轉值後貼入 L2047（去除空格）
    步驟2：場次時數薪資總表
        上半月：P~Q 欄（D>0）、N~O 欄（帳戶）
        下半月：W~X 欄（E>0）、U~V 欄（帳戶）
    步驟3：
        上半月：場次時數薪資總表 Q4:Q → PDF產出 B2:B，B欄非空的 H欄=Y
        下半月：場次時數薪資總表 X4:X → PDF產出 B2:B，B欄非空的 H欄=Y
"""

from __future__ import annotations

import datetime
from typing import List, Optional

import gspread

from modules.auth import get_gspread_client
from modules.master_sheet import record_execution


TS_FMT = "%Y/%m/%d %H:%M"


def _now_ts() -> str:
    return datetime.datetime.now().strftime(TS_FMT)


def _log(log: List[str], msg: str) -> None:
    log.append(msg)


def _punch(task_key: str, region: str, period: str) -> str:
    ts = _now_ts()
    record_execution(region, period, task_key, ts)
    return ts


def _col_letter(n: int) -> str:
    result = ""
    while n > 0:
        n, r = divmod(n - 1, 26)
        result = chr(65 + r) + result
    return result


def _to_num(val) -> float:
    try:
        return float(str(val).replace(",", "").strip())
    except (ValueError, TypeError):
        return 0.0


# ──────────────────────────────────────────────────────────────
# 06 季獎金（待實作）
# ──────────────────────────────────────────────────────────────

def run_season_bonus(
    cleaning_file_id: str,
    region: str,
    period: str,
    is_first_half: bool,
    log: List[str],
    region_cfg: dict = None,
    **kwargs,
) -> bool:
    """06 季獎金。GAS 原版尚未實作，此處保留框架。"""
    _log(log, "▶ 06季獎金：尚未實作")
    return False


# ──────────────────────────────────────────────────────────────
# 結算作業
# ──────────────────────────────────────────────────────────────

def run_settlement(
    cleaning_file_id: str,
    region: str,
    period: str,
    is_first_half: bool,
    log: List[str],
    region_cfg: dict = None,
    **kwargs,
) -> bool:
    """
    結算作業。

    步驟1：薪資表 L2048 轉值後複製到 L2047（去除空格）
    步驟2：場次時數薪資總表
        上半月：P~Q（A姓名/D數值，D>0）、N~O（P欄姓名對應H欄找I:J）
        下半月：W~X（A姓名/E數值，E>0）、U~V（X欄姓名對應H欄找I:J）
    步驟3：
        上半月：Q4:Q → PDF產出 B2:B，B非空的 H欄=Y
        下半月：X4:X → PDF產出 B2:B，B非空的 H欄=Y
    """
    label = "上半月" if is_first_half else "下半月"
    _log(log, f"▶ 結算作業 {label} 開始")
    try:
        gc = get_gspread_client()
        ss = gc.open_by_key(cleaning_file_id)

        ws_salary  = ss.worksheet("薪資表")
        ws_summary = ss.worksheet("場次時數薪資總表")
        ws_pdf     = ss.worksheet("PDF產出")

        # ── 步驟1：薪資表 L2048 → L2047 ──────────────────────
        _log(log, "  步驟1：薪資表 L2048 轉值複製至 L2047")
        _step1_copy_salary_row(ws_salary, log)

        # ── 步驟2：場次時數薪資總表 ───────────────────────────
        _log(log, f"  步驟2：場次時數薪資總表（{label}）")
        _step2_summary(ws_summary, is_first_half, log)

        # ── 步驟3：PDF產出 ─────────────────────────────────────
        _log(log, f"  步驟3：PDF產出（{label}）")
        _step3_pdf_output(ws_summary, ws_pdf, is_first_half, log)

        ts = _punch("結算作業", region, period)
        _log(log, f"✅ 結算作業 {label} 完成｜{ts}")
        return True

    except Exception as e:
        _log(log, f"❌ 結算作業失敗：{e}")
        return False


# ──────────────────────────────────────────────────────────────
# 步驟1：薪資表 L2048 轉值複製至 L2047（去除空格）
# ──────────────────────────────────────────────────────────────

def _step1_copy_salary_row(
    ws_salary: gspread.Worksheet,
    log: List[str],
) -> None:
    """
    讀取薪資表 L2048（公式列）轉為靜態值，
    去除各儲存格中的空格後貼入 L2047。
    """
    last_col    = ws_salary.col_count
    last_letter = _col_letter(last_col)

    # 讀取 L2048（用 UNFORMATTED_VALUE 取原始值，公式已計算完）
    raw = ws_salary.get(
        f"L2048:{last_letter}2048",
        value_render_option="UNFORMATTED_VALUE"
    ) or [[]]
    if not raw or not raw[0]:
        _log(log, "    L2048 無資料，跳過")
        return

    row2048 = raw[0]

    # 去除各儲存格的空格
    cleaned = []
    for cell in row2048:
        s = str(cell) if cell is not None and cell != "" else ""
        # 去除多餘空格（保留換行間隔，僅去頭尾及多餘空格）
        s = " ".join(s.split())
        cleaned.append(s)

    # 貼入 L2047（USER_ENTERED 讓 Sheets 判斷型別，不加 apostrophe）
    end_col = _col_letter(11 + len(cleaned))  # L=col12，偏移11
    ws_salary.update(
        f"L2047:{end_col}2047",
        [cleaned],
        value_input_option="USER_ENTERED"
    )
    _log(log, f"    L2048→L2047 完成（{len(cleaned)} 欄）")


# ──────────────────────────────────────────────────────────────
# 步驟2：場次時數薪資總表
# ──────────────────────────────────────────────────────────────

def _step2_summary(
    ws: gspread.Worksheet,
    is_first_half: bool,
    log: List[str],
) -> None:
    """
    上半月：
        P~Q：篩選 D4:D>0，A欄姓名→P欄，D欄數值→Q欄
        N~O：P欄姓名對應 H欄姓名，找 I:J 欄填入 N:O
    下半月：
        W~X：篩選 E4:E>0，A欄姓名→X欄，E欄數值→W欄
        U~V：X欄姓名對應 H欄姓名，找 I:J 欄填入 U:V
    """
    # 讀取 A:J 欄（A=1,D=4,E=5,H=8,I=9,J=10）
    last_row = ws.row_count
    raw = ws.get("A4:J", value_render_option="UNFORMATTED_VALUE") or []

    if is_first_half:
        # P~Q：D>0
        pq_rows = []
        for i, row in enumerate(raw):
            while len(row) < 10:
                row.append("")
            a = str(row[0]).strip()  # A欄姓名
            d = _to_num(row[3])      # D欄數值
            if a and d > 0:
                pq_rows.append((i, a, d))

        if pq_rows:
            # 清空 P4:Q（先清空再寫入）
            ws.batch_clear([f"P4:Q{3 + len(raw)}"])
            p_data = [[r[1]] for r in pq_rows]  # 姓名
            q_data = [[r[2]] for r in pq_rows]  # 數值
            ws.update(f"P4:P{3 + len(pq_rows)}", p_data, value_input_option="USER_ENTERED")
            ws.update(f"Q4:Q{3 + len(pq_rows)}", q_data, value_input_option="USER_ENTERED")
            _log(log, f"    上半月 P~Q 寫入：{len(pq_rows)} 筆")

        # N~O：P欄姓名對應 H欄，找 I:J
        _fill_account_cols(ws, raw, name_src_col=15, h_col=7, i_col=8, j_col=9,
                           tgt_col1=13, tgt_col2=14,
                           data_count=len(pq_rows), label="N~O", log=log)

    else:
        # W~X：E>0
        wx_rows = []
        for i, row in enumerate(raw):
            while len(row) < 10:
                row.append("")
            a = str(row[0]).strip()  # A欄姓名
            e = _to_num(row[4])      # E欄數值
            if a and e > 0:
                wx_rows.append((i, a, e))

        if wx_rows:
            ws.batch_clear([f"W4:X{3 + len(raw)}"])
            x_data = [[r[1]] for r in wx_rows]  # 姓名→X欄
            w_data = [[r[2]] for r in wx_rows]  # 數值→W欄
            ws.update(f"X4:X{3 + len(wx_rows)}", x_data, value_input_option="USER_ENTERED")
            ws.update(f"W4:W{3 + len(wx_rows)}", w_data, value_input_option="USER_ENTERED")
            _log(log, f"    下半月 W~X 寫入：{len(wx_rows)} 筆")

        # U~V：X欄姓名對應 H欄，找 I:J
        _fill_account_cols(ws, raw, name_src_col=23, h_col=7, i_col=8, j_col=9,
                           tgt_col1=20, tgt_col2=21,
                           data_count=len(wx_rows), label="U~V", log=log)


def _fill_account_cols(
    ws: gspread.Worksheet,
    raw: list,
    name_src_col: int,   # 姓名來源欄（0-based，對應試算表欄號-1，A4起）
    h_col: int,          # H 欄 0-based index
    i_col: int,          # I 欄 0-based index
    j_col: int,          # J 欄 0-based index
    tgt_col1: int,       # 目標欄1（N 或 U）試算表欄號 1-based
    tgt_col2: int,       # 目標欄2（O 或 V）試算表欄號 1-based
    data_count: int,
    label: str,
    log: List[str],
) -> None:
    """
    讀取 name_src 欄的姓名，對應 H 欄找到同名列，
    取 I:J 欄的值填入 tgt_col1:tgt_col2。
    """
    if data_count == 0:
        return

    # 讀取 name_src 欄（P 或 X）的姓名
    src_letter = _col_letter(name_src_col)
    src_vals   = ws.get(f"{src_letter}4:{src_letter}{3 + data_count}",
                        value_render_option="UNFORMATTED_VALUE") or []
    names      = [r[0] if r else "" for r in src_vals]

    # 建立 H欄→(I,J) 的對照
    h_to_ij: dict = {}
    for row in raw:
        while len(row) < 10:
            row.append("")
        h_val = str(row[h_col]).strip()
        if h_val:
            h_to_ij[h_val] = (row[i_col], row[j_col])

    col1_data, col2_data = [], []
    for name in names:
        ij = h_to_ij.get(str(name).strip(), ("", ""))
        col1_data.append([ij[0]])
        col2_data.append([ij[1]])

    tgt1 = _col_letter(tgt_col1)
    tgt2 = _col_letter(tgt_col2)
    end  = 3 + data_count
    ws.update(f"{tgt1}4:{tgt1}{end}", col1_data, value_input_option="USER_ENTERED")
    ws.update(f"{tgt2}4:{tgt2}{end}", col2_data, value_input_option="USER_ENTERED")
    _log(log, f"    {label} 帳戶欄寫入：{data_count} 筆")


# ──────────────────────────────────────────────────────────────
# 步驟3：PDF產出
# ──────────────────────────────────────────────────────────────

def _step3_pdf_output(
    ws_summary: gspread.Worksheet,
    ws_pdf: gspread.Worksheet,
    is_first_half: bool,
    log: List[str],
) -> None:
    """
    上半月：場次時數薪資總表 Q4:Q → PDF產出 B2:B，B非空的 H欄=Y
    下半月：場次時數薪資總表 X4:X → PDF產出 B2:B，B非空的 H欄=Y
    """
    src_col = "Q" if is_first_half else "X"

    # 讀取 Q4:Q 或 X4:X（姓名）
    src_vals = ws_summary.get(
        f"{src_col}4:{src_col}",
        value_render_option="UNFORMATTED_VALUE"
    ) or []
    names = [r[0] for r in src_vals if r and str(r[0]).strip()]

    if not names:
        _log(log, f"    {src_col}4:無資料，跳過 PDF產出")
        return

    # 寫入 PDF產出 B2:B（B=col2）
    b_data = [[name] for name in names]
    ws_pdf.update(
        f"B2:B{1 + len(names)}",
        b_data,
        value_input_option="USER_ENTERED"
    )
    _log(log, f"    PDF產出 B2 寫入 {len(names)} 人")

    # B欄非空的 H欄=Y（H=col8）
    h_data = [["Y"] for _ in names]
    ws_pdf.update(
        f"H2:H{1 + len(names)}",
        h_data,
        value_input_option="USER_ENTERED"
    )
    _log(log, f"    PDF產出 H欄=Y 寫入完成")
