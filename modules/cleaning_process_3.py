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
import time
import unicodedata
from typing import List, Optional

import gspread

from modules.auth import get_gspread_client
from modules.master_sheet import record_execution
from modules.period_utils import format_taipei_time


TS_FMT = "%Y/%m/%d %H:%M"
SUMMARY_START = 4
SUMMARY_END = 120


def _name_key(value) -> str:
    """統一姓名字形並移除不可見空白。"""
    text = unicodedata.normalize("NFKC", str(value or ""))
    return "".join(
        ch for ch in text
        if not ch.isspace() and ch not in "\u200b\u200c\u200d\ufeff"
    )


def _now_ts() -> str:
    return format_taipei_time(fmt=TS_FMT)


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
        ws_proj_salary = ss.worksheet("專案薪資表")
        ws_summary = ss.worksheet("場次時數薪資總表")
        ws_pdf      = ss.worksheet("PDF產出")
        ws_proj_pdf = ss.worksheet("專案PDF產出")

        ws_summary.batch_clear(["AB4:AE120"])
        _log(log, "  已清空場次時數薪資總表 AB4:AE")

        # ── 步驟1：薪資表 L2048 → L2047 ──────────────────────
        _log(log, "  步驟1：薪資表 L2048 轉值複製至 L2047")
        _step1_copy_salary_row(ws_salary, log)

        # 專案人員另列於總表，D/E 改查專案薪資表。
        project_rows = _append_project_people_to_summary(
            ws_summary, ws_proj_salary, log
        )

        # ── 步驟2：場次時數薪資總表 ───────────────────────────
        _log(log, f"  步驟2：場次時數薪資總表（{label}）")
        _step2_summary(ws_summary, is_first_half, log)

        # ── 步驟3：PDF產出 ─────────────────────────────────────
        _log(log, f"  步驟3：PDF產出（{label}）")
        _step3_pdf_output(
            ws_summary, ws_pdf, ws_proj_pdf, is_first_half, project_rows, log
        )

        ts = _punch("結算作業", region, period)
        _log(log, f"✅ 結算作業 {label} 完成｜{ts}")
        return True

    except Exception as e:
        _log(log, f"❌ 結算作業失敗：{e}")
        return False


def _append_project_people_to_summary(
    ws_summary: gspread.Worksheet,
    ws_project_salary: gspread.Worksheet,
    log: List[str],
) -> list[int]:
    """把專案薪資表有金額的人員追加到總表 A 欄，並寫入專案 D/E 公式。"""
    last_col = _col_letter(ws_project_salary.col_count)
    headers = ws_project_salary.get(f"L1:{last_col}1") or [[]]
    row_2045 = ws_project_salary.get(
        f"L2045:{last_col}2045", value_render_option="UNFORMATTED_VALUE"
    ) or [[]]
    row_2046 = ws_project_salary.get(
        f"L2046:{last_col}2046", value_render_option="UNFORMATTED_VALUE"
    ) or [[]]

    names = []
    for idx, name in enumerate(headers[0]):
        name = str(name).strip()
        v1 = _to_num(row_2045[0][idx] if idx < len(row_2045[0]) else 0)
        v2 = _to_num(row_2046[0][idx] if idx < len(row_2046[0]) else 0)
        if name and (v1 != 0 or v2 != 0):
            names.append(name)

    if not names:
        _log(log, "    專案薪資表無非零人員")
        return []

    # 重跑結算時先移除上次追加的專案列，避免重複。
    existing_formulas = ws_summary.get(
        f"A{SUMMARY_START}:E{SUMMARY_END}", value_render_option="FORMULA"
    ) or []
    old_project_rows = []
    for offset, row in enumerate(existing_formulas):
        formulas = " ".join(str(cell) for cell in row[3:5])
        if "專案薪資表" in formulas:
            old_project_rows.append(SUMMARY_START + offset)
    if old_project_rows:
        ws_summary.batch_clear([f"A{row}:G{row}" for row in old_project_rows])

    existing = ws_summary.get(f"A{SUMMARY_START}:A{SUMMARY_END}") or []
    first_empty = SUMMARY_START
    for offset, row in enumerate(existing):
        if row and str(row[0]).strip():
            first_empty = SUMMARY_START + offset + 1
        else:
            break
    if first_empty + len(names) - 1 > SUMMARY_END:
        raise ValueError("場次時數薪資總表 A 欄空間不足，無法加入專案人員")

    project_rows = list(range(first_empty, first_empty + len(names)))
    data = []
    for row_num, name in zip(project_rows, names):
        data.extend([
            {"range": f"'{ws_summary.title}'!A{row_num}", "values": [[name]]},
            {"range": f"'{ws_summary.title}'!D{row_num}", "values": [[
                f"=IF(AND(E{row_num}=0,'專案薪資單'!$AD$1=$D$1),"
                f"HLOOKUP($A{row_num},'專案薪資表'!$1:$2046,2046,FALSE),"
                f"HLOOKUP($A{row_num},'專案薪資表'!$1:$2046,2045,FALSE))"
            ]]},
            {"range": f"'{ws_summary.title}'!E{row_num}", "values": [[
                f"=IF('專案薪資單'!$AD$1=$E$1,"
                f"HLOOKUP($A{row_num},'專案薪資表'!$1:$2046,2046,FALSE),0)"
            ]]},
        ])
    ws_summary.spreadsheet.values_batch_update({
        "valueInputOption": "USER_ENTERED",
        "data": data,
    })
    time.sleep(2)
    _log(log, f"    總表追加專案人員：{len(names)} 人")
    return project_rows


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
        N~O：P欄姓名清單對應 H欄，取 I:J 填入 N:O
    下半月：
        W~X：篩選 E4:E>0，A欄姓名→X欄，E欄數值→W欄
        U~V：X欄姓名清單對應 H欄，取 I:J 填入 U:V

    試算表欄號（1-based）：
        N=14, O=15, P=16, Q=17, U=21, V=22, W=23, X=24
    A4起 raw 0-based index：A=0,D=3,E=4,H=7,I=8,J=9
    """
    raw = ws.get("A4:J", value_render_option="UNFORMATTED_VALUE") or []

    if is_first_half:
        pq_rows = []
        for row in raw:
            while len(row) < 10:
                row.append("")
            a = str(row[0]).strip()
            d = _to_num(row[3])
            if a and d > 0:
                pq_rows.append((a, d))

        if pq_rows:
            n = len(pq_rows)
            ws.batch_clear([f"N4:Q{3 + len(raw)}"])
            ws.update(f"P4:P{3+n}", [[r[0]] for r in pq_rows], value_input_option="USER_ENTERED")
            ws.update(f"Q4:Q{3+n}", [[r[1]] for r in pq_rows], value_input_option="USER_ENTERED")
            _log(log, f"    P~Q 寫入：{n} 筆")
            _fill_account_cols(ws, raw, [r[0] for r in pq_rows],
                               h_idx=7, i_idx=8, j_idx=9,
                               tgt1=14, tgt2=15, label="N~O", log=log)
        else:
            _log(log, "    D>0 無資料，跳過 P~Q 及 N~O")

    else:
        wx_rows = []
        for row in raw:
            while len(row) < 10:
                row.append("")
            a = str(row[0]).strip()
            e = _to_num(row[4])
            if a and e > 0:
                wx_rows.append((a, e))

        if wx_rows:
            n = len(wx_rows)
            ws.batch_clear([f"U4:X{3 + len(raw)}"])
            ws.update(f"X4:X{3+n}", [[r[0]] for r in wx_rows], value_input_option="USER_ENTERED")
            ws.update(f"W4:W{3+n}", [[r[1]] for r in wx_rows], value_input_option="USER_ENTERED")
            _log(log, f"    W~X 寫入：{n} 筆")
            _fill_account_cols(ws, raw, [r[0] for r in wx_rows],
                               h_idx=7, i_idx=8, j_idx=9,
                               tgt1=21, tgt2=22, label="U~V", log=log)
        else:
            _log(log, "    E>0 無資料，跳過 W~X 及 U~V")


def _fill_account_cols(
    ws: gspread.Worksheet,
    raw: list,
    names: List[str],
    h_idx: int,
    i_idx: int,
    j_idx: int,
    tgt1: int,
    tgt2: int,
    label: str,
    log: List[str],
) -> None:
    """
    用 names 清單對應 H 欄（raw 中 h_idx）找同名列，
    取 I:J 填入 tgt1:tgt2 欄（1-based 欄號）。
    """
    if not names:
        return

    h_map: dict = {}
    for row in raw:
        while len(row) < 10:
            row.append("")
        h_val = _name_key(row[h_idx])
        if h_val:
            h_map[h_val] = (row[i_idx], row[j_idx])

    col1_data, col2_data = [], []
    for name in names:
        ij = h_map.get(_name_key(name), ("", ""))
        col1_data.append([ij[0]])
        col2_data.append([ij[1]])

    c1  = _col_letter(tgt1)
    c2  = _col_letter(tgt2)
    end = 3 + len(names)
    ws.update(f"{c1}4:{c1}{end}", col1_data, value_input_option="USER_ENTERED")
    ws.update(f"{c2}4:{c2}{end}", col2_data, value_input_option="USER_ENTERED")
    _log(log, f"    {label} 寫入：{len(names)} 筆")

# ──────────────────────────────────────────────────────────────
# 步驟3：PDF產出
# ──────────────────────────────────────────────────────────────

def _step3_pdf_output(
    ws_summary: gspread.Worksheet,
    ws_pdf: gspread.Worksheet,
    ws_proj_pdf: gspread.Worksheet,
    is_first_half: bool,
    project_rows: list[int],
    log: List[str],
) -> None:
    """
    貼入前先清空 PDF產出 和 專案PDF產出 的 B2:H。
    上半月：Q4:Q → PDF產出 B2:B，B非空的 H欄=Y
    下半月：X4:X → PDF產出 B2:B，B非空的 H欄=Y
    """
    # 先清空兩個工作表的 B2:H
    ws_pdf.batch_clear(["B2:H"])
    ws_proj_pdf.batch_clear(["B2:H"])
    _log(log, "    PDF產出 & 專案PDF產出 B2:H 已清空")

    src_col = "Q" if is_first_half else "X"

    src_vals = ws_summary.get(
        f"{src_col}4:{src_col}",
        value_render_option="UNFORMATTED_VALUE"
    ) or []
    names = [str(r[0]).strip() for r in src_vals if r and str(r[0]).strip()]

    value_col = "D" if is_first_half else "E"
    project_names = []
    for row_num in project_rows:
        values = ws_summary.get(
            f"A{row_num}:{value_col}{row_num}",
            value_render_option="UNFORMATTED_VALUE",
        ) or [[]]
        row = values[0] if values else []
        name = str(row[0]).strip() if row else ""
        value_idx = 3 if is_first_half else 4
        amount = _to_num(row[value_idx] if len(row) > value_idx else 0)
        if name and amount > 0:
            project_names.append(name)

    # 合併清單中每個專案列移除一次；同人兼具一般與專案時仍會各產一份。
    normal_names = list(names)
    for name in project_names:
        if name in normal_names:
            normal_names.remove(name)

    if not names and not project_names:
        _log(log, f"    {src_col}4 無資料，跳過 PDF產出寫入")
        return

    if normal_names:
        n = len(normal_names)
        ws_pdf.update(
            f"B2:B{1+n}", [[name] for name in normal_names],
            value_input_option="USER_ENTERED",
        )
        ws_pdf.update(f"H2:H{1+n}", [["Y"]] * n, value_input_option="USER_ENTERED")
    if project_names:
        n = len(project_names)
        ws_proj_pdf.update(
            f"B2:B{1+n}", [[name] for name in project_names],
            value_input_option="USER_ENTERED",
        )
        ws_proj_pdf.update(f"H2:H{1+n}", [["Y"]] * n, value_input_option="USER_ENTERED")
    _log(
        log,
        f"    PDF名單：清潔 {len(normal_names)} 人，專案 {len(project_names)} 人",
    )
