"""
清潔承攬共用整理流程
對應 GAS runCommonProcess()

適用：01專員請款、02儲值獎金、03新人實境、04新人實習、05組長津貼

流程：QRS計算完後 →
1. 篩選 Q有值且R≠0 → 寫入 V(22)/W(23)/X(24)
2. U(21) = 統計 V 欄同名出現次數
3. 依 U→V→X 排序
4. Y(25) = 同名時合併所有 X 欄，用全形「，」分隔（只寫第一列）
           U=1 時 Y=X
5. AA(27) = V 欄去重（唯一姓名，依排序後順序）
6. AB(28) = SUMIF：AA=V 時加總 W
7. AC(29) = AC$1 & Y（Y 欄的值）
"""

from __future__ import annotations
from collections import OrderedDict
from typing import List
import gspread


def run_common_process(ws: gspread.Worksheet, log: List[str]) -> None:
    """
    執行共通 QRS→U-Y→AA-AC 流程。
    ws：已開啟的工作表物件（01~05 各自的工作表）
    """
    # ── 找 Q 欄最後有資料的列 ────────────────────────────────
    q_vals = ws.col_values(17)   # Q = col 17
    last_q = 0
    for i in range(len(q_vals) - 1, 0, -1):
        if str(q_vals[i]).strip():
            last_q = i + 1
            break
    if last_q < 2:
        _log(log, "    共通流程：Q 欄無資料，跳過")
        return

    # ── 讀取 Q/R/S ───────────────────────────────────────────
    qrs = ws.get(f"Q2:S{last_q}") or []
    rows = []
    for r in qrs:
        q  = str(r[0]).strip() if r else ""
        rv = r[1] if len(r) > 1 else ""
        s  = str(r[2]).strip() if len(r) > 2 else ""
        rows.append((q, rv, s))

    # ── 步驟1：篩選 Q有值且R≠0 ──────────────────────────────
    valid = []
    for q, r, s in rows:
        if not q:
            continue
        try:
            r_val = float(str(r).replace(",", "")) if r else 0
        except (ValueError, TypeError):
            r_val = 0
        if r_val != 0:
            valid.append((q, r_val, s))

    if not valid:
        _log(log, "    共通流程：R 欄全為 0，跳過")
        return

    # ── 步驟2：計算各姓名出現次數 ────────────────────────────
    name_count: dict = {}
    for q, r, s in valid:
        name_count[q] = name_count.get(q, 0) + 1

    # ── 步驟3：組合 U/V/W/X，依 U→V→X 排序 ──────────────────
    uvwx = [(name_count[q], q, r, s) for q, r, s in valid]
    uvwx.sort(key=lambda row: (row[0], row[1], row[3]))

    # ── 步驟4：計算 Y 欄（同名 X 欄合併，只寫第一列）─────────
    # 以排序後的順序，收集每個姓名的所有 X
    name_xs: OrderedDict = OrderedDict()
    for u, v, w, x in uvwx:
        if v not in name_xs:
            name_xs[v] = []
        if x:
            name_xs[v].append(x)

    # 建立 name → Y 值（全形「，」分隔）
    name_y: dict = {}
    for name, xs in name_xs.items():
        if len(xs) == 0:
            name_y[name] = ""
        elif len(xs) == 1:
            name_y[name] = xs[0]        # U=1 時 Y=X
        else:
            name_y[name] = "，".join(xs)  # U>1 時合併

    # ── 寫入 V/W/X/U/Y ───────────────────────────────────────
    n      = len(uvwx)
    end_vwx = 1 + n

    # 清空 U:AC
    ws.batch_clear([f"U2:AC{max(last_q, end_vwx)}"])

    # V/W/X
    vwx_data = [[v, w, x] for u, v, w, x in uvwx]
    ws.update(f"V2:X{end_vwx}", vwx_data, value_input_option="USER_ENTERED")

    # U
    u_data = [[u] for u, v, w, x in uvwx]
    ws.update(f"U2:U{end_vwx}", u_data, value_input_option="USER_ENTERED")

    # Y：只在每個姓名第一次出現的列寫入
    y_data   = [[""] for _ in range(n)]
    seen_y   = set()
    for i, (u, v, w, x) in enumerate(uvwx):
        if v not in seen_y:
            y_data[i] = [name_y[v]]
            seen_y.add(v)
    ws.update(f"Y2:Y{end_vwx}", y_data, value_input_option="USER_ENTERED")

    # ── 步驟5-7：AA/AB/AC ────────────────────────────────────
    unique_names = list(name_xs.keys())   # 依排序後唯一順序

    # AB：各姓名 W 加總
    name_w: dict = {}
    for u, v, w, x in uvwx:
        name_w[v] = name_w.get(v, 0) + (w if isinstance(w, (int, float)) else 0)

    # AC$1 固定前綴
    try:
        ac1 = str(ws.acell("AC1").value or "").strip()
    except Exception:
        ac1 = ""

    aa_data, ab_data, ac_data = [], [], []
    for name in unique_names:
        aa_data.append([name])
        ab_data.append([name_w.get(name, 0)])
        y_val = name_y.get(name, "")
        ac_data.append([ac1 + y_val if y_val else ""])

    aa_end = 1 + len(unique_names)
    ws.update(f"AA2:AA{aa_end}", aa_data, value_input_option="USER_ENTERED")
    ws.update(f"AB2:AB{aa_end}", ab_data, value_input_option="USER_ENTERED")
    ws.update(f"AC2:AC{aa_end}", ac_data, value_input_option="USER_ENTERED")

    _log(log, f"    共通流程完成：{n} 筆 → {len(unique_names)} 人（唯一）")


def _log(log: List[str], msg: str) -> None:
    log.append(msg)
