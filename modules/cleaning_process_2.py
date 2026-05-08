"""
Lemon Clean 清潔承攬 — 01專員請款 / 02儲值獎金 / 03新人實境 / 04新人實習 / 05組長津貼
檔案：modules/cleaning_process_2.py

依賴：
    modules/auth.py         — get_gspread_client()
    modules/master_sheet.py — record_execution()

清潔承攬試算表（exec 工作表）關鍵儲存格：
    B1 → 期別 YYYYMM（如 202605）
    C2 → 請款試算表 ID（01專員請款）
    C3 → 薪資試算表 ID（03新人實境 / 04新人實習 / 05組長津貼）
    C5 → 金流對帳試算表 ID（02儲值獎金）

scheduleName = B1（YYYYMM）+ "-1"（上半月）/ "-2"（下半月）

共同流程（01~05 皆相同）：
    上半月：清空工作表 A2:AC
    下半月：找 A 欄第一個空白列，填入 IMPORTRANGE 公式
    等待載入 → 轉為靜態值
    計算 QRS
    執行共通 QRS→U-Y→AA-AC 流程

各作業差異（A欄公式 / QRS）：

    01專員請款
        來源 ID : exec C2
        公式    : FILTER(IMPORTRANGE(C2,"專員請款!AJ2:AQ3000"), AJ=scheduleName)
        QRS     : Q=B, R=F, S=H

    02儲值獎金
        來源 ID : exec C5
        公式    : FILTER(IMPORTRANGE(C5,"範本!A2:BB3000"), A="儲值金")
                  匯入欄位 B,C,D,E,M,BB（遮罩）
        QRS 前先處理：
            X 數量（F欄中X個數）→ 複製列數（最多3列）
            G = X數量+1（最小2）
            H = D含50,000→800；D含20,000→320
            I = F欄拆解後各人姓名（每列一人）
        QRS : Q=I, R=H/G（G<2以2計，四捨五入）, S=TEXT(C,"MM/DD")&E

    03新人實境
        來源 ID : exec C3
        公式    : FILTER(IMPORTRANGE(C3,"新人實境!A2:L500"), A=scheduleName)
        QRS     : Q=C, R=200*K, S=TEXT(E,"MM/DD")&G

    04新人實習
        來源 ID : exec C3
        公式    : FILTER(IMPORTRANGE(C3,"新人實習!A2:L500"), A=scheduleName)
        QRS     : Q=C, R=200*K, S=TEXT(E,"MM/DD")&G

    05組長津貼
        來源 ID : exec C3
        公式    : FILTER(IMPORTRANGE(C3,"新人實習!A2:L500"), A=scheduleName)
                  ※ 來源工作表名稱同 04
        QRS     : Q=H, R=J*K, S=TEXT(E,"MM/DD")&G

打卡（exec 工作表列 + 主控試算表）：
    01專員請款      exec 列13
    02儲值獎金      exec 列14
    03新人實境      exec 列15
    04新人實習      exec 列16
    05組長津貼      exec 列17
    新人實境實習期別 exec 列23
"""

from __future__ import annotations

import datetime
import re
import time
from typing import Dict, List

import gspread

from modules.auth import get_gspread_client
from modules.master_sheet import record_execution


# ──────────────────────────────────────────────────────────────
# 常數
# ──────────────────────────────────────────────────────────────

TS_FMT = "%Y/%m/%d %H:%M"



# ──────────────────────────────────────────────────────────────
# 共用工具
# ──────────────────────────────────────────────────────────────

def _now_ts() -> str:
    return datetime.datetime.now().strftime(TS_FMT)


def _log(log: List[str], msg: str) -> None:
    log.append(msg)


def _col_letter(n: int) -> str:
    """欄號（1-based）→ 欄字母，如 12 → 'L'。"""
    result = ""
    while n > 0:
        n, r = divmod(n - 1, 26)
        result = chr(65 + r) + result
    return result


def _to_num(val) -> float:
    """安全轉數字，失敗回傳 0。"""
    try:
        return float(str(val).replace(",", "").strip())
    except (ValueError, TypeError):
        return 0.0


def _format_date_mmdd(val) -> str:
    """日期值 → 'MM/DD' 字串，無法解析回傳原字串。"""
    if isinstance(val, (int, float)) and 30000 < val < 60000:
        d = datetime.date(1899, 12, 30) + datetime.timedelta(days=int(val))
        return d.strftime("%m/%d")
    s = str(val).strip()
    for fmt in ("%Y/%m/%d", "%Y-%m-%d"):
        try:
            return datetime.datetime.strptime(s, fmt).strftime("%m/%d")
        except ValueError:
            pass
    return s


def _first_empty_row_col_a(ws: gspread.Worksheet) -> int:
    """找 A 欄第一個空白列（下半月接續用），最少從第 2 列起。"""
    vals = ws.col_values(1)
    for i in range(1, len(vals)):   # index 0 = 第1列（標題）
        if not str(vals[i]).strip():
            return i + 1
    return len(vals) + 1


def _punch(
    task_key: str,
    region: str,
    period: str,
) -> str:
    """打卡至主控試算表。"""
    ts = _now_ts()
    record_execution(region, period, task_key, ts)
    return ts


# ──────────────────────────────────────────────────────────────
# 共同步驟
# ──────────────────────────────────────────────────────────────

def _clear_first_half(ws: gspread.Worksheet, log: List[str]) -> None:
    """上半月：清空 A2:AC。"""
    ws.batch_clear(["A2:AC"])
    _log(log, "    上半月：已清空 A2:AC")


def _wait_and_convert(
    ws: gspread.Worksheet,
    check_cell: str,
    log: List[str],
    extra_wait: int = 3,
) -> int:
    """
    等待 IMPORTRANGE 載入，轉為靜態值，回傳有效列數（A欄非空，不含標題）。
    """
    deadline = time.time() + 30
    while time.time() < deadline:
        v = ws.acell(check_cell).value
        if v and str(v).strip():
            break
        time.sleep(2)
    else:
        _log(log, f"    ⚠️ 等待逾時：{check_cell} 仍為空，嘗試繼續")

    time.sleep(extra_wait)

    a_vals  = ws.col_values(1)
    last    = 0
    for i in range(1, len(a_vals)):
        if str(a_vals[i]).strip():
            last = i + 1
    if last < 2:
        return 0

    data = ws.get(f"A2:AC{last}") or []
    if data:
        ws.update(f"A2:AC{last}", data, value_input_option="USER_ENTERED")
    _log(log, f"    A2:AC{last} 轉靜態值完成（{last - 1} 列）")
    return last - 1


def _run_common_process(ws: gspread.Worksheet, log: List[str]) -> None:
    """
    共通 QRS→U-Y→AA-AC 流程。

    欄位：
      Q(17)=姓名, R(18)=金額, S(19)=備註

    步驟：
    1. 篩選 R≠0 的列 → 寫入 V(22)/W(23)/X(24)
    2. U(21) = V 欄同名出現次數
    3. Y(25) = 當 U>1 時，合併同名的所有 X 欄，用全形「，」分隔
    4. AA(27) = V 欄去重（唯一姓名）
    5. AB(28) = SUMIF：AA=V 時加總 W
    6. AC(29) = AC$1 & Y（Y 欄合併後的備註）
    """
    # 找 Q 欄最後有資料的列
    q_vals = ws.col_values(17)
    last_q = 0
    for i in range(len(q_vals) - 1, 0, -1):
        if str(q_vals[i]).strip():
            last_q = i + 1
            break
    if last_q < 2:
        _log(log, "    共通流程：Q 欄無資料，跳過")
        return

    qrs = ws.get(f"Q2:S{last_q}") or []
    rows = []
    for r in qrs:
        q = r[0] if r else ""
        rv = r[1] if len(r) > 1 else ""
        s  = r[2] if len(r) > 2 else ""
        rows.append((str(q).strip(), rv, str(s).strip()))

    # 篩選 R≠0
    vwx = [(q, r, s) for q, r, s in rows if r and str(r).strip() not in ("", "0")]
    if not vwx:
        _log(log, "    共通流程：R 欄全為 0，跳過")
        return

    n = len(vwx)
    end_row = 1 + n  # 資料從第2列開始

    # 清空 U:AC
    ws.batch_clear([f"U2:AC{max(last_q, end_row)}"])

    # 寫入 V/W/X
    vwx_data = [[q, r, s] for q, r, s in vwx]
    ws.update(f"V2:X{end_row}", vwx_data, value_input_option="USER_ENTERED")

    # U 欄：統計 V 欄同名次數
    v_col  = [t[0] for t in vwx]
    u_data = [[v_col.count(name)] for name in v_col]
    ws.update(f"U2:U{end_row}", u_data, value_input_option="USER_ENTERED")

    # Y 欄：同名時合併所有 X 欄，用全形「，」分隔
    # 先建立 name→[x列表] 的對照
    from collections import defaultdict, OrderedDict
    name_xs: dict = defaultdict(list)
    name_first_row: dict = OrderedDict()  # 記錄每個姓名第一次出現的列索引
    for i, (q, r, s) in enumerate(vwx):
        name_xs[q].append(s)
        if q not in name_first_row:
            name_first_row[q] = i

    y_data = [[""] for _ in range(n)]
    for name, first_i in name_first_row.items():
        xs = name_xs[name]
        if len(xs) > 1:
            # 只在第一列寫合併結果，其餘空白
            y_data[first_i] = ["，".join(x for x in xs if x)]

    ws.update(f"Y2:Y{end_row}", y_data, value_input_option="USER_ENTERED")

    # AA:AC — 去重後彙總
    # AA = 唯一姓名（依出現順序）
    unique_names = list(name_first_row.keys())
    aa_data  = [[name] for name in unique_names]
    ab_data  = []  # W 欄加總
    ac_data  = []  # AC$1 & Y

    # 讀取 AC1（固定前綴）
    ac1 = str(ws.acell("AC1").value or "").strip()

    # 建立 name→W合計 和 name→Y 的對照
    name_w: dict = defaultdict(float)
    for q, r, s in vwx:
        try:
            name_w[q] += float(str(r).replace(",", ""))
        except (ValueError, TypeError):
            pass
    name_y: dict = {name: y_data[first_i][0] for name, first_i in name_first_row.items()}

    for name in unique_names:
        ab_data.append([name_w[name]])
        y_val = name_y.get(name, "")
        # 若只有一筆，Y 欄是空的，直接用 X 欄
        if not y_val:
            y_val = name_xs[name][0] if name_xs[name] else ""
        ac_data.append([ac1 + y_val if y_val else ""])

    aa_end = 1 + len(unique_names)
    ws.update(f"AA2:AA{aa_end}", aa_data, value_input_option="USER_ENTERED")
    ws.update(f"AB2:AB{aa_end}", ab_data, value_input_option="USER_ENTERED")
    ws.update(f"AC2:AC{aa_end}", ac_data, value_input_option="USER_ENTERED")

    _log(log, f"    共通流程完成：V/W/X {n} 筆，AA/AB/AC {len(unique_names)} 筆（唯一姓名）")


# ──────────────────────────────────────────────────────────────
# QRS 計算
# ──────────────────────────────────────────────────────────────

def _calc_qrs_direct(
    ws: gspread.Worksheet,
    start_row: int,
    num_rows: int,
    q_col: int,
    r_col: int,
    s_col: int,
    log: List[str],
) -> None:
    """直接對應欄號複製（用於 01專員請款：Q=B, R=F, S=H）。"""
    end_row = start_row + num_rows - 1

    def _get(col: int) -> List[List]:
        raw = ws.get(f"{_col_letter(col)}{start_row}:{_col_letter(col)}{end_row}") or []
        padded = raw + [[""]] * (num_rows - len(raw))
        return padded

    ws.update(f"Q{start_row}:Q{end_row}", _get(q_col), value_input_option="USER_ENTERED")
    ws.update(f"R{start_row}:R{end_row}", _get(r_col), value_input_option="USER_ENTERED")
    ws.update(f"S{start_row}:S{end_row}", _get(s_col), value_input_option="USER_ENTERED")
    _log(log, f"    QRS 寫入完成（{num_rows} 列）")


def _calc_qrs_salary(
    ws: gspread.Worksheet,
    start_row: int,
    num_rows: int,
    q_idx: int,
    r_expr: str,
    log: List[str],
) -> None:
    """
    03/04/05 的 QRS 計算。
    q_idx  : Q 來源欄索引（0-based，對應 A:L 讀取後的位置）
    r_expr : "200*K"（R = 200 × K）或 "J*K"（R = J × K）
    S 欄   : TEXT(E,"MM/DD") & G（固定）
    A:L 欄索引（0-based）: A=0,B=1,C=2,D=3,E=4,F=5,G=6,H=7,I=8,J=9,K=10,L=11
    """
    end_row = start_row + num_rows - 1
    raw     = ws.get(f"A{start_row}:L{end_row}") or []

    q_data, r_data, s_data = [], [], []
    for row in raw:
        while len(row) < 12:
            row.append("")

        q_data.append([row[q_idx]])

        if r_expr == "200*K":
            r_data.append([200 * _to_num(row[10]) or ""])
        elif r_expr == "J*K":
            j, k = _to_num(row[9]), _to_num(row[10])
            r_data.append([j * k if (j and k) else ""])
        else:
            r_data.append([""])

        s_data.append([_format_date_mmdd(row[4]) + str(row[6])])

    ws.update(f"Q{start_row}:Q{end_row}", q_data, value_input_option="USER_ENTERED")
    ws.update(f"R{start_row}:R{end_row}", r_data, value_input_option="USER_ENTERED")
    ws.update(f"S{start_row}:S{end_row}", s_data, value_input_option="USER_ENTERED")
    _log(log, f"    QRS 寫入完成（{num_rows} 列）")


# ──────────────────────────────────────────────────────────────
# 01 專員請款
# ──────────────────────────────────────────────────────────────

def run_allowance(
    cleaning_file_id: str,
    region: str,
    period: str,
    is_first_half: bool,
    log: List[str],
    region_cfg: dict = None,
) -> bool:
    label = "上半月" if is_first_half else "下半月"
    _log(log, f"▶ 01專員請款 {label} 開始")
    try:
        gc = get_gspread_client()
        ss = gc.open_by_key(cleaning_file_id)
        ws_allowance = ss.worksheet("01專員請款")

        yyyymm       = period[:6]
        cfg          = region_cfg or {}
        allowance_id = str(cfg.get("allowance_id", "") or "").strip()
        schedule     = f"{yyyymm}-{'1' if is_first_half else '2'}"
        if not allowance_id:
            raise ValueError("config 地區設定缺少 allowance_id（請款 ID）")

        if is_first_half:
            _clear_first_half(ws_allowance, log)
            target_row = 2
        else:
            target_row = _first_empty_row_col_a(ws_allowance)
            _log(log, f"    下半月接續列：{target_row}")

        formula = (
            f'=FILTER('
            f'IMPORTRANGE("{allowance_id}","專員請款!AJ2:AQ3000"),'
            f'IMPORTRANGE("{allowance_id}","專員請款!AJ2:AJ3000")="{schedule}"'
            f')'
        )
        ws_allowance.update_cell(target_row, 1, formula)
        _log(log, f"    IMPORTRANGE 已寫入 A{target_row}")

        num_rows = _wait_and_convert(ws_allowance, f"A{target_row}", log)
        if num_rows == 0:
            raise ValueError("匯入資料為空，請確認請款試算表ID與期別")
        _log(log, f"    匯入完成：{num_rows} 筆")

        # Q=B(col2), R=F(col6), S=H(col8)
        _calc_qrs_direct(ws_allowance, target_row, num_rows, 2, 6, 8, log)
        _run_common_process(ws_allowance, log)

        ts = _punch("01專員請款", region, period)
        _log(log, f"✅ 01專員請款 {label} 完成｜{ts}")
        return True

    except Exception as e:
        _log(log, f"❌ 01專員請款失敗：{e}")
        return False


# ──────────────────────────────────────────────────────────────
# 02 儲值獎金
# ──────────────────────────────────────────────────────────────

def run_voucher(
    cleaning_file_id: str,
    region: str,
    period: str,
    is_first_half: bool,
    log: List[str],
    region_cfg: dict = None,
    payment_file_id: str = None,
) -> bool:
    label = "上半月" if is_first_half else "下半月"
    _log(log, f"▶ 02儲值獎金 {label} 開始")
    try:
        gc = get_gspread_client()
        ss = gc.open_by_key(cleaning_file_id)
        ws_voucher = ss.worksheet("02儲值獎金")

        # payment_file_id 由 salaryapp.py 透過 find_payment_file() 取得
        payment_id = payment_file_id or ""
        if not payment_id:
            raise ValueError("缺少金流對帳試算表 ID（payment_file_id），請確認已傳入")

        if is_first_half:
            _clear_first_half(ws_voucher, log)
            target_row = 2
        else:
            target_row = _first_empty_row_col_a(ws_voucher)
            _log(log, f"    下半月接續列：{target_row}")

        # 匯入欄位遮罩：只取 B(2),C(3),D(4),E(5),M(13),BB(54)，其餘補空字串
        # 使用 {IF(TRUE,col,...), ...} 的方式比 IMPORTRANGE 遮罩更可靠
        # 改用較簡單的方式：先全部匯入後再取欄
        formula = (
            f'=FILTER('
            f'IMPORTRANGE("{payment_id}","範本!A2:BB3000"),'
            f'IMPORTRANGE("{payment_id}","範本!A2:A3000")="儲值金"'
            f')'
        )
        ws_voucher.update_cell(target_row, 1, formula)
        _log(log, f"    IMPORTRANGE 已寫入 A{target_row}")

        num_rows = _wait_and_convert(ws_voucher, f"A{target_row}", log, extra_wait=8)
        if num_rows == 0:
            _log(log, "    ⚠️ 本期無儲值金資料")
            ts = _punch("02儲值獎金", region, period)
            _log(log, f"✅ 02儲值獎金 {label} 完成（無資料）｜{ts}")
            return True
        _log(log, f"    匯入完成：{num_rows} 筆（原始）")

        # 匯入後只保留需要的欄，其餘清空
        # 保留欄：B(2),C(3),D(4),E(5),M(13),BB(54)
        # 工作表現在 A 欄起對應原 A 欄（全部匯入）
        # 需要的欄（以工作表欄號1-based）: 2,3,4,5,13,54
        _voucher_keep_cols(ws_voucher, target_row, num_rows, {2, 3, 4, 5, 13, 54}, log)

        # 拆解 F 欄（原 BB=col54，匯入後在 col54 位置，但工作表只到 AC=col29）
        # 注意：工作表最多到 AC 欄，但 BB=col54 超出 AC 範圍
        # 因此轉值後，保留的欄會在工作表的對應位置
        # 重新讀取並展開：F欄（原BB欄資料）在轉值後的位置
        # 由 _voucher_expand_qrs 處理
        actual_rows = _voucher_expand_qrs(ws_voucher, target_row, num_rows, log)
        _log(log, f"    F欄拆解後共 {actual_rows} 列")

        _run_common_process(ws_voucher, log)

        ts = _punch("02儲值獎金", region, period)
        _log(log, f"✅ 02儲值獎金 {label} 完成｜{ts}")
        return True

    except Exception as e:
        _log(log, f"❌ 02儲值獎金失敗：{e}")
        return False


def _voucher_keep_cols(
    ws: gspread.Worksheet,
    start_row: int,
    num_rows: int,
    keep_cols: set,   # 1-based 欄號集合
    log: List[str],
) -> None:
    """
    只保留 keep_cols 的欄，其餘清空。
    因 FILTER(IMPORTRANGE) 全部匯入，需要把不要的欄清空。
    """
    end_row  = start_row + num_rows - 1
    last_col = ws.col_count
    clear_ranges = []
    for c in range(1, last_col + 1):
        if c not in keep_cols:
            letter = _col_letter(c)
            clear_ranges.append(f"{letter}{start_row}:{letter}{end_row}")
    if clear_ranges:
        # 批次清空（分批，避免請求過大）
        for i in range(0, len(clear_ranges), 50):
            ws.batch_clear(clear_ranges[i:i + 50])
    _log(log, f"    保留欄 {sorted(keep_cols)}，其餘已清空")


def _voucher_expand_qrs(
    ws: gspread.Worksheet,
    start_row: int,
    num_rows: int,
    log: List[str],
) -> int:
    """
    02儲值獎金：讀取保留欄後的資料，拆解 F 欄（原 BB，姓名字串），
    展開多列後寫回，並計算 QRS。

    保留欄對應（匯入並保留後）：
        工作表欄2  = 原 C 欄（日期）
        工作表欄3  = 原 D 欄（方案金額描述）
        工作表欄4  = 原 E 欄（期別）
        工作表欄5  = 原 M 欄（備註）
        工作表欄54 = 原 BB 欄（姓名字串，含 X 分隔）

    QRS：
        Q = I（col9，拆解後各人姓名）→ 寫到 Q(col17)
        R = H(col8) / G(col7)（G<2 以 2 計，四捨五入）
        S = TEXT(C(col2),"MM/DD") & E(col4)
    """
    end_row = start_row + num_rows - 1

    # 讀取關鍵欄：col2(C日期), col3(D方案), col4(E期別), col54(BB姓名)
    c_vals  = ws.get(f"B{start_row}:B{end_row}") or []   # col2=B（日期）
    d_vals  = ws.get(f"C{start_row}:C{end_row}") or []   # col3=C（方案）
    e_vals  = ws.get(f"D{start_row}:D{end_row}") or []   # col4=D（期別）
    bb_vals = ws.get(f"{_col_letter(54)}{start_row}:{_col_letter(54)}{end_row}") or []  # BB

    output_a: List[List] = []   # 回寫 A 欄（原值保留）
    q_data:   List[List] = []
    r_data:   List[List] = []
    s_data:   List[List] = []

    def _get(lst, i, default=""):
        return lst[i][0] if i < len(lst) and lst[i] else default

    for i in range(num_rows):
        c_val  = _get(c_vals, i)     # 日期
        d_val  = _get(d_vals, i)     # 方案描述
        e_val  = _get(e_vals, i)     # 期別
        bb_val = _get(bb_vals, i)    # 姓名字串

        # H 欄值（總獎金）
        d_str = str(d_val)
        if "50,000" in d_str or "50000" in d_str:
            h = 800
        elif "20,000" in d_str or "20000" in d_str:
            h = 320
        else:
            h = ""

        # 拆解 BB 欄姓名
        names = [n.strip() for n in re.split(r"\s*[Xx×Ｘ]\s*", str(bb_val)) if n.strip()]
        if not names:
            names = [""]

        # X 數量 = len(names) - 1（無X不複製，X1加1列，至多3列）
        names = names[:3]                   # 最多3人
        g     = max(2, len(names))          # G欄，最小2

        s_val = _format_date_mmdd(c_val) + str(e_val)

        for name in names:
            r_val = round(h / g) if h != "" else ""
            q_data.append([name])
            r_data.append([r_val])
            s_data.append([s_val])

    total   = len(q_data)
    new_end = start_row + total - 1

    if total > num_rows:
        # 需要插入額外列
        ws.insert_rows([[]] * (total - num_rows), row=end_row + 1)

    ws.update(f"Q{start_row}:Q{new_end}", q_data, value_input_option="USER_ENTERED")
    ws.update(f"R{start_row}:R{new_end}", r_data, value_input_option="USER_ENTERED")
    ws.update(f"S{start_row}:S{new_end}", s_data, value_input_option="USER_ENTERED")
    _log(log, f"    Q/R/S 寫入完成（展開後 {total} 列）")
    return total


# ──────────────────────────────────────────────────────────────
# 新人實境實習期別
# ──────────────────────────────────────────────────────────────

def run_newcomer_label(
    cleaning_file_id: str,
    region: str,
    period: str,
    is_first_half: bool,
    log: List[str],
    region_cfg: dict = None,
) -> bool:
    """
    新人實境實習期別。
    讀取薪資表 L1:1 員工名單，
    對照 03新人實境 AH 欄（姓名）× AF 欄（結訓日期）：
        結訓日期為空 → AK 欄（col37）寫入當期期別碼
    """
    label = "上半月" if is_first_half else "下半月"
    _log(log, f"▶ 新人實境實習期別 {label} 開始")
    try:
        gc = get_gspread_client()
        ss = gc.open_by_key(cleaning_file_id)
        ws_salary   = ss.worksheet("薪資表")
        ws_newcomer = ss.worksheet("03新人實境")

        period_code = period   # 直接用 period（如 "202604-2"）

        # 薪資表 L1:1 員工名單（L=col12, index 11）
        l1        = ws_salary.row_values(1)
        employees = {v.strip() for v in l1[11:] if v and v.strip()}
        _log(log, f"    薪資表員工數：{len(employees)}")

        # 讀取 03新人實境 AH（col34）和 AF（col32）欄
        ah_vals = ws_newcomer.col_values(34)  # AH
        af_vals = ws_newcomer.col_values(32)  # AF

        updates = []
        count   = 0
        for i in range(1, len(ah_vals)):    # index 0 = 標題
            name   = str(ah_vals[i]).strip() if i < len(ah_vals) else ""
            af_val = str(af_vals[i]).strip() if i < len(af_vals) else ""
            if not name or name not in employees:
                continue
            if not af_val:
                row = i + 1
                updates.append({
                    "range": f"'{ws_newcomer.title}'!AK{row}",
                    "values": [[period_code]],
                })
                count += 1

        if updates:
            ws_newcomer.spreadsheet.values_batch_update({
                "valueInputOption": "USER_ENTERED",
                "data": updates,
            })
        _log(log, f"    標註完成：{count} 筆")

        ts = _punch("新人實境實習期別", region, period)
        _log(log, f"✅ 新人實境實習期別 {label} 完成｜{ts}")
        return True

    except Exception as e:
        _log(log, f"❌ 新人實境實習期別失敗：{e}")
        return False


# ──────────────────────────────────────────────────────────────
# 03 新人實境 / 04 新人實習 / 05 組長津貼（共用框架）
# ──────────────────────────────────────────────────────────────

def run_newcomer(
    cleaning_file_id: str, region: str, period: str,
    is_first_half: bool, log: List[str],
    region_cfg: dict = None,
) -> bool:
    """03 新人實境。來源：新人實境!A2:L500；Q=C(idx2), R=200*K, S=TEXT(E)&G"""
    return _run_salary_module(
        cleaning_file_id, region, period, is_first_half, log,
        task_key  = "03新人實境",
        sheet_name= "03新人實境",
        src_sheet = "新人實境",
        id_cell   = "C3",
        src_range = "A2:L500",
        q_idx     = 2,          # C 欄（0-based index in A:L）
        r_expr    = "200*K",
    )


def run_intern(
    cleaning_file_id: str, region: str, period: str,
    is_first_half: bool, log: List[str],
    region_cfg: dict = None,
) -> bool:
    """04 新人實習。來源：新人實習!A2:L500；Q=C(idx2), R=200*K, S=TEXT(E)&G"""
    return _run_salary_module(
        cleaning_file_id, region, period, is_first_half, log,
        task_key  = "04新人實習",
        sheet_name= "04新人實習",
        src_sheet = "新人實習",
        id_cell   = "C3",
        src_range = "A2:L500",
        q_idx     = 2,          # C 欄
        r_expr    = "200*K",
    )


def run_leader(
    cleaning_file_id: str, region: str, period: str,
    is_first_half: bool, log: List[str],
    region_cfg: dict = None,
) -> bool:
    """05 組長津貼。來源：新人實習!A2:L500（同 04）；Q=H(idx7), R=J*K, S=TEXT(E)&G"""
    return _run_salary_module(
        cleaning_file_id, region, period, is_first_half, log,
        task_key  = "05組長津貼",
        sheet_name= "05組長津貼",
        src_sheet = "新人實習",  # ※ 同 04，來源工作表名稱相同
        id_cell   = "C3",
        src_range = "A2:L500",
        q_idx     = 7,          # H 欄（0-based index in A:L）
        r_expr    = "J*K",
    )


def _run_salary_module(
    cleaning_file_id: str,
    region: str,
    period: str,
    is_first_half: bool,
    log: List[str],
    task_key: str,
    sheet_name: str,
    src_sheet: str,
    id_cell: str,
    src_range: str,
    q_idx: int,
    r_expr: str,
    **kwargs,
) -> bool:
    """
    03/04/05 共用執行框架。
    q_idx  : Q 來源欄的 0-based index（對應 A:L 讀取後的位置）
    r_expr : "200*K" 或 "J*K"
    S 欄   : TEXT(E,"MM/DD") & G（固定）
    """
    label = "上半月" if is_first_half else "下半月"
    _log(log, f"▶ {task_key} {label} 開始")
    try:
        gc       = get_gspread_client()
        ss       = gc.open_by_key(cleaning_file_id)
        ws       = ss.worksheet(sheet_name)

        yyyymm   = period[:6]
        cfg      = kwargs.get("region_cfg") or {}
        # id_cell 對應 config 欄位：C3 → salary_id
        id_map   = {"C2": "allowance_id", "C3": "salary_id", "C4": "roster_id", "C5": "payment_id"}
        cfg_key  = id_map.get(id_cell, "salary_id")
        src_id   = str(cfg.get(cfg_key, "") or "").strip()
        schedule = f"{yyyymm}-{'1' if is_first_half else '2'}"
        if not src_id:
            raise ValueError(f"config 地區設定缺少 {cfg_key}（{id_cell}）")

        # 篩選結束列號（從 src_range 解析）
        filter_end = src_range.split(":")[1]  # 如 "L500" → "L500"
        filter_col = src_range[0]             # 篩選欄字母（"A"）

        if is_first_half:
            _clear_first_half(ws, log)
            target_row = 2
        else:
            target_row = _first_empty_row_col_a(ws)
            _log(log, f"    下半月接續列：{target_row}")

        formula = (
            f'=FILTER('
            f'IMPORTRANGE("{src_id}","{src_sheet}!{src_range}"),'
            f'IMPORTRANGE("{src_id}","{src_sheet}!{filter_col}2:{filter_end}")="{schedule}"'
            f')'
        )
        ws.update_cell(target_row, 1, formula)
        _log(log, f"    IMPORTRANGE 已寫入 A{target_row}")

        num_rows = _wait_and_convert(ws, f"A{target_row}", log)
        if num_rows == 0:
            _log(log, f"    ⚠️ {task_key} 無資料，結束")
            ts = _punch(task_key, region, period)
            _log(log, f"✅ {task_key} {label} 完成（無資料）｜{ts}")
            return True
        _log(log, f"    匯入完成：{num_rows} 筆")

        _calc_qrs_salary(ws, target_row, num_rows, q_idx, r_expr, log)
        _run_common_process(ws, log)

        ts = _punch(task_key, region, period)
        _log(log, f"✅ {task_key} {label} 完成｜{ts}")
        return True

    except Exception as e:
        _log(log, f"❌ {task_key} 失敗：{e}")
        return False
