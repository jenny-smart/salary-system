"""
清潔承攬／其他承攬費申報（承攬作業 ⑥）
檔案：modules/contract_report.py

每期（只做 -2 期別）執行：
  1. 清潔承攬檔：把「場次時數薪資總表」複製成「「場次時數薪資總表」的副本」並轉為靜態值（留底）
  2. 其他承攬檔：把「薪資總表」複製成「「薪資總表」的副本」並轉為靜態值（留底）
  3. 承攬費申報檔（地區設定 contract_report_id）：
       期別分頁    第 月份+1 列：A=期別、B=清潔承攬 ID、C=其他承攬 ID
       清潔各月    該月區塊：第1列 ID（引用期別!B）、第2列合計、第6列起每人第一／第二階段
       水洗各月    同清潔各月版面；以 IMPORTRANGE 公式讀其他承攬「「薪資總表」的副本」（A 欄＝水洗），
                   副本重產後自動更新（每個新來源檔第一次需在申報檔按「允許存取」）
       新訓退訓實境費用  自年度「專員薪資相關」同名分頁整張覆寫數值
     清潔各月由 Python 寫入數值。
清潔各月／水洗各月每月區塊寬度沿用申報檔原本版面（台北 4 欄、台中 3 欄），
讓「清潔」分頁原本的 VLOOKUP 不用改。
"""

from __future__ import annotations

import re
from typing import Callable, List, Optional

CLEAN_SRC, CLEAN_COPY = "場次時數薪資總表", "「場次時數薪資總表」的副本"
OTHER_SRC, OTHER_COPY = "薪資總表", "「薪資總表」的副本"
PERIOD_SHEET, CLEAN_MONTHLY, WASH_MONTHLY = "期別", "清潔各月", "水洗各月"
TRAINING_SHEET = "新訓退訓實境費用"
PERSON_START_ROW = 6          # 清潔各月：第 6 列起為每人資料（2～5 列為合計列）
DEFAULT_BLOCK_WIDTH = 4


# ──────────────────────────────────────────────────────────────
# 純函式（可測試）
# ──────────────────────────────────────────────────────────────

def block_width(row1: list) -> int:
    """依第 1 列兩個「…-1」標籤的距離判斷每月區塊寬度（台北 4、台中 3）。"""
    idx = [i for i, v in enumerate(row1 or []) if re.fullmatch(r"\d{5,6}-1", str(v).strip())]
    if len(idx) >= 2 and 2 <= idx[1] - idx[0] <= 6:
        return idx[1] - idx[0]
    return DEFAULT_BLOCK_WIDTH


def month_labels(period: str, current: tuple = ("", "")) -> List[str]:
    """區塊第 1 列的期別標籤；原本已是同月份（西元或民國）就保留，否則改成西元。"""
    yyyy, mm = int(period[:4]), period[4:6]
    ok = {f"{yyyy}{mm}", f"{yyyy - 1911}{mm}"}
    out = []
    for half, cur in zip(("1", "2"), current):
        cur = str(cur or "").strip()
        out.append(cur if cur[:-2] in ok and cur.endswith(f"-{half}") else f"{yyyy}{mm}-{half}")
    return out


def _num(v):
    if v in ("", None):
        return ""
    try:
        f = float(str(v).replace(",", ""))
        return int(f) if f.is_integer() else f
    except ValueError:
        return v


def clean_person_rows(copy_values: list, width: int) -> List[list]:
    """
    「場次時數薪資總表」的副本 → 每人一列，等同原公式
    FILTER(A4:F, A<>0) 取 {姓名, D 第一階段, E 第二階段, F 薪資申報}，依區塊寬度截取。
    """
    rows = []
    for r in (copy_values or [])[3:]:          # 第 4 列起
        r = list(r) + [""] * 6
        name = str(r[0]).strip()
        if not name or name == "0":
            continue
        rows.append([name, _num(r[3]), _num(r[4]), _num(r[5])][:max(width, 3)])
    return rows


def wash_person_rows(copy_values: list, width: int) -> List[list]:
    """「薪資總表」的副本 → A 欄＝水洗 的每人 {姓名 B, 第一階段 E, 第二階段 F}。"""
    rows = []
    for r in (copy_values or [])[2:]:          # 第 3 列起
        r = list(r) + [""] * 6
        name = str(r[1]).strip()
        if str(r[0]).strip() != "水洗" or not name or name == "0":
            continue
        rows.append([name, _num(r[4]), _num(r[5])] + [""] * max(width - 3, 0))
    return rows


def col_letter(n: int) -> str:
    s = ""
    while n:
        n, rem = divmod(n - 1, 26)
        s = chr(65 + rem) + s
    return s


# ──────────────────────────────────────────────────────────────
# Google Sheets 操作
# ──────────────────────────────────────────────────────────────

def _open(spreadsheet_id: str):
    """Jenny 帳號優先（申報檔與期別檔的擁有者，可寫入）；不可用時改用 Service Account。"""
    from modules.auth import get_gspread_client, open_jenny_spreadsheet
    try:
        return open_jenny_spreadsheet(spreadsheet_id)
    except Exception:
        return get_gspread_client().open_by_key(spreadsheet_id)


def trim_rows(values: list) -> list:
    """去掉尾端整列空白（含只有核取方塊 FALSE）的列。"""
    rows = [list(r) for r in (values or [])]
    while rows and all(str(v).strip() in ("", "FALSE") for v in rows[-1]):
        rows.pop()
    return rows


def _sheet(ss, title: str, rows: int = 1000, cols: int = 26, create: bool = True):
    try:
        return ss.worksheet(title)
    except Exception:
        if not create:
            return None
        return ss.add_worksheet(title=title, rows=rows, cols=cols)


def snapshot_sheet(ss, src_title: str, copy_title: str) -> list:
    """把 src_title 複製成 copy_title 並轉為靜態值（已存在就重建）；回傳副本內容（UNFORMATTED）。"""
    src = ss.worksheet(src_title)
    old = _sheet(ss, copy_title, create=False)
    if old is not None:
        ss.del_worksheet(old)
    new = ss.duplicate_sheet(src.id, insert_sheet_index=len(ss.worksheets()), new_sheet_name=copy_title)
    ss.batch_update({"requests": [{"copyPaste": {
        "source": {"sheetId": new.id}, "destination": {"sheetId": new.id},
        "pasteType": "PASTE_VALUES",
    }}]})
    return new.get(value_render_option="UNFORMATTED_VALUE") or []


def _ensure_cols(ws, need: int):
    if ws.col_count < need:
        ws.add_cols(need - ws.col_count)


def write_month_block(ws, month: int, width: int, id_formula: str, labels: List[str],
                      totals: list, person_rows: List[list], label_a2: str = ""):
    """寫入某月區塊：第1列 ID 公式＋標籤、第2列合計、第6列起每人資料（先清空舊資料）。"""
    c0 = (month - 1) * width + 1
    c_last = c0 + width - 1
    _ensure_cols(ws, c_last)
    first, last = col_letter(c0), col_letter(c_last)
    row1 = (ws.get(f"{first}1:{last}1", value_render_option="FORMULA") or [[]])[0] + [""] * width
    id_cell = row1[0] if "期別" in str(row1[0]) else id_formula
    data = [
        {"range": f"{first}1:{col_letter(c0 + 2)}1", "values": [[id_cell] + labels]},
        {"range": f"{col_letter(c0 + 1)}2:{col_letter(c0 + 2)}2", "values": [list(totals[:2])]},
    ]
    if label_a2 and month == 1:
        data.append({"range": "A2", "values": [[label_a2]]})
    ws.batch_clear([f"{first}{PERSON_START_ROW}:{last}{ws.row_count}"])
    if person_rows:
        end = PERSON_START_ROW + len(person_rows) - 1
        if end > ws.row_count:
            ws.add_rows(end - ws.row_count)
        data.append({"range": f"{first}{PERSON_START_ROW}:{last}{end}",
                     "values": [(r + [""] * width)[:width] for r in person_rows]})
    ws.batch_update(data, value_input_option="USER_ENTERED")


def wash_formulas(month: int, width: int, period_row: int) -> dict:
    """水洗各月某月區塊的公式：第1列 ID、第2列合計、第6列 FILTER（A＝水洗、姓名非空）。"""
    c0 = (month - 1) * width + 1
    a1 = f"{col_letter(c0)}$1"
    src = "「薪資總表」的副本"

    def imp(rng):
        return f'IMPORTRANGE({a1},"{src}!{rng}")'

    return {
        "id": f"='{PERIOD_SHEET}'!C{period_row}",
        # 不包 IFERROR：未授權時要顯示 #REF! 才能按「允許存取」；只把 FILTER 找不到（#N/A）轉空白
        "totals": f"={imp('E2:F2')}",
        "people": (f'=IFNA(FILTER({{{imp("B3:B80")},{imp("E3:F80")}}},'
                   f'{imp("A3:A80")}="水洗",{imp("B3:B80")}<>"",{imp("B3:B80")}<>0),"")'),
    }


def write_wash_formula_block(ws, month: int, width: int, labels: List[str], label_a2: str = "薪資表"):
    c0 = (month - 1) * width + 1
    c_last = c0 + width - 1
    _ensure_cols(ws, c_last)
    first, last = col_letter(c0), col_letter(c_last)
    f = wash_formulas(month, width, month + 1)
    ws.batch_clear([f"{first}2:{last}{ws.row_count}"])
    data = [
        {"range": f"{first}1:{col_letter(c0 + 2)}1", "values": [[f["id"]] + labels]},
        {"range": f"{col_letter(c0 + 1)}2", "values": [[f["totals"]]]},
        {"range": f"{first}{PERSON_START_ROW}", "values": [[f["people"]]]},
    ]
    if month == 1 and label_a2:
        data.append({"range": "A2", "values": [[label_a2]]})
    ws.batch_update(data, value_input_option="USER_ENTERED")


def same_values(a: list, b: list) -> bool:
    """兩張表的值是否相同（忽略尾端空白列／欄）。"""
    def norm(rows):
        out = [[str(v).strip() for v in r] for r in (rows or [])]
        out = [r[:max([i + 1 for i, v in enumerate(r) if v] or [0])] for r in out]
        while out and not out[-1]:
            out.pop()
        return out
    return norm(a) == norm(b)


# ──────────────────────────────────────────────────────────────
# 主流程
# ──────────────────────────────────────────────────────────────

def run_contract_report_all(
    region: str, period: str, root_folder_id: str, region_cfg: dict,
    log: Callable[[str], None],
    find_cleaning: Optional[Callable] = None, find_other: Optional[Callable] = None,
) -> bool:
    if not str(period).endswith("-2"):
        log("  承攬費申報只做 -2 期別，上半月略過")
        log(f"✅ 清潔承攬／其他承攬費申報 完成（{period} 略過）")
        return True
    cfg = region_cfg or {}
    report_id = str(cfg.get("contract_report_id", "") or "").strip()
    if not report_id:
        log(f"❌ 「{region}」地區設定缺少 contract_report_id（承攬費申報試算表 ID）")
        return False
    if find_cleaning is None:
        from modules.cleaning_process_1 import find_cleaning_file as find_cleaning
    if find_other is None:
        from modules.other_contract_process import _find_other_file as find_other

    month = int(period[4:6])
    yyyy = int(period[:4])
    log(f"▶ 清潔承攬／其他承攬費申報 {region} {period}")

    clean_id = find_cleaning(root_folder_id, period, region)
    clean_ss = _open(clean_id)
    clean_vals = snapshot_sheet(clean_ss, CLEAN_SRC, CLEAN_COPY)
    log(f"  清潔承攬：已建立「{CLEAN_COPY}」（{len(clean_vals)} 列）")

    other_id, other_vals = "", None
    try:
        other_id = find_other(root_folder_id, period, region)
        other_vals = snapshot_sheet(_open(other_id), OTHER_SRC, OTHER_COPY)
        log(f"  其他承攬：已建立「{OTHER_COPY}」（{len(other_vals)} 列）")
    except Exception as e:
        log(f"⚠️ 其他承攬略過（水洗各月不更新）：{e}")

    report = _open(report_id)

    # 期別
    ws_p = _sheet(report, PERIOD_SHEET)
    head = (ws_p.get("A1:C1") or [[]])[0] + ["", "", ""]
    head_upd = []
    if not str(head[0]).strip() and cfg.get("salary_id"):
        head_upd.append({"range": "A1", "values": [[cfg["salary_id"]]]})
    if not str(head[1]).strip():
        head_upd.append({"range": "B1", "values": [[yyyy - 1911]]})
    if not str(head[2]).strip():
        head_upd.append({"range": "C1", "values": [["其他承攬ID"]]})
    head_upd.append({"range": f"A{month + 1}:C{month + 1}", "values": [[period, clean_id, other_id]]})
    ws_p.batch_update(head_upd, value_input_option="RAW")
    log(f"  期別：第 {month + 1} 列 已寫入清潔／其他承攬 ID")

    # 清潔各月
    ws_c = _sheet(report, CLEAN_MONTHLY, rows=1005, cols=12 * DEFAULT_BLOCK_WIDTH)
    width = block_width((ws_c.get("A1:BZ1") or [[]])[0])
    c0 = (month - 1) * width + 1
    cur = (ws_c.get(f"{col_letter(c0 + 1)}1:{col_letter(c0 + 2)}1") or [["", ""]])[0] + ["", ""]
    totals = (list(clean_vals[2]) + [""] * 6)[3:5] if len(clean_vals) > 2 else ["", ""]
    people = clean_person_rows(clean_vals, width)
    write_month_block(ws_c, month, width, f"='{PERIOD_SHEET}'!B{month + 1}",
                      month_labels(period, tuple(cur[:2])), [_num(t) for t in totals], people, "薪資表")
    log(f"  清潔各月：{month}月 {len(people)} 人（區塊寬 {width} 欄）")

    # 水洗各月
    if other_vals is not None:
        ws_w = _sheet(report, WASH_MONTHLY, rows=1000, cols=12 * width)
        cur_w = (ws_w.get(f"{col_letter(c0 + 1)}1:{col_letter(c0 + 2)}1") or [["", ""]])[0] + ["", ""]
        write_wash_formula_block(ws_w, month, width, month_labels(period, tuple(cur_w[:2])))
        log(f"  水洗各月：{month}月 已寫入 IMPORTRANGE 公式（副本 {len(wash_person_rows(other_vals, width))} 人；"
            "新來源檔第一次請在申報檔按「允許存取」）")

    # 新訓退訓實境費用
    salary_id = str(cfg.get("salary_id", "") or "").strip()
    try:
        src = trim_rows(_open(salary_id).worksheet(TRAINING_SHEET).get_all_values())
        ws_t = _sheet(report, TRAINING_SHEET, rows=max(len(src) + 10, 100),
                      cols=max((len(r) for r in src), default=6))
        ws_t.clear()
        if src:
            ws_t.update(values=src, range_name="A1", value_input_option="USER_ENTERED")
        log(f"  {TRAINING_SHEET}：已同步 {len(src)} 列")
    except Exception as e:
        log(f"⚠️ {TRAINING_SHEET} 同步失敗（不影響申報）：{e}")

    try:
        from modules.master_sheet import record_execution
        record_execution(region, period, "承攬費申報", None)
    except Exception as e:
        log(f"⚠️ 主控表打卡失敗：{e}")
    log(f"✅ 清潔承攬／其他承攬費申報 完成（{region} {period}）")
    return True
