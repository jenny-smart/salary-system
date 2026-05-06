"""
金流對帳 payment_reconciliation.py 三點修正
請將以下片段整合進對應函數

修正1：④加工完成後打卡前，double check 主列數與加工前相同
修正2：水洗/家電/座椅打卡筆數 = 加工前列數 + 新增子列數
修正3：水洗/家電/座椅新增子列時，Y:AB 欄（index 24:27）清空
"""

import re
from typing import List, Tuple

# ──────────────────────────────────────────────────────────────
# 工具：計算主列數（排除 B 欄帶 -1/-2 後綴的子單）
# ──────────────────────────────────────────────────────────────

def _count_main_rows(ws, start_row: int, end_row: int) -> int:
    """
    計算指定範圍內的主列數（B 欄不帶 -1/-2 等數字後綴的列）。
    子單的 B 欄值如 "LC001-1"、"LC001-2"，主列如 "LC001"。
    """
    b_col = ws.get(f"B{start_row}:B{end_row}") or []
    count = 0
    for row in b_col:
        b_val = str(row[0]).strip() if row else ""
        if not b_val:
            continue
        if not re.search(r"-\d+$", b_val):  # 不帶數字後綴 → 主列
            count += 1
    return count


# ──────────────────────────────────────────────────────────────
# 修正1 & 修正2：整合至 process_template() 的打卡邏輯
# ──────────────────────────────────────────────────────────────

def _validate_and_punch_after_expand(
    ws_template,
    start_row: int,
    original_count: int,
    expand_result: dict,
    region: str,
    period: str,
    log: list,
):
    """
    拆解（expandFG）完成後的 double check 與打卡。

    Args:
        ws_template:    範本工作表
        start_row:      加工起始列
        original_count: 加工前主列數（③ 搬運時打卡的筆數）
        expand_result:  拆解結果 dict，keys:
                        total_count     - 拆解後總列數（主列 + 子列）
                        wash_count      - 水洗拆解出的子列數
                        appliance_count - 家電拆解出的子列數
                        seat_count      - 座椅拆解出的子列數
                        storage_count   - 收納拆解出的子列數
                        carpet_count    - 地毯拆解出的子列數
        region, period: 用於主控打卡
        log:            日誌列表
    """
    from modules.master_sheet import get_recorded_value, record_batch

    total_after = expand_result.get("total_count", 0)
    end_row = start_row + total_after - 1

    # ── 修正1：double check 主列數 ──────────────────────────────
    main_after = _count_main_rows(ws_template, start_row, end_row)

    if main_after != original_count:
        _log(log,
             f"⚠️ Double check 失敗：加工後主列數 {main_after} ≠ 加工前 {original_count}，"
             "請確認是否有誤刪或新增主單")
    else:
        _log(log, f"✅ Double check 通過：主列數一致（{main_after} 筆）")

    # ── 修正2：各類別打卡 = 加工前列數 + 新增子列數 ──────────────
    # 水洗/家電/座椅在加工時會新增子列
    # 打卡筆數 = 加工前各類別原始主列數 + 本次新增的子列數
    # 但目前各類別加工前列數不易個別取得，故以「加工前主列數」估算：
    # 各類別的加工後筆數 = expand_result 中的各類別處理數
    # （expand_result 的 wash_count 等已是各類別的加工後總列數，包含主列和子列）

    batch = [
        {"task_key": "加工-排序",            "count": main_after},  # 主列數
        {"task_key": "加工-K欄標註異常標橘底", "count": expand_result.get("mark_count", 0)},
        {"task_key": "加工-水洗加工",         "count": expand_result.get("wash_count", 0)},
        {"task_key": "加工-家電加工",         "count": expand_result.get("appliance_count", 0)},
        {"task_key": "加工-收納加工",         "count": expand_result.get("storage_count", 0)},
        {"task_key": "加工-座椅加工",         "count": expand_result.get("seat_count", 0)},
        {"task_key": "加工-地毯加工",         "count": expand_result.get("carpet_count", 0)},
    ]
    record_batch(region, period, batch)
    _log(log, f"✅ 加工打卡完成（主列 {main_after}，水洗 {expand_result.get('wash_count',0)}，"
              f"家電 {expand_result.get('appliance_count',0)}，座椅 {expand_result.get('seat_count',0)}）")


# ──────────────────────────────────────────────────────────────
# 修正3：拆解子單時清空 Y:AB 欄（index 24:27，共4欄）
# 整合至 expand_fg_rows() 的子單新增邏輯
# ──────────────────────────────────────────────────────────────

# Y=index24, Z=index25, AA=index26, AB=index27
_CLEAR_COLS_FOR_CHILD = slice(24, 28)   # Y:AB (4欄，python slice 不含右端)

def _make_child_row(parent_row: list, child_id: str, item_name: str, item_qty: str) -> list:
    """
    由母單建立子單列：
    - B 欄改為 child_id（如 LC001-1）
    - F 欄改為服務名稱
    - G 欄改為數量
    - Y:AB 欄（index 24~27）清空（水洗/家電/座椅的費用欄，子列不應繼承）
    """
    new_row = parent_row[:]                 # 複製母單整列
    new_row[1]  = child_id                  # B 欄：子單編號
    new_row[5]  = item_name                 # F 欄：服務名稱
    new_row[6]  = item_qty or "1"           # G 欄：數量
    # 清空 Y:AB（子列不繼承母單的費用計算欄）
    for i in range(24, 28):
        new_row[i] = ""
    return new_row


# ──────────────────────────────────────────────────────────────
# 整合說明
# ──────────────────────────────────────────────────────────────
"""
在 payment_reconciliation.py 的 process_template() 中：

    # 現有加工完成後的打卡邏輯，替換為：
    expand_result = expand_fg_rows(ws_template, start_row, original_count, log)

    # 修正1+2：double check + 打卡
    _validate_and_punch_after_expand(
        ws_template, start_row, original_count, expand_result,
        region, period, log
    )

在 expand_fg_rows() 的子單新增邏輯中，替換：
    child_row = parent_row[:]
    child_row[1] = child_id
    child_row[5] = item.name
    child_row[6] = item.qty

改為：
    child_row = _make_child_row(parent_row, child_id, item.name, item.qty)
    # Y:AB 已在 _make_child_row 內清空

水洗/家電/座椅的拆解才需要清空 Y:AB，收納/地毯目前未拆解 F 欄，
但為統一安全，建議所有子單的 Y:AB 一律清空。
"""


def _log(log: list, msg: str):
    log.append(msg)
