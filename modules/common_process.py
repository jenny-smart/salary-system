"""
清潔承攬共用處理模組
對應 GAS runCommonProcess()

適用功能：01專員請款、02儲值獎金、03新人實境、04新人實習、05組長津貼

流程：
Q/R/S 計算完後 →
1. 過濾 Q 有值且 R≠0 → 貼到 V/W/X（第 22/23/24 欄）
2. U 欄（第 21）統計同名出現次數
3. 排序 U→V→X
4. 合併備註 Y 欄（第 25），逗號分隔
5. V 欄去重複 → AA 欄（第 27）
6. AB 欄（第 28）：SUMIF 金額加總
7. AC 欄（第 29）：備註組字
"""

from modules.sheet_helper import open_spreadsheet


def run_common_process(file_id: str, sheet_name: str) -> dict:
    """
    執行共用整理流程
    回傳：{"processed": 處理筆數, "unique_names": 去重後人數}
    """
    ss = open_spreadsheet(file_id)
    sheet = ss.worksheet(sheet_name)

    last_row = len(sheet.col_values(1))
    if last_row <= 1:
        return {"processed": 0, "unique_names": 0}

    num_rows = last_row - 1

    # 讀取 Q/R/S（第 17/18/19 欄，1-based）
    qrs_data = sheet.get(f"Q2:S{last_row}")

    # ─── 步驟1：過濾 Q 有值且 R≠0 → V/W/X ───
    vwx_data = []
    for row in qrs_data:
        q = row[0] if len(row) > 0 else ""
        r = row[1] if len(row) > 1 else ""
        s = row[2] if len(row) > 2 else ""
        try:
            r_val = float(str(r).replace(",", "")) if r else 0
        except (ValueError, TypeError):
            r_val = 0

        if q and q != "" and r_val != 0:
            vwx_data.append([q, r_val, s])
        else:
            vwx_data.append(["", "", ""])

    # 過濾真正有資料的列
    valid_rows = [(i, row) for i, row in enumerate(vwx_data) if row[0] != ""]

    if not valid_rows:
        return {"processed": 0, "unique_names": 0}

    # ─── 步驟2：統計 U 欄（V 欄姓名出現次數）───
    # 先算各姓名出現次數
    name_count = {}
    for _, row in valid_rows:
        name = row[0]
        name_count[name] = name_count.get(name, 0) + 1

    # 組合 U/V/W/X 資料
    uvwx_rows = []
    for _, row in valid_rows:
        name = row[0]
        uvwx_rows.append([name_count[name], row[0], row[1], row[2]])

    # ─── 步驟3：排序 U→V→X（count → name → remark）───
    uvwx_rows.sort(key=lambda r: (r[0], r[1], r[3]))

    # ─── 步驟4：合併備註 Y 欄（相同姓名的備註用逗號串）───
    processed_names = set()
    final_rows = []  # [u, v, w, x, y]

    i = 0
    while i < len(uvwx_rows):
        count = uvwx_rows[i][0]
        name = uvwx_rows[i][1]

        if name in processed_names:
            i += 1
            continue

        processed_names.add(name)

        # 收集同名的所有備註
        remarks = []
        for j in range(i, min(i + count, len(uvwx_rows))):
            x_val = uvwx_rows[j][3]
            if x_val and str(x_val).strip():
                remarks.append(str(x_val).strip())

        combined_remark = ",".join(remarks)

        # 只保留第一列（合計金額、合併備註）
        final_rows.append([
            count,              # U：出現次數
            name,               # V：姓名
            uvwx_rows[i][2],    # W：金額（第一筆）
            uvwx_rows[i][3],    # X：原備註
            combined_remark     # Y：合併備註
        ])

        i += count

    if not final_rows:
        return {"processed": 0, "unique_names": 0}

    # ─── 步驟5：V 欄去重複 → AA 欄，同時算 AB/AC ───
    unique_names = [row[1] for row in final_rows]  # V 欄去重後即為 final_rows

    # AA 欄寫入唯一姓名
    aa_data = [[name] for name in unique_names]

    # AB 欄：SUMIF 金額（用 Python 計算，不用公式）
    # 先建立 name → 金額加總 的 map
    name_amount_map = {}
    for _, row in valid_rows:
        name = row[0]
        try:
            amount = float(str(row[1]).replace(",", "")) if row[1] else 0
        except (ValueError, TypeError):
            amount = 0
        name_amount_map[name] = name_amount_map.get(name, 0) + amount

    # AC 欄：備註
    name_remark_map = {row[1]: row[4] for row in final_rows}

    ab_data = [[name_amount_map.get(name, 0)] for name in unique_names]
    ac_data = [[name_remark_map.get(name, "")] for name in unique_names]

    # ─── 寫回試算表 ───
    # 先清空 U:AC 區域
    sheet.batch_clear([f"U2:AC{last_row}"])

    # 寫入 U/V/W/X/Y（第 21-25 欄）
    uvwxy_output = [
        [row[0], row[1], row[2], row[3], row[4]]
        for row in final_rows
    ]
    if uvwxy_output:
        sheet.update(f"U2:Y{1 + len(uvwxy_output)}", uvwxy_output)

    # 寫入 AA/AB/AC（第 27-29 欄）
    if unique_names:
        aabac_output = [
            [aa_data[i][0], ab_data[i][0], ac_data[i][0]]
            for i in range(len(unique_names))
        ]
        sheet.update(f"AA2:AC{1 + len(unique_names)}", aabac_output)

    return {
        "processed": len(valid_rows),
        "unique_names": len(unique_names)
    }
