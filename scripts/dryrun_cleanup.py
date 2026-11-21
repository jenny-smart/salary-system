"""
2026 承攬費申報補跑（台北／台中，202601～202611-1）。WRITE=1 才寫入；寫入前先備份申報檔。
- 清潔：台中 清潔各月 第1列每 3 欄寫入該月清潔檔 ID（1～10月用 -2 檔，11月用 202611-1）；
        台北 期別 B 欄為 importrange 自動帶入，只核對不寫。
- 水洗：其他承攬「薪資總表」B=姓名、E=-1 金額、F=-2 金額；
        第一階段＝當月 -1 檔 E，第二階段＝當月 -2 檔 F；申報檔沒有的姓名新增在最後一列（只填姓名）。
- 其他承攬 202611-1：台北／台中／新竹／桃園 沒有「薪資總表」時，從台北複製並清空 H 欄以後的個資。
不印身分證、地址、帳號。
"""
import os
import sys
from datetime import datetime

sys.path.insert(0, os.getcwd())
from gspread.utils import rowcol_to_a1  # noqa: E402
from modules import auth  # noqa: E402
from modules.cleaning_process_1 import find_cleaning_file  # noqa: E402
from modules.other_contract_process import _find_other_file  # noqa: E402

WRITE = os.environ.get("WRITE") == "1"
ROOT = {"台北": "1kJTGZYpgXBPlyR4SjRKoP0-sK30MhAfW", "台中": "1peMrCBDFJ2-sSKUzuc0X5p0Mc3g0VPrG",
        "桃園": "18Patt1SGG_6CWCqSaNgYNoDC4O0p5Vpm", "新竹": "1rnCtWrw3AUvn1PCj1NKsLtbM5UrWKuiI"}
REPORT = {"台北": "1PYUvL9bz3a-8f1r2WA0qEfy6O5rMKWEOytGJ-lRm9fg",
          "台中": "1eAt1gczhwHPOpVX2uFQumJ6xnvTWnij8N1w0IBY5qkk"}
# 水洗分頁版面：第一個月「第一階段」欄（0-based）、每月欄寬、人員起始列
WASH_LAYOUT = {"台北": (6, 4, 4), "台中": (5, 2, 4)}
PERIODS = [f"2026{m:02d}-{h}" for m in range(1, 12) for h in (1, 2)][:-1]

gc = auth.get_gspread_client()
jgc = auth.get_jenny_gspread_client()
jdrive = auth.get_jenny_drive_service()
print("模式：", "寫入" if WRITE else "預演（不寫入）")


def num(v):
    try:
        return float(str(v).replace(",", "")) if str(v).strip() not in ("", "#N/A") else 0.0
    except ValueError:
        return 0.0


# ── 找各期檔案 ──
files = {}
for region in ("台北", "台中"):
    for p in PERIODS:
        for kind, fn in (("清潔", find_cleaning_file), ("其他", _find_other_file)):
            try:
                files[(region, p, kind)] = fn(ROOT[region], p, region)
            except Exception:
                files[(region, p, kind)] = None

# ── 其他承攬 各期補「薪資總表」（台北／台中／新竹／桃園）──
src_other = gc.open_by_key(_find_other_file(ROOT["台北"], "202611-1", "台北"))
src_sheet = src_other.worksheet("薪資總表")
print("[薪資總表] 台北範本 E1/F1/E3/F3：", src_sheet.get("E1:F3", value_render_option="FORMULA"))
added = {}
for region in ("台北", "台中", "新竹", "桃園"):
    missing = []
    for p in PERIODS:
        try:
            oid = files.get((region, p, "其他")) or _find_other_file(ROOT[region], p, region)
        except Exception:
            continue
        files[(region, p, "其他")] = oid
        ss = gc.open_by_key(oid)
        if "薪資總表" in [w.title for w in ss.worksheets()]:
            continue
        missing.append(p)
        if WRITE:
            res = src_sheet.copy_to(oid)
            new = ss.get_worksheet_by_id(res["sheetId"])
            new.update_title("薪資總表")
            new.batch_clear([f"H3:{rowcol_to_a1(new.row_count, new.col_count)}"])  # 清掉台北人員個資
            added[(region, p)] = True
    print(f"[薪資總表] {region} 缺少：{'、'.join(missing) or '無'}" + ("（已補上）" if WRITE and missing else ""))

# ── 讀水洗金額（薪資總表）──
wash = {r: {} for r in ("台北", "台中")}   # wash[region][(name, month, stage)] = amount
for region in ("台北", "台中"):
    for p in PERIODS:
        oid = files[(region, p, "其他")]
        m, half = int(p[4:6]), p[-1]
        if not oid:
            print(f"[水洗] {region} {p} 其他承攬找不到 → 該階段不填")
            continue
        ss_o = gc.open_by_key(oid)
        col = 4 if half == "1" else 5
        try:
            rows = ss_o.worksheet("薪資總表").get("A3:F60", value_render_option="UNFORMATTED_VALUE") or []
        except Exception:
            # 預演時尚未補上：依薪資總表同樣規則計算（水洗薪資單 AD1 ＝ 本期 才取第285列）
            sh = ss_o.worksheet("水洗薪資表")
            ad1 = str((ss_o.worksheet("水洗薪資單").get("AD1") or [[""]])[0][0]).strip()
            r1 = (sh.get("J1:Z1") or [[]])[0]
            r285 = (sh.get("J285:Z285", value_render_option="UNFORMATTED_VALUE") or [[]])[0] + [""] * 20
            rows = []
            for i, n in enumerate(r1):
                if str(n).strip():
                    v = r285[i] if ad1 == p else 0
                    rows.append(["水洗", n, "", "", v if half == "1" else "", v if half == "2" else ""])
            print(f"    （{region} {p} 尚無薪資總表，依規則估算：AD1={ad1}）")
        for r in rows:
            r = list(r) + [""] * 6
            if str(r[0]).strip() != "水洗" or not str(r[1]).strip() or str(r[1]).strip() == "0":
                continue
            wash[region][(str(r[1]).strip(), m, half)] = num(r[col])

# ── 備份申報檔 ──
stamp = datetime.now().strftime("%Y%m%d-%H%M")
if WRITE:
    for region, rid in REPORT.items():
        meta = jdrive.files().get(fileId=rid, fields="name,parents", supportsAllDrives=True).execute()
        cp = jdrive.files().copy(fileId=rid, supportsAllDrives=True, body={
            "name": f"[備份{stamp}]{meta['name']}", "parents": meta.get("parents", [])}).execute()
        print(f"[備份] {region} → {cp['id']}")

# ── 清潔 ──
tc = jgc.open_by_key(REPORT["台中"])
ws = tc.worksheet("清潔各月")
row1 = (ws.get("A1:AI1") or [[]])[0] + [""] * 40
updates = []
for m in range(1, 12):
    p = f"2026{m:02d}-2" if m < 11 else "202611-1"
    fid = files[("台中", p, "清潔")]
    c = 3 * (m - 1)
    want = [fid or "", f"2026{m:02d}-1", f"2026{m:02d}-2"]
    have = [str(row1[c]).rstrip("/"), row1[c + 1], row1[c + 2]]
    if want != have:
        print(f"[清潔] 台中 {m}月 {rowcol_to_a1(1, c + 1)}：{str(have[0])[:12]}…/{have[1]} → {str(want[0])[:12]}…（{p}）/{want[1]}")
        updates.append({"range": f"{rowcol_to_a1(1, c + 1)}:{rowcol_to_a1(1, c + 3)}", "values": [want]})
if WRITE and updates:
    ws.batch_update(updates, value_input_option="RAW")
    print(f"    台中 清潔各月 已更新 {len(updates)} 個月")

tp = jgc.open_by_key(REPORT["台北"])
qb = tp.worksheet("期別").get("A2:B13") or []
for i, r in enumerate(qb[:11], 1):
    r = list(r) + ["", ""]
    p = f"2026{i:02d}-2" if i < 11 else "202611-1"
    fid = files[("台北", p, "清潔")] or ""
    got = str(r[1]).strip().rstrip("/")
    ok = "一致" if got == fid else f"不一致（期別表={got!r}，程式找到 {p}={fid!r}）"
    print(f"[清潔] 台北 {r[0]}：{ok}")

# ── 水洗 ──
for region in ("台北", "台中"):
    first, width, start = WASH_LAYOUT[region]
    ss = tp if region == "台北" else tc
    w = ss.worksheet("水洗")
    names = [str(x[0]).strip() if x else "" for x in (w.get(f"A{start}:A400") or [])]
    row_of = {n: start + i for i, n in enumerate(names) if n}
    last = start + max([i for i, n in enumerate(names) if n], default=-1)
    people = sorted({k[0] for k in wash[region]})
    data, new_rows = [], []
    for n in people:
        if n not in row_of:
            last += 1
            row_of[n] = last
            new_rows.append({"range": f"A{last}", "values": [[n]]})
            print(f"[水洗] {region} 新增人員 {n} → 第{last}列")
    cur = {}
    if row_of:
        top, bot = min(row_of.values()), max(row_of.values())
        grid = w.get(f"A{top}:{rowcol_to_a1(bot, first + width * 11)}", value_render_option="UNFORMATTED_VALUE") or []
        for r_i, row in enumerate(grid):
            cur[top + r_i] = list(row)
    for (n, m, half), amt in sorted(wash[region].items(), key=lambda k: (k[0][0], k[0][1], k[0][2])):
        c = first + width * (m - 1) + (0 if half == "1" else 1)
        r = row_of[n]
        old = (cur.get(r, []) + [""] * 200)[c]
        new = int(amt) if float(amt).is_integer() else amt
        if num(old) != num(new) or (old == "" and new != 0):
            if old == "" and new == 0:
                continue
            print(f"[水洗] {region} {n} {m}月{'第一' if half == '1' else '第二'}階段 {rowcol_to_a1(r, c + 1)}：{old!r} → {new}")
            data.append({"range": rowcol_to_a1(r, c + 1), "values": [[new]]})
    if WRITE:
        if new_rows:
            w.batch_update(new_rows, value_input_option="RAW")
        if data:
            w.batch_update(data, value_input_option="RAW")
        print(f"    {region} 水洗 已寫入 {len(data)} 格、新增 {len(new_rows)} 人")
print("完成")
