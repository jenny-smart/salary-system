"""唯讀盤點：承攬費申報檔版面、各期清潔／其他承攬檔案 ID（不寫入任何檔案）。"""
import os
import sys

sys.path.insert(0, os.getcwd())
from modules import auth  # noqa: E402
from modules.cleaning_process_1 import find_cleaning_file  # noqa: E402
from modules.other_contract_process import _find_other_file  # noqa: E402

REPORT = {"台北": "1PYUvL9bz3a-8f1r2WA0qEfy6O5rMKWEOytGJ-lRm9fg",
          "台中": "1eAt1gczhwHPOpVX2uFQumJ6xnvTWnij8N1w0IBY5qkk"}
ROOT = {"台北": "1kJTGZYpgXBPlyR4SjRKoP0-sK30MhAfW", "台中": "1peMrCBDFJ2-sSKUzuc0X5p0Mc3g0VPrG"}
PERIODS = [f"2026{m:02d}-{h}" for m in range(1, 12) for h in (1, 2)][:-1]

gc = auth.get_gspread_client()
for region, rid in REPORT.items():
    print(f"\n===== {region} 申報檔 =====")
    ss = gc.open_by_key(rid)
    print("分頁：", [w.title for w in ss.worksheets()])
    for tab, rng in (("清潔各月", "A1:AL3"), ("水洗", "A1:AD3")):
        try:
            ws = ss.worksheet(tab)
        except Exception as e:
            print(f"  {tab}：無（{e}）")
            continue
        print(f"  {tab} 大小 {ws.row_count}x{ws.col_count}")
        for r, row in enumerate(ws.get(rng, value_render_option="FORMULA") or [], 1):
            print(f"   R{r}:", [str(v)[:70] for v in row])
        colA = ws.col_values(1)[:20]
        print(f"   A1:A20：", [str(v)[:45] for v in colA])

for region, root in ROOT.items():
    print(f"\n===== {region} 各期檔案 =====")
    for p in PERIODS:
        try:
            c = find_cleaning_file(root, p, region)
        except Exception as e:
            c = f"找不到（{str(e)[:60]}）"
        try:
            o = _find_other_file(root, p, region)
        except Exception as e:
            o = f"找不到（{str(e)[:60]}）"
        print(f"  {p} 清潔={c} 其他={o}")

# 其他承攬檔版面（台中 202610-2）
try:
    oid = _find_other_file(ROOT["台中"], "202610-2", "台中")
    ss = gc.open_by_key(oid)
    print("\n===== 台中 202610-2 其他承攬 分頁：", [w.title for w in ss.worksheets()])
    ws = ss.worksheet("水洗薪資表")
    rows = ws.get("A1:C300", value_render_option="FORMULA") or []
    for i, r in enumerate(rows, 1):
        if r and any(str(v).strip() for v in r[:1]):
            print(f"   A{i}: {str(r[0])[:50]} | B: {str(r[1])[:60] if len(r)>1 else ''}")
except Exception as e:
    print("其他承攬讀取失敗：", e)
