"""唯讀盤點 2：台北期別分頁、水洗申報內容、其他承攬水洗金額位置（不寫入；不印身分證／地址欄）。"""
import os
import sys

sys.path.insert(0, os.getcwd())
from modules import auth  # noqa: E402

jgc = auth.get_jenny_gspread_client()
gc = auth.get_gspread_client()

tp = jgc.open_by_key("1PYUvL9bz3a-8f1r2WA0qEfy6O5rMKWEOytGJ-lRm9fg")
print("== 台北 期別 A1:D14")
for r in tp.worksheet("期別").get("A1:D14", value_render_option="FORMULA") or []:
    print("  ", r)
print("== 台北 清潔各月 A10:D14")
for r in tp.worksheet("清潔各月").get("A10:D14", value_render_option="FORMULA") or []:
    print("  ", [str(v)[:90] for v in r])
ws = tp.worksheet("水洗")
print("== 台北 水洗 A4:A20 與 D4:Q20（公式）")
names = ws.get("A4:A20") or []
vals = ws.get("D4:Q20", value_render_option="FORMULA") or []
for i in range(max(len(names), len(vals))):
    n = names[i][0] if i < len(names) and names[i] else ""
    v = vals[i] if i < len(vals) else []
    if n or any(str(x).strip() for x in v):
        print(f"  R{i+4} {n} |", [str(x)[:80] for x in v])

tc = gc.open_by_key("1eAt1gczhwHPOpVX2uFQumJ6xnvTWnij8N1w0IBY5qkk")
print("== 台中 清潔 A1:A6, F4:H5 公式、清潔 E 欄")
print("  ", tc.worksheet("清潔").get("D1:E5", value_render_option="FORMULA"))
print("  ", tc.worksheet("清潔").get("I1:K4", value_render_option="FORMULA"))

for label, oid in (("台中 202610-2", "11hzKVpKeFj83YChhyNHFiaVDHQ4tMxu0pp0mB_0Yneg"),
                   ("台北 202610-2", "1KDtOAumMI7kMPzw1PF6UZoYJ0wh0fmjIAXRsvqArnQA"),
                   ("台北 202610-1", "1UJX5YADf_xtQO6Pv-_Y8PYqfuCNrHdWXAJF7HleA4c0")):
    ss = gc.open_by_key(oid)
    print(f"== {label} 其他承攬")
    ws = ss.worksheet("水洗薪資表")
    print("   水洗薪資表 大小", ws.row_count, ws.col_count)
    print("   R1 C1:L1:", (ws.get("A1:L1") or [[]])[0])
    for r in range(276, 290):
        row = (ws.get(f"A{r}:L{r}", value_render_option="UNFORMATTED_VALUE") or [[]])
        row = row[0] if row else []
        if row:
            print(f"   R{r}:", [str(v)[:30] for v in row])
    for tab in ("服務薪資總表", "薪資總表", "服務薪資年度總表"):
        try:
            t = ss.worksheet(tab)
        except Exception:
            continue
        print(f"   {tab} 大小 {t.row_count}x{t.col_count}")
        for i, row in enumerate(t.get("A1:P6", value_render_option="FORMULA") or [], 1):
            print(f"     R{i}:", [str(v)[:60] for v in row])
