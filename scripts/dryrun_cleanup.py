"""唯讀：台北申報檔清潔各月／清潔分頁公式、清潔承攬檔副本分頁名稱（不印個資欄）。"""
import os
import sys

sys.path.insert(0, os.getcwd())
from modules import auth  # noqa: E402

jgc = auth.get_jenny_gspread_client()
gc = auth.get_gspread_client()
tp = jgc.open_by_key("1PYUvL9bz3a-8f1r2WA0qEfy6O5rMKWEOytGJ-lRm9fg")
w = tp.worksheet("清潔各月")
for r, row in enumerate(w.get("A4:H7", value_render_option="FORMULA") or [], 4):
    print(f"清潔各月 R{r}:", [str(v)[:160] for v in row])
c = tp.worksheet("清潔")
print("清潔 R1-3 D:N:", c.get("D1:N3", value_render_option="FORMULA"))
print("清潔 R4 D:N:", [str(v)[:120] for v in (c.get("D4:N4", value_render_option="FORMULA") or [[]])[0]])
print("期別 C1:E3:", tp.worksheet("期別").get("C1:E3", value_render_option="FORMULA"))
print("申報檔分頁大小：", [(x.title, x.row_count, x.col_count) for x in tp.worksheets()])
for label, fid in (("台北 202610-2 清潔", "1Ng8MIA-ECo1eH-_PmiJBciJetgsSLENQscLD_w37ENw"),
                   ("台中 202610-2 清潔", "19ZD_cT-0sfGn-kaK-Sy2r_BSmmwBK_QTWlMH3ZLGlPY")):
    ss = gc.open_by_key(fid)
    names = [x.title for x in ss.worksheets()]
    print(label, "含副本/值分頁：", [n for n in names if "場次時數薪資總表" in n])
    t = ss.worksheet("場次時數薪資總表")
    print("   場次時數薪資總表 A1:F4 公式：", [[str(v)[:60] for v in r] for r in (t.get("A1:F4", value_render_option="FORMULA") or [])])
s = jgc.open_by_key("1AvoUGlFCxaKsPOPavgBmqHoyn-_t4c0I82bIth7SvRQ").worksheet("新訓退訓實境費用")
print("新訓退訓實境費用 大小", s.row_count, s.col_count, "已用列數", len(s.col_values(1)))
