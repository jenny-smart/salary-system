"""唯讀：比對清潔各月 ⑥ 寫入前（備份）與寫入後；台中另與來源場次時數薪資總表比對。"""
import os
import sys

sys.path.insert(0, os.getcwd())
from modules import auth  # noqa: E402

jgc = auth.get_jenny_gspread_client()
TP_NOW, TP_BAK = "1PYUvL9bz3a-8f1r2WA0qEfy6O5rMKWEOytGJ-lRm9fg", "1AP-Bt4jCVEaYAzeSAWndeDZGp4ZX6QvA773UKWFnzNw"
TC_NOW = "1eAt1gczhwHPOpVX2uFQumJ6xnvTWnij8N1w0IBY5qkk"


def num(v):
    try:
        return round(float(str(v).replace(",", "")), 2) if str(v).strip() not in ("",) else 0.0
    except ValueError:
        return str(v)


def blocks(vals, width, months=10):
    out = {}
    for m in range(1, months + 1):
        c = (m - 1) * width
        row2 = (list(vals[1]) + [""] * 80) if len(vals) > 1 else [""] * 80
        out[(m, "合計")] = [num(row2[c + 1]), num(row2[c + 2])]
        for r in vals[5:]:
            r = list(r) + [""] * 80
            n = str(r[c]).strip()
            if n:
                out[(m, n)] = [num(r[c + k]) for k in range(1, width)]
    return out


def compare(label, a, b):
    diff = []
    for k in sorted(set(a) | set(b), key=lambda x: (x[0], str(x[1]))):
        if a.get(k) != b.get(k):
            diff.append((k, a.get(k), b.get(k)))
    by_month = {}
    for (m, n), x, y in diff:
        by_month.setdefault(m, []).append((n, x, y))
    total = len({k for k in a} | {k for k in b})
    print(f"\n== {label}：共 {total} 項，不同 {len(diff)} 項")
    for m in sorted(by_month):
        items = by_month[m]
        print(f"  {m}月 不同 {len(items)} 項：")
        for n, x, y in items[:12]:
            print(f"     {n}：前 {x} → 後 {y}")


bak = jgc.open_by_key(TP_BAK).worksheet("清潔各月").get("A1:AT400", value_render_option="UNFORMATTED_VALUE") or []
now = jgc.open_by_key(TP_NOW).worksheet("清潔各月").get("A1:AT400", value_render_option="UNFORMATTED_VALUE") or []
errs = sum(1 for r in bak for v in r if str(v).startswith("#"))
print(f"台北備份檔錯誤格數：{errs}")
compare("台北 清潔各月（寫入前備份 vs 現在）", blocks(bak, 4), blocks(now, 4))

# 台北 清潔分頁每月合計
for label, fid in (("備份", TP_BAK), ("現在", TP_NOW)):
    r3 = (jgc.open_by_key(fid).worksheet("清潔").get("H3:AU3", value_render_option="UNFORMATTED_VALUE") or [[]])[0]
    print(f"台北 清潔 第3列 {label}：", [num(v) for v in r3])

# 台中：現在 vs 來源
tc = jgc.open_by_key(TC_NOW)
now_tc = blocks(tc.worksheet("清潔各月").get("A1:AH400", value_render_option="UNFORMATTED_VALUE") or [], 3)
ids = tc.worksheet("期別").get("B2:B11") or []
src = {}
for m, row in enumerate(ids, 1):
    v = jgc.open_by_key(row[0]).worksheet("場次時數薪資總表").get("A1:F200", value_render_option="UNFORMATTED_VALUE") or []
    src[(m, "合計")] = [num((list(v[2]) + [""] * 6)[3]), num((list(v[2]) + [""] * 6)[4])]
    for r in v[3:]:
        r = list(r) + [""] * 6
        if str(r[0]).strip() and str(r[0]).strip() != "0":
            src[(m, str(r[0]).strip())] = [num(r[3]), num(r[4])]
compare("台中 清潔各月（現在 vs 來源場次時數薪資總表）", src, now_tc)
