"""唯讀：台北／台中 2026 各期其他承攬檔，檢查水洗訂單服務日期是否落在該期範圍（找前期未刪乾淨的訂單）。"""
import calendar
import os
import re
import sys
from datetime import date, datetime, timedelta

sys.path.insert(0, os.getcwd())
from modules import auth  # noqa: E402
from modules.other_contract_process import _find_other_file  # noqa: E402

ROOT = {"台北": "1kJTGZYpgXBPlyR4SjRKoP0-sK30MhAfW", "台中": "1peMrCBDFJ2-sSKUzuc0X5p0Mc3g0VPrG"}
PERIODS = [f"2026{m:02d}-{h}" for m in range(1, 12) for h in (1, 2)][:-1]
jgc = auth.get_jenny_gspread_client()


def to_date(v):
    if isinstance(v, (int, float)) and 30000 < v < 60000:
        return date(1899, 12, 30) + timedelta(days=int(v))
    s = str(v or "").strip()
    m = re.match(r"(\d{4})[/.-](\d{1,2})[/.-](\d{1,2})", s)
    if m:
        try:
            return date(int(m[1]), int(m[2]), int(m[3]))
        except ValueError:
            return None
    return None


for region, root in ROOT.items():
    print(f"\n===== {region} =====")
    for p in PERIODS:
        y, m, half = int(p[:4]), int(p[4:6]), p[-1]
        start = date(y, m, 1 if half == "1" else 16)
        end = date(y, m, 15 if half == "1" else calendar.monthrange(y, m)[1])
        try:
            ss = jgc.open_by_key(_find_other_file(root, p, region))
        except Exception as e:
            print(f"{p}：找不到其他承攬檔（{str(e)[:40]}）")
            continue
        titles = [w.title for w in ss.worksheets()]
        order_tabs = [t for t in titles if "水洗" in t and "訂單" in t] or [t for t in titles if t.endswith("訂單")]
        try:
            ad1 = (ss.worksheet("水洗薪資單").get("AD1") or [[""]])[0][0]
        except Exception:
            ad1 = "（無水洗薪資單）"
        if not order_tabs:
            print(f"{p}：沒有訂單分頁（分頁：{titles[:8]}）AD1={ad1}")
            continue
        for tab in order_tabs[:2]:
            vals = ss.worksheet(tab).get("A1:Z600", value_render_option="UNFORMATTED_VALUE") or []
            if not vals:
                print(f"{p} {tab}：空白 AD1={ad1}")
                continue
            hdr = [str(h) for h in vals[0]]
            cand = [i for i, h in enumerate(hdr) if "日期" in h] or list(range(len(hdr)))
            best, best_n = None, 0
            for i in cand:
                n = sum(1 for r in vals[1:] if len(r) > i and to_date(r[i]))
                if n > best_n:
                    best, best_n = i, n
            if best is None:
                print(f"{p} {tab}：找不到日期欄 表頭={hdr[:10]} AD1={ad1}")
                continue
            dates = [to_date(r[best]) for r in vals[1:] if len(r) > best and to_date(r[best])]
            out = sorted({d for d in dates if not (start <= d <= end)})
            n_out = sum(1 for d in dates if not (start <= d <= end))
            months = sorted({d.strftime("%Y-%m") + ("上" if d.day <= 15 else "下") for d in dates if not (start <= d <= end)})
            flag = "⚠️ " if n_out else "✅ "
            print(f"{flag}{p} {tab}（日期欄「{hdr[best]}」）：{len(dates)} 筆，期外 {n_out} 筆"
                  + (f"（{out[0]}～{out[-1]}，{'、'.join(months)}）" if n_out else "") + f"｜AD1={ad1}")
