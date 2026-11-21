"""
清除其他承攬水洗殘留訂單（台北 202607-1；台中 202601-1～202607-2），
清除前把整列存到該檔「已清除殘留訂單」分頁；再把 -2 檔第284/280列依清理後的 -1 檔第285/279列重帶，
最後重跑 ⑥。不印客戶資料。
"""
import calendar
import os
import re
import sys
import time
from datetime import date, datetime, timedelta

sys.path.insert(0, os.getcwd())
from modules import auth  # noqa: E402
from modules.contract_report import run_contract_report_all  # noqa: E402
from modules.other_contract_process import _find_other_file  # noqa: E402
from modules.year_root import cfg_for_period  # noqa: E402

gc = auth.get_gspread_client()
jgc = auth.get_jenny_gspread_client()
reg_rows = gc.open_by_key("1GdW3FSZ0s3TGeYiNx3JtYvED_RRfJjiFYwLFeYHZ1hA").worksheet("地區設定").get_all_values()
cfgs = {r[0]: dict(zip(reg_rows[0], r + [""] * 20)) for r in reg_rows[1:]}
TARGETS = {"台北": ["202607-1"],
           "台中": [f"2026{m:02d}-{h}" for m in range(1, 8) for h in (1, 2)]}
STAMP = datetime.now().strftime("%Y/%m/%d %H:%M")


def to_date(v, year):
    if isinstance(v, (int, float)) and 30000 < v < 60000:
        return date(1899, 12, 30) + timedelta(days=int(v))
    s = str(v or "").strip()
    try:
        m = re.match(r"(\d{4})[/.-](\d{1,2})[/.-](\d{1,2})", s)
        if m:
            return date(int(m[1]), int(m[2]), int(m[3]))
        m = re.match(r"(\d{1,2})[/.-](\d{1,2})", s)
        if m:
            return date(year, int(m[1]), int(m[2]))
    except ValueError:
        return None
    return None


def record_sheet(ss):
    try:
        return ss.worksheet("已清除殘留訂單")
    except Exception:
        ws = ss.add_worksheet(title="已清除殘留訂單", rows=200, cols=70)
        ws.update(values=[["清除時間", "分頁", "原列號／說明", "原內容（A 欄起）"]], range_name="A1")
        return ws


def open_other(region, p):
    cfg = cfg_for_period(cfgs[region], p)
    return jgc.open_by_key(_find_other_file(cfg["root_folder_id"], p, region))


for region, periods in TARGETS.items():
    print(f"\n===== {region} 清除殘留 =====")
    for p in periods:
        y, m, half = int(p[:4]), int(p[4:6]), p[-1]
        lo = date(y, m, 1)
        hi = date(y, m, 15 if half == "1" else calendar.monthrange(y, m)[1])
        ss = open_other(region, p)
        titles = [w.title for w in ss.worksheets()]
        tab = "水洗訂單" if "水洗訂單" in titles else "水洗營收明細"
        ws = ss.worksheet(tab)
        vals = ws.get("A1:BJ600", value_render_option="FORMULA") or []
        raw = ws.get("A1:BJ600", value_render_option="UNFORMATTED_VALUE") or []
        hrow = next((i for i, r in enumerate(raw[:10]) if any("服務日期" in str(x) for x in r)), None)
        if hrow is None:
            print(f"{p} {tab}：沒有表頭，略過")
            continue
        dcol = next(i for i, h in enumerate(raw[hrow]) if "服務日期" in str(h))
        bad = []
        for i in range(hrow + 1, len(raw)):
            r = raw[i]
            d = to_date(r[dcol], y) if len(r) > dcol else None
            if d and not (lo <= d <= hi):
                bad.append((i + 1, d))
        if not bad:
            print(f"{p} {tab}：沒有殘留")
            continue
        rec = record_sheet(ss)
        rec.append_rows([[STAMP, tab, f"第{r}列（服務日期 {d}）"] + list(vals[r - 1]) for r, d in bad],
                        value_input_option="RAW")
        ws.batch_clear([f"A{r}:BJ{r}" for r, _ in bad])
        print(f"{p} {tab}：清除 {len(bad)} 列（服務日期 {', '.join(str(d) for _, d in bad)}），已存到「已清除殘留訂單」")
        time.sleep(1)

print("\n===== 下半月檔第284/280列依清理後上半月重帶 =====")
for region, periods in TARGETS.items():
    for p in sorted({x[:6] for x in periods}):
        s1, s2 = open_other(region, f"{p}-1"), open_other(region, f"{p}-2")
        t1, t2 = s1.worksheet("水洗薪資表"), s2.worksheet("水洗薪資表")
        time.sleep(3)
        new285 = t1.get("J285:O285", value_render_option="UNFORMATTED_VALUE") or [[]]
        new279 = t1.get("J279:O279", value_render_option="UNFORMATTED_VALUE") or [[]]
        old284 = t2.get("J284:O284", value_render_option="UNFORMATTED_VALUE") or [[]]
        old280 = t2.get("J280:O280", value_render_option="UNFORMATTED_VALUE") or [[]]
        record_sheet(s2).append_rows([
            [STAMP, "水洗薪資表", "第284列（重帶前）"] + list(old284[0]),
            [STAMP, "水洗薪資表", "第280列（重帶前）"] + list(old280[0]),
        ], value_input_option="RAW")
        t2.update(values=new285, range_name="J284:O284", value_input_option="RAW")
        t2.update(values=new279, range_name="J280:O280", value_input_option="RAW")
        print(f"{region} {p}-2：284 {old284[0]} → {new285[0]}；280 {old280[0]} → {new279[0]}")

print("\n===== 重跑 ⑥ =====")
for region, periods in TARGETS.items():
    for p in sorted({x[:6] for x in periods}):
        period = f"{p}-2"
        cfg = cfg_for_period(cfgs[region], period)
        run_contract_report_all(region, period, cfg["root_folder_id"], cfg,
                                lambda msg: print(msg) if ("水洗各月" in msg or "❌" in msg) else None)

print("\n===== 申報檔「水洗」分頁 =====")
for region, rows in (("台北", "A4:AH5"), ("台中", "A4:S4")):
    w = jgc.open_by_key(cfgs[region]["contract_report_id"]).worksheet("水洗")
    for r in w.get(rows) or []:
        print(f"  {region} {r[0]}：", [v for v in r[5:]])
