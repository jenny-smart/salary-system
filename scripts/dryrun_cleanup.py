"""補做上半月前置的「清空第280/284列」（台北 202607-1；台中 202601-1～202607-1），重帶 -2 的 284/280，重跑 ⑥；台北水洗 7月第一階段改回公式。"""
import os
import sys
import time
from datetime import datetime

sys.path.insert(0, os.getcwd())
from modules import auth  # noqa: E402
from modules.contract_report import run_contract_report_all  # noqa: E402
from modules.other_contract_process import _find_other_file  # noqa: E402
from modules.year_root import cfg_for_period  # noqa: E402

gc = auth.get_gspread_client()
jgc = auth.get_jenny_gspread_client()
reg_rows = gc.open_by_key("1GdW3FSZ0s3TGeYiNx3JtYvED_RRfJjiFYwLFeYHZ1hA").worksheet("地區設定").get_all_values()
cfgs = {r[0]: dict(zip(reg_rows[0], r + [""] * 20)) for r in reg_rows[1:]}
MONTHS = {"台北": ["202607"], "台中": [f"2026{m:02d}" for m in range(1, 8)]}
STAMP = datetime.now().strftime("%Y/%m/%d %H:%M")


def open_other(region, p):
    cfg = cfg_for_period(cfgs[region], p)
    return jgc.open_by_key(_find_other_file(cfg["root_folder_id"], p, region))


changed = []
for region, months in MONTHS.items():
    for ym in months:
        s1, s2 = open_other(region, f"{ym}-1"), open_other(region, f"{ym}-2")
        t1, t2 = s1.worksheet("水洗薪資表"), s2.worksheet("水洗薪資表")
        old = t1.get("J280:O284", value_render_option="UNFORMATTED_VALUE") or []
        s1.worksheet("已清除殘留訂單").append_rows(
            [[STAMP, "水洗薪資表", f"第{280 + i}列（補做上半月清空前）"] + list(r) for i, r in enumerate(old)],
            value_input_option="RAW")
        t1.batch_clear(["J280:O280", "J284:O284"])
        time.sleep(3)
        new285 = t1.get("J285:O285", value_render_option="UNFORMATTED_VALUE") or [[]]
        new279 = t1.get("J279:O279", value_render_option="UNFORMATTED_VALUE") or [[]]
        o284 = t2.get("J284:O284", value_render_option="UNFORMATTED_VALUE") or [[]]
        s2.worksheet("已清除殘留訂單").append_rows(
            [[STAMP, "水洗薪資表", "第284列（重帶前）"] + list(o284[0])], value_input_option="RAW")
        t2.update(values=new285, range_name="J284:O284", value_input_option="RAW")
        t2.update(values=new279, range_name="J280:O280", value_input_option="RAW")
        print(f"{region} {ym}：-1 第284列 {old[4] if len(old) > 4 else []} → 清空；-1 第285列 {new285[0]}；-2 第284列 {o284[0]} → {new285[0]}")
        if o284[0] != new285[0]:
            changed.append((region, ym))

for region, ym in sorted(set(changed) | {("台北", "202607")}):
    cfg = cfg_for_period(cfgs[region], f"{ym}-2")
    run_contract_report_all(region, f"{ym}-2", cfg["root_folder_id"], cfg,
                            lambda m: print(m) if ("水洗各月" in m or "❌" in m) else None)

w = jgc.open_by_key(cfgs["台北"]["contract_report_id"]).worksheet("水洗")
if str(w.acell("A4").value).strip() == "洪暐智" and str(w.acell("AE4", value_render_option="UNFORMATTED_VALUE").value) in ("3150", "3150.0"):
    w.update(values=[['=IFNA(VLOOKUP($A4,\'水洗各月\'!$Y:$AA,2,FALSE),"")']], range_name="AE4", value_input_option="USER_ENTERED")
    print("台北水洗 AE4（洪暐智 7月第一階段）改回公式")
time.sleep(3)
for region, rng in (("台北", "A4:AH5"), ("台中", "A4:S4")):
    ws = jgc.open_by_key(cfgs[region]["contract_report_id"]).worksheet("水洗")
    for r in ws.get(rng) or []:
        print(f"  {region} {r[0]}：", r[5:])
