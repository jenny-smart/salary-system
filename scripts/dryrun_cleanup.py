"""重跑 ⑥（台北／台中 202601-2～202610-2）：重產清潔與其他承攬副本，清潔各月／水洗各月改回 Python 寫值。"""
import os
import sys
from datetime import datetime

sys.path.insert(0, os.getcwd())
from modules import auth  # noqa: E402
from modules.contract_report import run_contract_report_all  # noqa: E402
from modules.year_root import cfg_for_period  # noqa: E402

gc = auth.get_gspread_client()
jgc = auth.get_jenny_gspread_client()
reg_rows = gc.open_by_key("1GdW3FSZ0s3TGeYiNx3JtYvED_RRfJjiFYwLFeYHZ1hA").worksheet("地區設定").get_all_values()
cfgs = {r[0]: dict(zip(reg_rows[0], r + [""] * 20)) for r in reg_rows[1:]}
jdrive = auth.get_jenny_drive_service()
stamp = datetime.now().strftime("%Y%m%d-%H%M")
for region in ("台北", "台中"):
    rid = cfgs[region]["contract_report_id"]
    meta = jdrive.files().get(fileId=rid, fields="name,parents", supportsAllDrives=True).execute()
    cp = jdrive.files().copy(fileId=rid, supportsAllDrives=True, body={
        "name": f"[備份{stamp}]{meta['name']}", "parents": meta.get("parents", [])}).execute()
    print(f"[備份] {region} → {cp['id']}")

for region in ("台北", "台中"):
    for m in range(1, 11):
        period = f"2026{m:02d}-2"
        cfg = cfg_for_period(cfgs[region], period)
        try:
            run_contract_report_all(region, period, cfg["root_folder_id"], cfg, print)
        except Exception as e:
            print(f"❌ {region} {period}：{e}")

for region in ("台北", "台中"):
    ss = jgc.open_by_key(cfgs[region]["contract_report_id"])
    w = ss.worksheet("水洗各月").get("A1:AT8") or []
    refs = sum(1 for r in w for v in r if "#REF" in str(v))
    r3 = (ss.worksheet("清潔").get("H3:AU3") or [[]])[0]
    print(f"\n{region}：水洗各月 #REF 格數 {refs}；清潔 第3列 {r3}")
