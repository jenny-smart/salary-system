"""套用新版承攬費申報（台北／台中 202601-2～202610-2）；先備份申報檔、補台中 contract_report_id。"""
import os
import sys
from datetime import datetime

sys.path.insert(0, os.getcwd())
from modules import auth  # noqa: E402
from modules.contract_report import run_contract_report_all  # noqa: E402
from modules.year_root import cfg_for_period  # noqa: E402

MASTER = "1GdW3FSZ0s3TGeYiNx3JtYvED_RRfJjiFYwLFeYHZ1hA"
TC_REPORT = "1eAt1gczhwHPOpVX2uFQumJ6xnvTWnij8N1w0IBY5qkk"
gc = auth.get_gspread_client()
reg = gc.open_by_key(MASTER).worksheet("地區設定")
rows = reg.get_all_values()
hdr = rows[0]
cfgs = {}
for i, r in enumerate(rows[1:], 2):
    d = dict(zip(hdr, r + [""] * len(hdr)))
    if d["name"] == "台中" and not d.get("contract_report_id"):
        col = hdr.index("contract_report_id") + 1
        reg.update_cell(i, col, TC_REPORT)
        d["contract_report_id"] = TC_REPORT
        print(f"地區設定：台中 contract_report_id 已填入（第{i}列）")
    cfgs[d["name"]] = d

jdrive = auth.get_jenny_drive_service()
stamp = datetime.now().strftime("%Y%m%d-%H%M")
for region in ("台北", "台中"):
    rid = cfgs[region]["contract_report_id"]
    meta = jdrive.files().get(fileId=rid, fields="name,parents", supportsAllDrives=True).execute()
    cp = jdrive.files().copy(fileId=rid, supportsAllDrives=True, body={
        "name": f"[備份{stamp}]{meta['name']}", "parents": meta.get("parents", [])}).execute()
    print(f"[備份] {region} → {cp['id']}")

ok = True
for region in ("台北", "台中"):
    for m in range(1, 11):
        period = f"2026{m:02d}-2"
        cfg = cfg_for_period(cfgs[region], period)
        try:
            if not run_contract_report_all(region, period, cfg["root_folder_id"], cfg, print):
                ok = False
        except Exception as e:
            ok = False
            print(f"❌ {region} {period} 失敗：{e}")
print("全部完成" if ok else "有失敗項目")
