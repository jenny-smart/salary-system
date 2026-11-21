"""
台北／台中 202601-2～202610-2：
1. 比對其他承攬 -2 檔「薪資總表」與「「薪資總表」的副本」，不一致就重產副本
2. 申報檔「水洗各月」該月改寫為 IMPORTRANGE 公式（讀副本）
3. 嘗試開啟 IMPORTRANGE 權限，回報各月是否還需手動「允許存取」
"""
import os
import sys
import time
from datetime import datetime

sys.path.insert(0, os.getcwd())
import requests  # noqa: E402
from google.auth.transport.requests import Request  # noqa: E402
from modules import auth  # noqa: E402
from modules import contract_report as cr  # noqa: E402
from modules.other_contract_process import _find_other_file  # noqa: E402
from modules.year_root import cfg_for_period  # noqa: E402

gc = auth.get_gspread_client()
jgc = auth.get_jenny_gspread_client()
reg_rows = gc.open_by_key("1GdW3FSZ0s3TGeYiNx3JtYvED_RRfJjiFYwLFeYHZ1hA").worksheet("地區設定").get_all_values()
cfgs = {r[0]: dict(zip(reg_rows[0], r + [""] * 20)) for r in reg_rows[1:]}

jdrive = auth.get_jenny_drive_service()
stamp = datetime.now().strftime("%Y%m%d-%H%M")
for region in ():
    rid = cfgs[region]["contract_report_id"]
    meta = jdrive.files().get(fileId=rid, fields="name,parents", supportsAllDrives=True).execute()
    cp = jdrive.files().copy(fileId=rid, supportsAllDrives=True, body={
        "name": f"[備份{stamp}]{meta['name']}", "parents": meta.get("parents", [])}).execute()
    print(f"[備份] {region} → {cp['id']}")

creds = auth.get_jenny_credentials()
creds.refresh(Request())
for region in ("台北", "台中"):
    print(f"\n===== {region} =====")
    report = jgc.open_by_key(cfgs[region]["contract_report_id"])
    ws_w = report.worksheet(cr.WASH_MONTHLY)
    width = cr.block_width((report.worksheet(cr.CLEAN_MONTHLY).get("A1:BZ1") or [[]])[0])
    for m in range(1, 11):
        period = f"2026{m:02d}-2"
        cfg = cfg_for_period(cfgs[region], period)
        try:
            oid = _find_other_file(cfg["root_folder_id"], period, region)
        except Exception as e:
            print(f"{period}：找不到其他承攬檔，略過（{str(e)[:30]}）")
            continue
        oss = jgc.open_by_key(oid)
        live = oss.worksheet(cr.OTHER_SRC).get("A1:Z100", value_render_option="UNFORMATTED_VALUE")
        try:
            copy = oss.worksheet(cr.OTHER_COPY).get("A1:Z100", value_render_option="UNFORMATTED_VALUE")
        except Exception:
            copy = None
        if copy is not None:  # 本次只重寫公式（副本剛比對／重產過）
            status = "副本一致"
        else:
            cr.snapshot_sheet(oss, cr.OTHER_SRC, cr.OTHER_COPY)
            status = "副本不存在→已產生" if copy is None else "副本不一致→已重產"
            # 列出水洗人員差異
            old = {r[0]: r[1:] for r in cr.wash_person_rows(copy or [], 3)}
            new = {r[0]: r[1:] for r in cr.wash_person_rows(live, 3)}
            for n in sorted(set(old) | set(new)):
                if old.get(n) != new.get(n):
                    status += f"｜{n} {old.get(n)}→{new.get(n)}"
        c0 = (m - 1) * width + 1
        cur = (ws_w.get(f"{cr.col_letter(c0 + 1)}1:{cr.col_letter(c0 + 2)}1") or [["", ""]])[0] + ["", ""]
        cr.write_wash_formula_block(ws_w, m, width, cr.month_labels(period, tuple(cur[:2])))
        url = (f"https://docs.google.com/spreadsheets/d/{report.id}/externaldata/addimportrangepermissions"
               f"?donorDoc={oid}")
        code = requests.post(url, headers={"Authorization": f"Bearer {creds.token}"}).status_code
        print(f"{period}：{status}；水洗各月公式已寫入（權限 API {code}）")
    time.sleep(8)
    vals = ws_w.get("A1:AT12") or []
    print("  寫入後檢查：")
    for m in range(1, 11):
        c = (m - 1) * width
        r2 = (list(vals[1]) + [""] * 60)[c + 1:c + 3] if len(vals) > 1 else ["", ""]
        r6 = (list(vals[5]) + [""] * 60)[c:c + 3] if len(vals) > 5 else ["", "", ""]
        cell = f"{cr.col_letter(c + 2)}2"
        flag = f"⚠️ 需按允許存取（{cell}）" if any("#REF" in str(v) for v in r2 + r6) else "✅"
        print(f"   {m}月 {flag} 合計={r2} 第6列={r6[:1]}")
