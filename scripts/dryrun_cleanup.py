"""
申報檔「水洗」分頁改用公式，從「水洗各月」帶出（同「清潔」分頁的 VLOOKUP 做法）。WRITE=1 才寫入。
- 每月「第一階段」「第二階段」欄：=IFNA(VLOOKUP($A列,'水洗各月'!該月區塊,2/3,FALSE),"")
- 已有手填數字且與水洗各月不同的格子保留手填值，列出待確認
- 水洗各月有金額、水洗分頁還沒有的人，新增在最後一列
不印身分證、地址。
"""
import os
import sys
from datetime import datetime

sys.path.insert(0, os.getcwd())
from modules import auth  # noqa: E402
from modules.contract_report import block_width, col_letter  # noqa: E402

WRITE = os.environ.get("WRITE") == "1"
REPORT = {"台北": "1PYUvL9bz3a-8f1r2WA0qEfy6O5rMKWEOytGJ-lRm9fg",
          "台中": "1eAt1gczhwHPOpVX2uFQumJ6xnvTWnij8N1w0IBY5qkk"}
START = 4
jgc = auth.get_jenny_gspread_client()
print("模式：", "寫入" if WRITE else "預演（不寫入）")


def num(v):
    try:
        return float(str(v).replace(",", "")) if str(v).strip() != "" else None
    except ValueError:
        return None


if WRITE:
    jdrive = auth.get_jenny_drive_service()
    stamp = datetime.now().strftime("%Y%m%d-%H%M")
    for region, rid in REPORT.items():
        meta = jdrive.files().get(fileId=rid, fields="name,parents", supportsAllDrives=True).execute()
        cp = jdrive.files().copy(fileId=rid, supportsAllDrives=True, body={
            "name": f"[備份{stamp}]{meta['name']}", "parents": meta.get("parents", [])}).execute()
        print(f"[備份] {region} → {cp['id']}")


# ── 修正補上的「薪資總表」E1/F1（舊檔沒有「水洗訂單」或 AD1 寫法不同），並重跑 ⑥ ──
import re as _re  # noqa: E402
from modules.contract_report import run_contract_report_all  # noqa: E402
from modules.other_contract_process import _find_other_file  # noqa: E402
from modules.year_root import cfg_for_period  # noqa: E402

gc = auth.get_gspread_client()
reg_rows = gc.open_by_key("1GdW3FSZ0s3TGeYiNx3JtYvED_RRfJjiFYwLFeYHZ1hA").worksheet("地區設定").get_all_values()
cfgs = {r[0]: dict(zip(reg_rows[0], r + [""] * 20)) for r in reg_rows[1:]}


def norm(v):
    return _re.sub(r"[^0-9-]", "", str(v or ""))


for region in REPORT:
    for m in range(1, 11):
        period = f"2026{m:02d}-2"
        cfg = cfg_for_period(cfgs[region], period)
        changed = False
        for half in ("1", "2"):
            p = f"2026{m:02d}-{half}"
            try:
                oss = jgc.open_by_key(_find_other_file(cfg["root_folder_id"], p, region))
            except Exception:
                continue
            titles = [t.title for t in oss.worksheets()]
            if "薪資總表" not in titles:
                continue
            ws = oss.worksheet("薪資總表")
            e1f1 = (ws.get("E1:F1") or [["", ""]])[0] + ["", ""]
            ad1 = str((oss.worksheet("水洗薪資單").get("AD1") or [[""]])[0][0]).strip()
            want = []
            for k, h in enumerate(("1", "2")):
                target = f"2026{m:02d}-{h}"
                want.append(ad1 if norm(ad1) == target else target)
            bad = any("#" in str(x) for x in e1f1) or "水洗訂單" not in titles
            if bad or (norm(ad1) in (f"2026{m:02d}-1", f"2026{m:02d}-2") and ad1 not in e1f1):
                print(f"[薪資總表] {region} {p} E1/F1 {e1f1[:2]} → {want}（AD1={ad1}）")
                if WRITE:
                    ws.update(values=[want], range_name="E1:F1", value_input_option="RAW")
                changed = True
        if changed and WRITE:
            run_contract_report_all(region, period, cfg["root_folder_id"], cfg, print)

for region, rid in REPORT.items():
    ss = jgc.open_by_key(rid)
    wm = ss.worksheet("水洗各月")
    wm_vals = wm.get("A1:BZ200", value_render_option="UNFORMATTED_VALUE") or []
    width = block_width(wm_vals[0] if wm_vals else [])
    # 水洗各月：{(姓名, 月): (第一, 第二)}
    monthly = {}
    for m in range(1, 13):
        c = (m - 1) * width
        for row in wm_vals[5:]:
            row = list(row) + [""] * (c + 3)
            name = str(row[c]).strip()
            if name:
                monthly[(name, m)] = (row[c + 1], row[c + 2])

    w = ss.worksheet("水洗")
    hdr2 = (w.get("A2:BZ2") or [[]])[0]
    stage1_cols = [i for i, v in enumerate(hdr2) if str(v).strip() == "第一階段"][:12]
    print(f"\n===== {region}：水洗各月區塊寬 {width}，水洗分頁 {len(stage1_cols)} 個月（第一階段欄 "
          f"{', '.join(col_letter(i + 1) for i in stage1_cols)}）")
    names = [str(r[0]).strip() if r else "" for r in (w.get(f"A{START}:A300") or [])]
    row_of = {n: START + i for i, n in enumerate(names) if n}
    last = START + max([i for i, n in enumerate(names) if n], default=-1)
    has_amount = sorted({n for (n, _), v in monthly.items() if any(num(x) for x in v)}
                        | ({"陳信全"} if region == "台中" else set()))
    data = []
    for n in has_amount:
        if n not in row_of:
            last += 1
            row_of[n] = last
            data.append({"range": f"A{last}", "values": [[n]]})
            print(f"  新增人員 {n} → 第{last}列")
    if not row_of:
        print("  沒有任何人員，略過")
        continue
    bot = max(row_of.values())
    cur = w.get(f"A{START}:BZ{bot}", value_render_option="UNFORMATTED_VALUE") or []
    cur = {START + i: list(r) + [""] * 80 for i, r in enumerate(cur)}
    pending, n_formula = [], 0
    for name, r in sorted(row_of.items(), key=lambda x: x[1]):
        for m, c in enumerate(stage1_cols, 1):
            b0 = (m - 1) * width
            rng = f"'水洗各月'!${col_letter(b0 + 1)}:${col_letter(b0 + 3)}"
            calc = monthly.get((name, m), ("", ""))
            for k in (0, 1):
                col = c + k
                old = cur.get(r, [""] * 80)[col]
                new_v = calc[k]
                if num(old) not in (None, 0) and num(old) != num(new_v):
                    pending.append(f"{name} {m}月{'第一' if k == 0 else '第二'}階段 {col_letter(col + 1)}{r}："
                                   f"手填 {old}，水洗各月 {new_v if new_v != '' else '空白'} → 保留手填")
                    continue
                f = f'=IFNA(VLOOKUP($A{r},{rng},{2 + k},FALSE),"")'
                data.append({"range": f"{col_letter(col + 1)}{r}", "values": [[f]]})
                n_formula += 1
    print(f"  將寫入公式 {n_formula} 格（{len(row_of)} 人 × {len(stage1_cols)} 月 × 2 階段，扣除保留）")
    print("  範例：", data[-1] if data else "")
    for p in pending:
        print("  待確認：" + p)
    if WRITE and data:
        w.batch_update(data, value_input_option="USER_ENTERED")
        print(f"  已寫入 {len(data)} 格")
print("完成")
