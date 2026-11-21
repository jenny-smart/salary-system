"""唯讀：台北水洗申報已填金額 vs 各期其他承攬 水洗薪資表 第279／285列（只印姓名與金額）。"""
import os
import sys

sys.path.insert(0, os.getcwd())
from modules import auth  # noqa: E402

OTHER = {  # 台北 其他承攬
 "202601-1":"1Oh-096BA6YlrQmMUWVEvIf1khOaltJLDVihlSiarVMs","202601-2":"18wDCMp2gL2r8PkZuw3k4FtU_hV1TMLG9ySqBo4RdQt4",
 "202602-1":"10Rq8LI4W6KorpBhYBZ4lgBItYmPjRVyLULDnuSPDUV0","202602-2":"1tS0DTOMBtfTpaAnBzrTeR9zo2GqpEu_qbN2oCbGjHsg",
 "202603-1":"1VrowA-JgopWWkVlcBHl6y6n8AETGiUCivIFQXuwQJt4","202603-2":"1Pi6a3xKBdg78WcBF6Y0NctmBgKg_FIlnuktoUo347Tc",
 "202604-1":"1e3CC_2u_MBOvoxGph3gyenCPWvumW2nSJjjBoeEQxgk","202604-2":"1yPPnSNGtWlrkPHLLMbRBlYX6ihRGTKsgBcMs0Clx5_4",
 "202605-1":"1tusqKwELM2AZCZ_zy2Ck3iKrTwFPhKwIvEzMAd4F6E4",
 "202606-1":"1x1gb1IEZyNvXSue4R8nonIART1dRPTXKi6mXw_9UHgs","202606-2":"1MjePwTbA7wWmgkaWaju2z6yWBsAFJOD1ImmUzsQOvOc",
 "202607-1":"1l4OPDog2RifPzYHftrFDb86o96EaHtg-l3rlgpDIe0s","202607-2":"1kV8xaS0krMmGCPXGXxlzJ1zBxwqdK3sNh7T9I91XMGg",
 "202608-1":"16FxRNJLCgfhJLt2qoIsLYWzJe9R-JFuUmhNKCzmwvJE","202608-2":"1kaUNPLt8Q0Xc-FUIdxhY6hmzFCuAIj8Vwk033Ixrvuo",
 "202609-1":"12nwdgbNSU29HydyI3YFO9-KR_NlIzJfx2rEiq6MKm-0","202609-2":"18DiRYUDZLncR-h79GODMCbChQdm2B2Yyq9KMhYQGhhs",
 "202610-1":"1UJX5YADf_xtQO6Pv-_Y8PYqfuCNrHdWXAJF7HleA4c0","202610-2":"1KDtOAumMI7kMPzw1PF6UZoYJ0wh0fmjIAXRsvqArnQA",
 "202611-1":"1uTLOYiwrkqM_Vz-EoM3HkVQur1j6ecVFhxl-NavUwuE",
}
gc = auth.get_gspread_client()
jgc = auth.get_jenny_gspread_client()
ws = jgc.open_by_key("1PYUvL9bz3a-8f1r2WA0qEfy6O5rMKWEOytGJ-lRm9fg").worksheet("水洗")
hdr = ws.get("A1:AZ2") or [[], []]
names = [r[0] if r else "" for r in (ws.get("A4:A30") or [])]
vals = ws.get("A4:AZ30", value_render_option="UNFORMATTED_VALUE") or []
print("台北水洗申報 R1:", [str(v)[:8] for v in hdr[0]])
print("台北水洗申報 R2:", [str(v)[:8] for v in (hdr[1] if len(hdr) > 1 else [])])
for n, row in zip(names, vals):
    if n:
        print(f"  申報已填 {n}:", ["" if v == "" else v for v in row[6:50]])

for p, oid in OTHER.items():
    try:
        sh = gc.open_by_key(oid).worksheet("水洗薪資表")
        r1 = (sh.get("J1:Z1") or [[]])[0]
        r279 = (sh.get("J279:Z279", value_render_option="UNFORMATTED_VALUE") or [[]])[0]
        r285 = (sh.get("J285:Z285", value_render_option="UNFORMATTED_VALUE") or [[]])[0]
        ad1 = (sh.spreadsheet.worksheet("水洗薪資單").get("AD1") or [[""]])[0][0]
        out = []
        for i, n in enumerate(r1):
            if not str(n).strip():
                continue
            a = r279[i] if i < len(r279) else ""
            b = r285[i] if i < len(r285) else ""
            if a not in ("", 0) or b not in ("", 0):
                out.append(f"{n} 279={a} 285={b}")
        print(f"{p}（水洗薪資單AD1={ad1}）:", "；".join(out) or "全部 0")
    except Exception as e:
        print(f"{p}: 讀取失敗 {e}")
