"""薪資表範本試跑（只寫入複本）：回歸比對 + 大掃除情境檢查。"""
from __future__ import annotations

import os
import sys
import time
import traceback
from datetime import datetime

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from modules import auth, master_sheet  # noqa: E402
from modules import cleaning_process_1 as cp1  # noqa: E402
from modules import cleaning_process_3 as cp3  # noqa: E402
from modules import import_engine as engine  # noqa: E402

REGION, PERIOD = "台北", "202611-1"
SRC = os.environ["DRYRUN_CLEANING_ID"]


def say(m=""):
    print(m, flush=True)


for mod in (master_sheet, cp1, cp3):
    for n in ("record_execution", "record_batch"):
        if hasattr(mod, n):
            setattr(mod, n, lambda *a, **k: None)

drive = auth.get_jenny_drive_service()
gc = auth.get_gspread_client()
sa = auth._load_service_account_info()["client_email"]
folder = drive.files().create(body={"name": f"薪資表範本試跑-{datetime.now():%Y%m%d-%H%M}",
                                    "mimeType": "application/vnd.google-apps.folder"},
                              fields="id").execute()["id"]
drive.permissions().create(fileId=folder, body={"type": "user", "role": "writer", "emailAddress": sa},
                           sendNotificationEmail=False).execute()
say(f"測試資料夾：https://drive.google.com/drive/folders/{folder}")


def copy(name):
    return drive.files().copy(fileId=SRC, body={"name": name, "parents": [folder]}, fields="id").execute()["id"]


def run(copy_id, label, dates=None):
    ss = gc.open_by_key(copy_id)
    ws = ss.worksheet("薪資表")
    if dates:
        ws.update(values=[[d] for d in dates], range_name="B2006:B2008", value_input_option="USER_ENTERED")
        say(f"- {label}：B2006～B2008 改為 {', '.join(dates)}")
    log = []
    try:
        cp1._prep_step1_salary_rows(ws, True, log, ss.worksheet("專案薪資表"))
        cp3.run_settlement(cleaning_file_id=copy_id, region=REGION, period=PERIOD,
                           is_first_half=True, log=log, region_cfg={"name": REGION})
    except Exception as e:
        log.append(f"❌ {e}\n{traceback.format_exc()}")
    for line in log:
        if any(k in line for k in ("範本", "⚠", "❌", "✅", "非0")):
            say(f"    {line.strip()}")
    time.sleep(3)
    return ss


# ── 1. 回歸：日期不變，與原檔逐格比對 ─────────────────────────
say("\n### 1. 回歸（原日期）")
reg = run(copy("[範本試跑-回歸]202611-1清潔承攬-台北"), "回歸")
orig = gc.open_by_key(SRC)
for sheet in ("場次時數薪資總表", "薪資表", "專案薪資表"):
    rng = "A1:Z200" if sheet == "場次時數薪資總表" else "A1996:DZ2048"
    a = orig.worksheet(sheet).get(rng, value_render_option="UNFORMATTED_VALUE") or []
    b = reg.worksheet(sheet).get(rng, value_render_option="UNFORMATTED_VALUE") or []
    d = engine.compare_blocks(a, b, max([len(x) for x in a + b] + [1]))
    first = 1 if sheet == "場次時數薪資總表" else 1996
    if not d:
        say(f"- {sheet} {rng}：**與原檔完全一致**")
        continue
    by_row = {}
    for r, c, x, y in d:
        by_row.setdefault(first + r - 1, []).append((engine.col_letter(c), x, y))
    say(f"- {sheet} {rng}：{len(d)} 格不同，分布在 {len(by_row)} 列：")
    for rr, items in sorted(by_row.items()):
        kinds = sorted({f"{x!r}→{y!r}" if not isinstance(y, (int, float)) or x in ('', None) or str(x).startswith('#') else 'value' for _, x, y in items})
        sample = "、".join(f"{c}:{x!r}→{y!r}" for c, x, y in items[:3])
        say(f"    - 第 {rr} 列：{len(items)} 格（例：{sample}）")

# ── 2. 大掃除情境：11/1～11/7 大掃除1、11/8～11/15 大掃除2 ───────
say("\n### 2. 大掃除情境（大掃除1：11/1～11/7、大掃除2：11/8～11/15）")
sc = run(copy("[範本試跑-大掃除]202611-1清潔承攬-台北"), "大掃除", ["2026/11/1", "2026/11/8", "2026/11/15"])
ws = sc.worksheet("薪資表")
names = ws.row_values(1)[11:]
end = cp1._col_letter(11 + len(names))
vals = ws.get(f"L1996:{end}2027", value_render_option="UNFORMATTED_VALUE")
row = lambda r: vals[r - 1996] if r - 1996 < len(vals) else []
v = lambda r, i: (row(r)[i] if i < len(row(r)) and row(r)[i] not in ("",) else 0)
bad_1996, bad_sum, bad_sub, refs = [], [], [], []
totals = {r: 0 for r in range(2006, 2015)}
interns = []
for i, n in enumerate(names):
    if not str(n).strip():
        continue
    cells = [v(r, i) for r in range(1996, 2028)]
    if any(isinstance(c, str) and c.startswith("#") for c in cells):
        refs.append(n)
        continue
    if round(float(v(1996, i)), 6) != 0:
        bad_1996.append(f"{n}={v(1996, i)}")
    hours = v(2003, i) + sum(v(r, i) for r in range(2006, 2015))
    if round(hours - v(2015, i), 6) != 0:
        bad_sum.append(f"{n}: 分類合計 {hours} ≠ 總時數 {v(2015, i)}")
    for r in totals:
        totals[r] += v(r, i)
    sub = (v(2002, i) * v(2016, i) + v(2003, i) * v(2017, i) + v(2004, i) * v(2016, i) + v(2005, i) * v(2017, i)
           + sum(v(h, i) * v(rt, i) for h, rt in zip(range(2006, 2015), range(2018, 2027))))
    if round(sub - v(2027, i), 4) != 0:
        bad_sub.append(f"{n}: 計算 {sub} ≠ 小計 {v(2027, i)}")
    if v(2003, i):
        interns.append(f"{n}（實習 {v(2003, i)}h：大掃除1平日 {v(2009, i)}、大掃除2平日 {v(2012, i)}、大掃除2週末 {v(2014, i)}、總時數 {v(2015, i)}）")
labels = ["平日", "晚間", "週末", "大掃除1平日", "大掃除1晚間", "大掃除1週末", "大掃除2平日", "大掃除2晚間", "大掃除2週末"]
say("- 全員各類時數合計：" + "、".join(f"{lb} {totals[r]:g}" for lb, r in zip(labels, totals)))
say("- 錯誤值（#REF! 等）：" + ("無" if not refs else "、".join(refs)))
say("- 1996 檢查：" + ("**全部為 0**" if not bad_1996 else "、".join(bad_1996[:15])))
say("- 實習＋各類時數＝總時數：" + ("**全部吻合（無重複、無遺漏）**" if not bad_sum else "；".join(bad_sum[:10])))
say("- 小計＝時數×費率：" + ("**全部吻合**" if not bad_sub else "；".join(bad_sub[:10])))
say("- 實習生：" + "；".join(interns))
rate = ws.get("L2021:L2026", value_render_option="UNFORMATTED_VALUE")
say("- L 欄（蔡立娟）大掃除費率 2021～2026：" + "、".join(str(x[0]) if x else "" for x in rate))
