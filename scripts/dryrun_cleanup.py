"""唯讀：訂單來源盤點（薪資檔「訂單資料」分頁、Drive「訂單資料」匯出檔），並分析收現對不到訂單的原因。"""
import os
import re
import sys
from collections import Counter
from datetime import date

sys.path.insert(0, os.getcwd())
from modules import auth  # noqa: E402

jgc = auth.get_jenny_gspread_client()
drive = auth.get_jenny_drive_service()


def d(v):
    m = re.match(r"(\d{4})[/-](\d{1,2})[/-](\d{1,2})", str(v or "").strip())
    return date(int(m[1]), int(m[2]), int(m[3])) if m else None


od = jgc.open_by_key("1AvoUGlFCxaKsPOPavgBmqHoyn-_t4c0I82bIth7SvRQ").worksheet("訂單資料")
vals = od.get("A1:AI40000")
h = vals[0]
print("訂單資料 表頭前段：", h[:12], "… 服務專員欄：", h.index("服務專員") if "服務專員" in h else "無")
hi = {k: h.index(k) for k in ("訂單編號", "服務日期", "聯絡電話", "服務專員", "購買項目")}
rows = [r + [""] * 40 for r in vals[1:] if any(str(x).strip() for x in r)]
ds = [d(r[hi["服務日期"]]) for r in rows]
ds = [x for x in ds if x]
print(f"訂單資料：{len(rows)} 列；服務日期 {min(ds)}～{max(ds)}")
print("各月筆數：", sorted(Counter(x.strftime("%Y-%m") for x in ds).items()))
print("服務專員空白：", sum(1 for r in rows if not str(r[hi['服務專員']]).strip()))
print("A 欄（未命名）前幾個值：", Counter(str(r[0]) for r in rows).most_common(5))
try:
    print("訂單資料 分頁是否公式：", od.get("B2", value_render_option="FORMULA"))
except Exception:
    pass

# Drive 匯出檔
res = drive.files().list(q="name contains '訂單' and mimeType != 'application/vnd.google-apps.folder' and trashed=false",
                         orderBy="modifiedTime desc", pageSize=8, fields="files(name,modifiedTime,mimeType)").execute()
for f in res.get("files", []):
    print("Drive 匯出：", f["name"], f["modifiedTime"], f["mimeType"].split(".")[-1])

# 收現對不到的原因
cash = jgc.open_by_key("1bNcJuFuP--jdpNo2zJKOpvuq-5rSHW3LgGE8HEepf44").worksheet("專員收現").get_all_values()[1:]
idx_date = {}
for r in rows:
    k = d(r[hi["服務日期"]])
    if k:
        idx_date.setdefault(k, []).append(r)
reasons = Counter()
for c in cash:
    c = c + [""] * 15
    sd, ph, staff = d(c[3]), re.sub(r"\D", "", c[5])[-9:], c[2].strip()
    same = idx_date.get(sd, [])
    if not same:
        reasons["該日在訂單資料沒有任何訂單"] += 1
        continue
    byphone = [r for r in same if re.sub(r"\D", "", r[hi["聯絡電話"]])[-9:] == ph]
    bystaff = [r for r in same if staff and staff in r[hi["服務專員"]]]
    if byphone:
        reasons["電話對得到"] += 1
    elif len(bystaff) == 1:
        reasons["電話對不到，但當天該專員只有 1 張單"] += 1
    elif bystaff:
        reasons[f"電話對不到，當天該專員有多張單"] += 1
    else:
        reasons["當天訂單沒有該專員"] += 1
print("專員收現 對應訂單：", reasons.most_common())
