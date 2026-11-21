"""修正：還原台北水洗 AE5（7月第一階段誤寫 1470）；為台中清潔各月新來源開 IMPORTRANGE 權限。"""
import os
import sys
import time

sys.path.insert(0, os.getcwd())
import requests  # noqa: E402
from google.auth.transport.requests import Request  # noqa: E402
from modules import auth  # noqa: E402

jgc = auth.get_jenny_gspread_client()
tp = jgc.open_by_key("1PYUvL9bz3a-8f1r2WA0qEfy6O5rMKWEOytGJ-lRm9fg").worksheet("水洗")
print("AE5 目前：", tp.acell("AE5", value_render_option="UNFORMATTED_VALUE").value, "／A5：", tp.acell("A5").value)
if str(tp.acell("A5").value).strip() == "劉士誠" and str(tp.acell("AE5", value_render_option="UNFORMATTED_VALUE").value) in ("1470", "1470.0"):
    tp.update_acell("AE5", "")
    print("已還原 AE5 為空白")

DEST = "1eAt1gczhwHPOpVX2uFQumJ6xnvTWnij8N1w0IBY5qkk"
ws = jgc.open_by_key(DEST).worksheet("清潔各月")
row1 = (ws.get("A1:AI1") or [[]])[0]
srcs = [row1[3 * i] for i in range(11) if 3 * i < len(row1) and row1[3 * i]]
creds = auth.get_jenny_credentials()
creds.refresh(Request())
for src in srcs:
    url = (f"https://docs.google.com/spreadsheets/d/{DEST}/externaldata/addimportrangepermissions"
           f"?donorDoc={src}")
    r = requests.post(url, headers={"Authorization": f"Bearer {creds.token}"})
    print(f"  權限 {src[:10]}…：HTTP {r.status_code}")
time.sleep(5)
vals = (ws.get("A2:AI2") or [[]])[0] + [""] * 40
for m in range(1, 12):
    print(f"  {m}月 第一/第二：{vals[3 * (m - 1) + 1]!r} / {vals[3 * (m - 1) + 2]!r}")
