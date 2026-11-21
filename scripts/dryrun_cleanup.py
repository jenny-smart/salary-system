"""列出授權帳號近幾天建立、名稱含「試跑」或「引擎」的檔案與資料夾（只列出，不刪除）。"""
import os
import sys

sys.path.insert(0, os.getcwd())
from modules import auth  # noqa: E402

drive = auth.get_jenny_drive_service()
res = drive.files().list(
    q=("(name contains '試跑' or name contains '引擎' or name contains 'dryrun' or name contains 'COMPARE' "
       "or name contains 'PYTHON') and 'me' in owners and createdTime > '2026-11-19T00:00:00' "
       "and trashed=false"),
    fields="files(id,name,mimeType,createdTime,parents)", pageSize=200,
).execute()
files = res.get("files", [])
print(f"共 {len(files)} 個")
for f in sorted(files, key=lambda x: x["createdTime"]):
    kind = "資料夾" if f["mimeType"].endswith("folder") else "檔案"
    print(f"  [{kind}] {f['name']}｜{f['createdTime']}｜parent={(f.get('parents') or [''])[0]}")
