"""Service Account 擁有、名稱以「匯入引擎試跑」開頭的資料夾：列出並移到垃圾桶。"""
import os
import sys

sys.path.insert(0, os.getcwd())
from modules import auth  # noqa: E402

drive = auth.get_drive_service()
res = drive.files().list(
    q="'me' in owners and trashed=false and createdTime > '2026-11-15T00:00:00'",
    fields="files(id,name,mimeType,createdTime)", pageSize=200,
).execute()
files = res.get("files", [])
print(f"Service Account 近期檔案共 {len(files)} 個")
for f in files:
    print(f"  {f['name']}｜{f['mimeType'].split('.')[-1]}｜{f['createdTime']}")
for f in files:
    if f["mimeType"].endswith("folder") and f["name"].startswith("匯入引擎試跑"):
        drive.files().delete(fileId=f["id"]).execute()
        print(f"  已刪除：{f['name']}")
print("完成")
