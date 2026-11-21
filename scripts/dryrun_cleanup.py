"""清理試跑資料夾：只找授權帳號擁有、名稱開頭為試跑前綴的資料夾，移到垃圾桶（30 天內可還原）。"""
import os
import sys

sys.path.insert(0, os.getcwd())
from modules import auth  # noqa: E402

PREFIXES = ("匯入引擎試跑", "實習範本試跑", "薪資表範本試跑")
drive = auth.get_jenny_drive_service()
me = drive.about().get(fields="user(emailAddress)").execute()["user"]["emailAddress"]
print(f"授權帳號：{me}")

found = []
for prefix in PREFIXES:
    res = drive.files().list(
        q=(f"name contains '{prefix}' and mimeType='application/vnd.google-apps.folder' "
           "and 'me' in owners and trashed=false"),
        fields="files(id,name,createdTime)", pageSize=100,
    ).execute()
    found += [f for f in res.get("files", []) if f["name"].startswith(prefix)]

print(f"找到 {len(found)} 個試跑資料夾")
for f in found:
    drive.files().update(fileId=f["id"], body={"trashed": True}).execute()
    print(f"  已移到垃圾桶：{f['name']}（建立於 {f['createdTime']}）")
print("完成")
