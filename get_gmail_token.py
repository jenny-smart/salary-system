"""
產生各區寄件 Gmail 的授權 token（承攬費通知信「更新範本草稿」用）。

用法（在自己電腦執行，瀏覽器登入「該地區」的 Gmail 帳號）：
    python get_gmail_token.py 台北

前置：
    1. Google Cloud 專案啟用 Gmail API
    2. OAuth 用戶端 JSON 放在本資料夾（檔名同 get_refresh_token.py 使用的那份）
    3. OAuth 同意畫面若為「測試」狀態，需把各區 Gmail 加入測試使用者

完成後把印出的 JSON 整段存成 Streamlit secret「GMAIL_TOKEN_<代碼>」，
本機執行時也會另存 credentials/gmail_token_<代碼>.json。
"""

import sys
from pathlib import Path

from google_auth_oauthlib.flow import InstalledAppFlow

from modules.service_fee_draft import GMAIL_SCOPES, REGION_MAILBOXES

CLIENT_SECRET_FILE = "client_secret_xxxxxxxxx.json"   # ← 改成你的 JSON 檔名

if len(sys.argv) != 2 or sys.argv[1].replace("區", "") not in REGION_MAILBOXES:
    print("用法：python get_gmail_token.py <地區>")
    print("地區：" + "、".join(REGION_MAILBOXES))
    sys.exit(1)

region = sys.argv[1].replace("區", "")
email, code = REGION_MAILBOXES[region]
print(f"請在瀏覽器登入：{email}")

flow = InstalledAppFlow.from_client_secrets_file(CLIENT_SECRET_FILE, GMAIL_SCOPES)
creds = flow.run_local_server(
    port=0,
    access_type="offline",
    prompt="consent",
    login_hint=email,
)

token_json = creds.to_json()
out = Path("credentials") / f"gmail_token_{code.lower()}.json"
out.parent.mkdir(parents=True, exist_ok=True)
out.write_text(token_json, encoding="utf-8")

print()
print("=" * 60)
print(f"Streamlit secret 名稱：GMAIL_TOKEN_{code}")
print("內容（整段複製）：")
print(token_json)
print("=" * 60)
print(f"已另存：{out}")
