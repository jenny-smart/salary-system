"""
各區寄件 Gmail 授權（承攬費通知信用）。

用法：在本機執行 `python get_gmail_token.py client_secret_xxx.json`，
瀏覽器跳出時「登入要授權的那個寄件信箱」（例如 salary.lemontaipei@gmail.com）。
每個地區信箱各跑一次，把印出的 JSON 合併貼到 Streamlit secrets：

REGION_GMAIL_TOKENS = '''
{
  "salary.lemontaipei@gmail.com": {...},
  "salary.lemontaichung@gmail.com": {...}
}
'''

注意：Google Cloud 的 OAuth 同意畫面若是「測試中」，refresh token 7 天就會失效，
請改為「正式版（In production）」。
"""
import json
import sys

from google_auth_oauthlib.flow import InstalledAppFlow

SCOPES = [
    "https://www.googleapis.com/auth/gmail.send",
    "https://www.googleapis.com/auth/gmail.readonly",
]

client_file = sys.argv[1] if len(sys.argv) > 1 else "client_secret.json"
flow = InstalledAppFlow.from_client_secrets_file(client_file, SCOPES)
creds = flow.run_local_server(port=0, access_type="offline", prompt="consent")

import googleapiclient.discovery  # noqa: E402

mailbox = googleapiclient.discovery.build("gmail", "v1", credentials=creds, cache_discovery=False) \
    .users().getProfile(userId="me").execute()["emailAddress"]
print()
print("=" * 60)
print(json.dumps({mailbox: json.loads(creds.to_json())}, ensure_ascii=False, indent=2))
print("=" * 60)
