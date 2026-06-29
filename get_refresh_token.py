from google_auth_oauthlib.flow import InstalledAppFlow

SCOPES = [
    "https://www.googleapis.com/auth/drive",
    "https://www.googleapis.com/auth/spreadsheets",
]

flow = InstalledAppFlow.from_client_secrets_file(
    "client_secret_xxxxxxxxx.json",   # ← 改成你的 JSON 檔名
    SCOPES,
)

creds = flow.run_local_server(
    port=0,
    access_type="offline",
    prompt="consent",
)

print()
print("=" * 60)
print("CLIENT_ID")
print(creds.client_id)
print()
print("CLIENT_SECRET")
print(creds.client_secret)
print()
print("REFRESH_TOKEN")
print(creds.refresh_token)
print("=" * 60)
