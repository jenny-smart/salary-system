"""
Google API 授權模組
支援兩種模式：
1. OAuth（用 jenny@lemonclean.com.tw 帳號授權）← 主要模式，複製的檔案擁有者是你
2. Service Account（備用）
"""

import streamlit as st
from google.oauth2.credentials import Credentials
from google.oauth2.service_account import Credentials as SACredentials
from google_auth_oauthlib.flow import Flow
from googleapiclient.discovery import build
import gspread
from gspread.auth import local_server_flow

SCOPES = [
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/drive",
]


# ═══════════════════════════════════════
# OAuth 模式（主要）
# ═══════════════════════════════════════

def _get_oauth_flow() -> Flow:
    """建立 OAuth Flow"""
    client_config = {
        "web": {
            "client_id": st.secrets["oauth"]["client_id"],
            "client_secret": st.secrets["oauth"]["client_secret"],
            "auth_uri": "https://accounts.google.com/o/oauth2/auth",
            "token_uri": "https://oauth2.googleapis.com/token",
            "redirect_uris": ["https://salary-system.streamlit.app/_stcore/oauth_callback"],
        }
    }
    flow = Flow.from_client_config(
        client_config,
        scopes=SCOPES,
        redirect_uri="https://salary-system.streamlit.app/_stcore/oauth_callback"
    )
    return flow


def get_oauth_credentials():
    """
    取得 OAuth 憑證
    如果已有 token 就直接用，沒有就導向授權頁面
    """
    # 檢查是否已有 token
    if "oauth_token" in st.session_state:
        token_data = st.session_state.oauth_token
        creds = Credentials(
            token=token_data.get("token"),
            refresh_token=token_data.get("refresh_token"),
            token_uri="https://oauth2.googleapis.com/token",
            client_id=st.secrets["oauth"]["client_id"],
            client_secret=st.secrets["oauth"]["client_secret"],
            scopes=SCOPES,
        )
        return creds

    # 檢查是否從授權頁面回來
    query_params = st.query_params
    if "code" in query_params:
        flow = _get_oauth_flow()
        flow.fetch_token(code=query_params["code"])
        creds = flow.credentials
        st.session_state.oauth_token = {
            "token": creds.token,
            "refresh_token": creds.refresh_token,
        }
        st.query_params.clear()
        return creds

    return None


def show_login_button():
    """顯示 Google 登入按鈕"""
    flow = _get_oauth_flow()
    auth_url, _ = flow.authorization_url(
        prompt="consent",
        access_type="offline",
    )
    st.markdown(f"""
    <div style="text-align:center;padding:40px;">
        <h3>🍋 Lemon Clean 薪資系統</h3>
        <p>請用公司帳號登入以繼續</p>
        <a href="{auth_url}" target="_self">
            <button style="background:#1f6c9e;color:white;border:none;padding:12px 32px;
                border-radius:40px;font-size:1rem;font-weight:600;cursor:pointer;">
                🔐 用 Google 帳號登入
            </button>
        </a>
    </div>
    """, unsafe_allow_html=True)


# ═══════════════════════════════════════
# Service Account 模式（備用）
# ═══════════════════════════════════════

@st.cache_resource
def _get_sa_credentials():
    """取得 Service Account 憑證"""
    return SACredentials.from_service_account_info(
        st.secrets["gcp_service_account"],
        scopes=SCOPES
    )


# ═══════════════════════════════════════
# 統一對外介面
# ═══════════════════════════════════════

def get_credentials():
    """
    取得憑證
    優先用 OAuth，沒有則用 Service Account
    """
    creds = get_oauth_credentials()
    if creds:
        return creds
    return _get_sa_credentials()


def get_drive_service():
    """取得 Google Drive API 客戶端"""
    creds = get_credentials()
    return build("drive", "v3", credentials=creds)


def get_gspread_client():
    """取得 gspread 客戶端"""
    creds = get_credentials()
    return gspread.authorize(creds)


def open_spreadsheet(file_id: str):
    """用 ID 開啟試算表"""
    client = get_gspread_client()
    return client.open_by_key(file_id)


def get_sheet(file_id: str, sheet_name: str):
    """用試算表 ID 和工作表名稱取得工作表"""
    ss = open_spreadsheet(file_id)
    return ss.worksheet(sheet_name)


def is_logged_in() -> bool:
    """檢查是否已登入"""
    if "oauth_token" in st.session_state:
        return True
    query_params = st.query_params
    if "code" in query_params:
        return True
    return False


def logout():
    """登出"""
    if "oauth_token" in st.session_state:
        del st.session_state.oauth_token
