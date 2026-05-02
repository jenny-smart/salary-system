import gspread
import streamlit as st
from google.oauth2.service_account import Credentials
from googleapiclient.discovery import build

SCOPES = [
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/drive",
]

@st.cache_resource
def get_credentials():
    """從 Streamlit Secrets 取得 Service Account 憑證"""
    credentials = Credentials.from_service_account_info(
        st.secrets["gcp_service_account"],
        scopes=SCOPES
    )
    return credentials

def get_gspread_client():
    """取得 gspread 客戶端（操作 Google Sheets 用）"""
    credentials = get_credentials()
    return gspread.authorize(credentials)

def get_drive_service():
    """取得 Google Drive API 客戶端（操作 Drive 用）"""
    credentials = get_credentials()
    return build("drive", "v3", credentials=credentials)

def open_spreadsheet(file_id: str):
    """用 ID 開啟試算表"""
    client = get_gspread_client()
    return client.open_by_key(file_id)

def get_sheet(file_id: str, sheet_name: str):
    """用試算表 ID 和工作表名稱取得工作表"""
    ss = open_spreadsheet(file_id)
    return ss.worksheet(sheet_name)
