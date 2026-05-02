"""
Google API 授權模組（Service Account）
"""

import streamlit as st
import gspread
from google.oauth2.service_account import Credentials
from googleapiclient.discovery import build

SCOPES = [
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/drive",
]


@st.cache_resource
def get_credentials():
    """從 Streamlit Secrets 取得 Service Account 憑證"""
    return Credentials.from_service_account_info(
        st.secrets["gcp_service_account"],
        scopes=SCOPES
    )


def get_drive_service():
    """取得 Google Drive API 客戶端"""
    return build("drive", "v3", credentials=get_credentials())


def get_gspread_client():
    """取得 gspread 客戶端"""
    return gspread.authorize(get_credentials())


def open_spreadsheet(file_id: str):
    """用 ID 開啟試算表"""
    return get_gspread_client().open_by_key(file_id)


def get_sheet(file_id: str, sheet_name: str):
    """用試算表 ID 和工作表名稱取得工作表"""
    return open_spreadsheet(file_id).worksheet(sheet_name)
