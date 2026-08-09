"""
modules/auth.py
Google API 認證共用模組

- 既有 Google Sheets / Drive 自動化：Service Account
- 需要 Jenny 本人權限的 Google Sheets / Drive：Jenny OAuth
- 需要建立或覆蓋實體 Drive 檔案（xlsx / PDF）：Jenny OAuth

本機 Jenny OAuth token：credentials/jenny_token.json
也可透過環境變數 / Streamlit secret JENNY_GOOGLE_TOKEN 提供完整 token JSON。
"""

from __future__ import annotations

import json
import os
from pathlib import Path

import gspread
import googleapiclient.discovery
from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials as UserCredentials
from google.oauth2.service_account import Credentials


SCOPES = [
    "https://www.googleapis.com/auth/drive",
    "https://www.googleapis.com/auth/spreadsheets",
]

PROJECT_ROOT = Path(__file__).resolve().parent.parent
JENNY_TOKEN_PATH = PROJECT_ROOT / "credentials" / "jenny_token.json"


def _load_service_account_info() -> dict:
    raw = os.environ.get("GOOGLE_SERVICE_ACCOUNT", "").strip()

    if raw:
        return json.loads(raw)

    try:
        import streamlit as st
        raw = st.secrets.get("GOOGLE_SERVICE_ACCOUNT", "")
        if raw:
            return json.loads(raw)
    except Exception:
        pass

    local_paths = [
        "salary-461300-a575f1e99d06.json",
        "service_account.json",
        "credentials/service_account.json",
    ]

    for p in local_paths:
        path = Path(p)
        if path.exists():
            return json.loads(path.read_text(encoding="utf-8"))

    raise RuntimeError(
        "找不到 GOOGLE_SERVICE_ACCOUNT。請在 GitHub Secrets 或 Streamlit secrets 設定。"
    )


def _load_jenny_token_info() -> tuple[dict, Path | None]:
    """讀取 Jenny OAuth token；回傳 (token_info, local_path)。"""
    raw = os.environ.get("JENNY_GOOGLE_TOKEN", "").strip()
    if raw:
        return json.loads(raw), None

    try:
        import streamlit as st
        raw = st.secrets.get("JENNY_GOOGLE_TOKEN", "")
        if raw:
            return json.loads(raw), None
    except Exception:
        pass

    if JENNY_TOKEN_PATH.exists():
        return json.loads(JENNY_TOKEN_PATH.read_text(encoding="utf-8")), JENNY_TOKEN_PATH

    raise RuntimeError(
        "找不到 Jenny OAuth token。請先在本機完成 OAuth 授權並建立 "
        "credentials/jenny_token.json，或設定 JENNY_GOOGLE_TOKEN。"
    )


def get_credentials():
    """取得 Service Account credentials。"""
    info = _load_service_account_info()
    return Credentials.from_service_account_info(info, scopes=SCOPES)


def get_jenny_credentials():
    """取得 Jenny OAuth 使用者憑證，過期時自動 refresh。"""
    info, local_path = _load_jenny_token_info()
    creds = UserCredentials.from_authorized_user_info(info, scopes=SCOPES)

    if creds.expired and creds.refresh_token:
        creds.refresh(Request())

        # 本機模式下把更新後 token 寫回，避免之後使用舊 access token。
        if local_path is not None:
            local_path.write_text(creds.to_json(), encoding="utf-8")

    if not creds.valid:
        raise RuntimeError(
            "Jenny OAuth token 無效或已失效，請重新執行 get_refresh_token.py 完成授權。"
        )

    return creds


def get_drive_service():
    """Service Account Drive service：既有自動化/讀取用途。"""
    creds = get_credentials()
    return googleapiclient.discovery.build(
        "drive",
        "v3",
        credentials=creds,
        cache_discovery=False,
    )


def get_jenny_drive_service():
    """Jenny OAuth Drive service：建立/覆蓋 xlsx、PDF 等實體 Drive 檔案。"""
    creds = get_jenny_credentials()
    return googleapiclient.discovery.build(
        "drive",
        "v3",
        credentials=creds,
        cache_discovery=False,
    )


def get_sheets_service():
    """Service Account Google Sheets API service。"""
    creds = get_credentials()
    return googleapiclient.discovery.build(
        "sheets",
        "v4",
        credentials=creds,
        cache_discovery=False,
    )


def get_gspread_client():
    """Service Account gspread client：既有自動化試算表。"""
    creds = get_credentials()
    return gspread.authorize(creds)


def get_jenny_gspread_client():
    """
    Jenny OAuth gspread client。

    用於 Jenny 本人有權限、但 Service Account 沒有權限的 Google Sheet，
    例如各區「YYYY承攬服務費mail」。
    """
    creds = get_jenny_credentials()
    return gspread.authorize(creds)


def open_spreadsheet(spreadsheet_id: str):
    """使用 Service Account 開啟 Google Spreadsheet。"""
    gc = get_gspread_client()
    return gc.open_by_key(spreadsheet_id)


def open_jenny_spreadsheet(spreadsheet_id: str):
    """使用 Jenny OAuth 開啟 Google Spreadsheet。"""
    gc = get_jenny_gspread_client()
    return gc.open_by_key(spreadsheet_id)
