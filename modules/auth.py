"""
modules/auth.py
Google API 認證共用模組
GitHub Actions 使用 GOOGLE_SERVICE_ACCOUNT
Streamlit / 本機也可使用 Streamlit secrets 或環境變數
"""

from __future__ import annotations

import json
import os
from pathlib import Path

import gspread
import googleapiclient.discovery
from google.oauth2.service_account import Credentials


SCOPES = [
    "https://www.googleapis.com/auth/drive",
    "https://www.googleapis.com/auth/spreadsheets",
]


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


def get_credentials():
    info = _load_service_account_info()
    return Credentials.from_service_account_info(info, scopes=SCOPES)


def get_drive_service():
    creds = get_credentials()
    return googleapiclient.discovery.build(
        "drive",
        "v3",
        credentials=creds,
        cache_discovery=False,
    )


def get_sheets_service():
    creds = get_credentials()
    return googleapiclient.discovery.build(
        "sheets",
        "v4",
        credentials=creds,
        cache_discovery=False,
    )


def get_gspread_client():
    creds = get_credentials()
    return gspread.authorize(creds)


def open_spreadsheet(spreadsheet_id: str):
    gc = get_gspread_client()
    return gc.open_by_key(spreadsheet_id)
