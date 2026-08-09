"""
Google API 認證共用模組。

- 既有 Google Sheets / Drive 自動化：Service Account
- 需要 Jenny 本人權限的 Google Sheets / Drive：Jenny OAuth
- 需要建立或覆蓋實體 Drive 檔案：Jenny OAuth

本機 Jenny OAuth token：credentials/jenny_token.json
雲端可透過環境變數或 Streamlit secret JENNY_GOOGLE_TOKEN
提供完整 authorized-user token JSON。
"""

from __future__ import annotations

import json
import os
from pathlib import Path

import googleapiclient.discovery
import gspread
from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials as UserCredentials
from google.oauth2.service_account import Credentials as ServiceAccountCredentials


SCOPES = [
    "https://www.googleapis.com/auth/drive",
    "https://www.googleapis.com/auth/spreadsheets",
]

PROJECT_ROOT = Path(__file__).resolve().parent.parent
JENNY_TOKEN_PATH = PROJECT_ROOT / "credentials" / "jenny_token.json"


def _streamlit_secret(name: str) -> str:
    try:
        import streamlit as st

        value = st.secrets.get(name, "")
        if isinstance(value, str):
            return value.strip()
        if value:
            return json.dumps(dict(value))
    except Exception:
        pass
    return ""


def _load_json_object(raw: str, setting_name: str) -> dict:
    try:
        value = json.loads(raw)
    except json.JSONDecodeError as exc:
        raise RuntimeError(f"{setting_name} 不是有效的 JSON：{exc}") from exc
    if not isinstance(value, dict):
        raise RuntimeError(f"{setting_name} 必須是 JSON object。")
    return value


def _load_service_account_info() -> dict:
    raw = os.environ.get("GOOGLE_SERVICE_ACCOUNT", "").strip()
    if not raw:
        raw = _streamlit_secret("GOOGLE_SERVICE_ACCOUNT")
    if raw:
        return _load_json_object(raw, "GOOGLE_SERVICE_ACCOUNT")

    local_paths = [
        PROJECT_ROOT / "salary-461300-a575f1e99d06.json",
        PROJECT_ROOT / "service_account.json",
        PROJECT_ROOT / "credentials" / "service_account.json",
    ]
    for path in local_paths:
        if path.exists():
            return _load_json_object(
                path.read_text(encoding="utf-8"), str(path)
            )

    raise RuntimeError(
        "找不到 GOOGLE_SERVICE_ACCOUNT。請在環境變數或 Streamlit secrets 設定。"
    )


def _load_jenny_token_info() -> tuple[dict, Path | None]:
    """讀取 Jenny OAuth token，回傳 (token_info, local_path)。"""
    raw = os.environ.get("JENNY_GOOGLE_TOKEN", "").strip()
    if not raw:
        raw = _streamlit_secret("JENNY_GOOGLE_TOKEN")
    if raw:
        info = _load_json_object(raw, "JENNY_GOOGLE_TOKEN")
        return info, None

    if JENNY_TOKEN_PATH.exists():
        info = _load_json_object(
            JENNY_TOKEN_PATH.read_text(encoding="utf-8"),
            str(JENNY_TOKEN_PATH),
        )
        return info, JENNY_TOKEN_PATH

    raise RuntimeError(
        "找不到 Jenny OAuth token。請建立 credentials/jenny_token.json，"
        "或設定 JENNY_GOOGLE_TOKEN。"
    )


def get_credentials():
    """取得 Service Account credentials；不供 Jenny OAuth 流程使用。"""
    info = _load_service_account_info()
    return ServiceAccountCredentials.from_service_account_info(info, scopes=SCOPES)


def get_jenny_credentials():
    """取得 Jenny OAuth 使用者憑證，過期時以 refresh token 自動更新。"""
    info, local_path = _load_jenny_token_info()

    if info.get("type") == "service_account":
        raise RuntimeError(
            "JENNY_GOOGLE_TOKEN 內容是 Service Account，不是 Jenny 使用者 OAuth token。"
        )
    if not info.get("refresh_token"):
        raise RuntimeError(
            "Jenny OAuth token 缺少 refresh_token，請重新完成離線 OAuth 授權。"
        )

    try:
        creds = UserCredentials.from_authorized_user_info(info, scopes=SCOPES)
    except Exception as exc:
        raise RuntimeError(f"無法載入 Jenny OAuth token：{exc}") from exc

    if not creds.valid and creds.refresh_token:
        try:
            creds.refresh(Request())
        except Exception as exc:
            raise RuntimeError(
                "Jenny OAuth token 更新失敗，請重新執行 get_refresh_token.py 授權："
                f"{exc}"
            ) from exc

        if local_path is not None:
            local_path.parent.mkdir(parents=True, exist_ok=True)
            local_path.write_text(creds.to_json(), encoding="utf-8")

    if not creds.valid:
        raise RuntimeError(
            "Jenny OAuth token 無效，請重新執行 get_refresh_token.py 完成授權。"
        )

    return creds


def get_drive_service():
    """Service Account Drive service：僅供既有自動化使用。"""
    return googleapiclient.discovery.build(
        "drive", "v3", credentials=get_credentials(), cache_discovery=False
    )


def get_jenny_drive_service():
    """Jenny 使用者 OAuth Drive service：建立、複製及轉換 Drive 檔案。"""
    return googleapiclient.discovery.build(
        "drive", "v3", credentials=get_jenny_credentials(), cache_discovery=False
    )


def get_sheets_service():
    """Service Account Google Sheets API service。"""
    return googleapiclient.discovery.build(
        "sheets", "v4", credentials=get_credentials(), cache_discovery=False
    )


def get_gspread_client():
    """Service Account gspread client。"""
    return gspread.authorize(get_credentials())


def get_jenny_gspread_client():
    """Jenny OAuth gspread client。"""
    return gspread.authorize(get_jenny_credentials())


def open_spreadsheet(spreadsheet_id: str):
    """使用 Service Account 開啟 Google Spreadsheet。"""
    return get_gspread_client().open_by_key(spreadsheet_id)


def open_jenny_spreadsheet(spreadsheet_id: str):
    """使用 Jenny OAuth 開啟 Google Spreadsheet。"""
    return get_jenny_gspread_client().open_by_key(spreadsheet_id)
