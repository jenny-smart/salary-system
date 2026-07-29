"""由中控 GAS（以部署者權限）產出並儲存 PDF。"""

import os
import requests


def _url() -> str:
    value = os.environ.get("GAS_SCHEDULER_WEB_APP_URL", "").strip()
    if value:
        return value
    try:
        import streamlit as st
        return str(st.secrets.get("GAS_SCHEDULER_WEB_APP_URL", "")).strip()
    except Exception:
        return ""


def generate_pdf(spreadsheet_id, region, period, kind) -> dict:
    url = _url()
    if not url:
        return {"success": False, "message": "尚未設定 GAS_SCHEDULER_WEB_APP_URL"}
    response = requests.post(
        url,
        params={
            "action": "generatePdf",
            "spreadsheetId": spreadsheet_id,
            "region": region,
            "period": period,
            "kind": kind,
        },
        timeout=360,
    )
    try:
        data = response.json()
    except Exception:
        data = {"success": False, "message": response.text[:500]}
    if "success" not in data:
        data["success"] = False
        data["message"] = (
            data.get("message")
            or "中央 GAS 回應不是 PDF API；請更新並重新部署 Web App"
        )
    if response.status_code >= 400:
        data["success"] = False
        data["message"] = data.get("message") or f"HTTP {response.status_code}"
    return data


def run_yuanta(spreadsheet_id, region, period) -> dict:
    url = _url()
    if not url:
        return {"success": False, "message": "尚未設定 GAS_SCHEDULER_WEB_APP_URL"}
    response = requests.post(
        url,
        params={
            "action": "runYuanta",
            "spreadsheetId": spreadsheet_id,
            "region": region,
            "period": period,
        },
        timeout=360,
    )
    try:
        return response.json()
    except Exception:
        return {"success": False, "message": response.text[:500]}
