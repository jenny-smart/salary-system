"""
modules/gas_scheduler_client.py
呼叫 GAS Web App 更新排程 Trigger
"""

from __future__ import annotations

import os
import requests


def _get_gas_scheduler_url() -> str:
    url = os.environ.get("GAS_SCHEDULER_WEB_APP_URL", "").strip()

    if url:
        return url

    try:
        import streamlit as st
        return str(st.secrets.get("GAS_SCHEDULER_WEB_APP_URL", "")).strip()
    except Exception:
        return ""


def sync_gas_schedule_triggers() -> dict:
    url = _get_gas_scheduler_url()

    if not url:
        return {
            "success": False,
            "message": "尚未設定 GAS_SCHEDULER_WEB_APP_URL"
        }

    response = requests.post(
        url,
        params={"action": "syncTriggers"},
        timeout=60,
    )

    try:
        return response.json()
    except Exception:
        return {
            "success": False,
            "status_code": response.status_code,
            "text": response.text
        }
