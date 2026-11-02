"""
modules/lemon_backend.py
檸檬後台（backend.lemonclean.com.tw）登入與訂單待退狀態查詢。

登入流程與 tool-system 的 tools/common/lemon_backend_login.py 相同：
取登入頁 _token → 送出帳密 → 確認沒有被退回登入頁；結束時登出。
（salary-system 沒有 bs4，_token 改用正規表示式取得。）

帳密來源（依序）：
  1. st.secrets["accounts"][<地區key>]["email"/"password"]（與 tool-system 相同格式）
  2. 環境變數 LEMON_<地區KEY>_EMAIL／LEMON_<地區KEY>_PASSWORD（例如 LEMON_TAIPEI_EMAIL）
"""

from __future__ import annotations

import json
import os
import re
from typing import Any

import requests

BASE_URL = "https://backend.lemonclean.com.tw"
LOGIN_URL = f"{BASE_URL}/login"
LOGOUT_URL = f"{BASE_URL}/logout"
PURCHASE_URL = f"{BASE_URL}/purchase"

HEADERS = {"User-Agent": "Mozilla/5.0"}

# 地區 → 後台帳號 key（新北使用台北帳號）
REGION_ACCOUNT_KEYS = {
    "台北": "taipei",
    "新北": "taipei",
    "台中": "taichung",
    "桃園": "taoyuan",
    "新竹": "hsinchu",
    "高雄": "kaohsiung",
}

# 後台訂單「待退狀態」isRefund radio 值（見 tool-system change_order.py）
REFUND_STATUS_FULL = "3"   # 已全額退款
REFUND_STATUS_PARTIAL = "2"  # 已部份退款
FULL_REFUND_TEXT = "已全額退款"

_PURCHASE_FILTER_PARAMS = {
    "keyword": "", "name": "", "phone": "", "orderNo": "",
    "date_s": "", "date_e": "", "clean_date_s": "", "clean_date_e": "",
    "paid_at_s": "", "paid_at_e": "", "refundDateS": "", "refundDateE": "",
    "buy": "", "area_id": "", "isCharge": "", "isRefund": "", "payway": "",
    "purchase_status": "", "progress_status": "", "invoiceStatus": "",
    "otherFee": "", "orderBy": "",
}


def get_backend_account(region_name: str) -> dict:
    key = REGION_ACCOUNT_KEYS.get(str(region_name or "").strip())
    if not key:
        raise RuntimeError(f"「{region_name}」沒有對應的檸檬後台帳號設定")

    email = password = ""
    try:
        import streamlit as st

        acc = st.secrets["accounts"][key]
        email = str(acc.get("email", "")).strip()
        password = str(acc.get("password", "")).strip()
    except Exception:
        pass

    email = email or os.environ.get(f"LEMON_{key.upper()}_EMAIL", "").strip()
    password = password or os.environ.get(f"LEMON_{key.upper()}_PASSWORD", "").strip()
    if not email or not password:
        raise RuntimeError(
            f"找不到「{region_name}」的檸檬後台帳密，請設定 "
            f"secrets [accounts.{key}] email/password 或環境變數 "
            f"LEMON_{key.upper()}_EMAIL／LEMON_{key.upper()}_PASSWORD"
        )
    return {"email": email, "password": password}


def login(session: requests.Session, email: str, password: str, timeout: int = 60) -> None:
    """登入失敗一律丟出 RuntimeError。"""
    res = session.get(LOGIN_URL, headers=HEADERS, allow_redirects=True, timeout=timeout)
    res.raise_for_status()
    match = (
        re.search(r'name=["\']_token["\'][^>]*value=["\']([^"\']+)', res.text)
        or re.search(r'value=["\']([^"\']+)["\'][^>]*name=["\']_token["\']', res.text)
    )
    token = match.group(1).strip() if match else ""
    if not token:
        raise RuntimeError("檸檬後台登入頁找不到 _token")
    res = session.post(
        LOGIN_URL,
        data={"_token": token, "email": email, "password": password},
        headers=HEADERS, allow_redirects=True, timeout=timeout,
    )
    res.raise_for_status()
    if "/login" in res.url.lower():
        raise RuntimeError(f"檸檬後台登入失敗（{email}），請確認帳密")


def logout(session: requests.Session) -> None:
    try:
        session.get(LOGOUT_URL, headers=HEADERS, allow_redirects=True, timeout=15)
    except Exception:
        pass


def login_region(region_name: str) -> requests.Session:
    acc = get_backend_account(region_name)
    session = requests.Session()
    login(session, acc["email"], acc["password"])
    return session


def extract_purchase_list(html: str) -> list[dict[str, Any]]:
    """取出訂單查詢頁內嵌的 Vue purchaseList JSON 的 data 陣列。"""
    source = str(html or "")
    start = source.find("purchaseList:")
    if start < 0:
        return []
    brace = source.find("{", start)
    if brace < 0:
        return []

    depth = 0
    in_string = False
    escaped = False
    end = -1
    for index in range(brace, len(source)):
        char = source[index]
        if in_string:
            if escaped:
                escaped = False
            elif char == "\\":
                escaped = True
            elif char == '"':
                in_string = False
            continue
        if char == '"':
            in_string = True
        elif char == "{":
            depth += 1
        elif char == "}":
            depth -= 1
            if depth == 0:
                end = index + 1
                break
    if end < 0:
        return []
    try:
        payload = json.loads(source[brace:end])
    except json.JSONDecodeError:
        return []
    data = payload.get("data", []) if isinstance(payload, dict) else []
    return data if isinstance(data, list) else []


def is_full_refund_item(item: dict[str, Any]) -> bool:
    """訂單資料的待退狀態是否為「已全額退款」（isRefund = 3）。"""
    for key in ("isRefund", "is_refund"):
        value = item.get(key)
        if value is None:
            continue
        text = str(value).strip()
        if text == REFUND_STATUS_FULL or text == FULL_REFUND_TEXT:
            return True
    return False


def get_refund_status(session: requests.Session, order_no: str, timeout: int = 60) -> dict:
    """
    以訂單編號查後台，回傳 {"found": bool, "full_refund": bool, "raw": isRefund值}。
    order_no 需為不含 -1/-2 子單後綴的 LC 訂單編號。
    """
    params = dict(_PURCHASE_FILTER_PARAMS, orderNo=order_no)
    res = session.get(PURCHASE_URL, params=params, headers=HEADERS,
                      allow_redirects=True, timeout=timeout)
    res.raise_for_status()
    if "/login" in res.url.lower():
        raise RuntimeError("檸檬後台登入已失效，請重新執行")

    items = extract_purchase_list(res.text)
    target = str(order_no).strip().upper()
    for item in items:
        if str(item.get("order_no") or "").strip().upper() == target:
            raw = item.get("isRefund", item.get("is_refund"))
            return {"found": True, "full_refund": is_full_refund_item(item), "raw": raw}
    return {"found": False, "full_refund": False, "raw": None}
