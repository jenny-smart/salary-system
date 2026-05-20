"""
modules/config_manager.py
主控檔 / 永久設定表讀取工具

目的：
1. 優先讀取 CONFIG_SHEET_ID 指定的 Google Sheet。
2. 不再因為 Google Drive 空間已滿而嘗試自動建立永久設定表。
3. 若沒有 CONFIG_SHEET_ID，才退回讀 config.yaml。
4. 地區與相關 ID 可集中放在主控檔，新增 / 編輯 / 刪除時只改主控檔即可。

主控檔格式建議：
工作表名稱：地區設定

A: 地區名稱
B: 根目錄ID
C: 請款ID
D: 薪資ID
E: 名冊ID
"""

from __future__ import annotations

import os
from dataclasses import dataclass
from typing import Dict, List

import yaml
import gspread

from modules.auth import get_credentials


DEFAULT_CONFIG_YAML = "config.yaml"
DEFAULT_CONFIG_SHEET_TAB = "地區設定"


@dataclass
class RegionSetting:
    region_name: str
    root_folder_id: str = ""
    billing_sheet_id: str = ""
    salary_sheet_id: str = ""
    roster_sheet_id: str = ""


def _open_gspread_client():
    creds = get_credentials()
    return gspread.authorize(creds)


def _read_from_google_sheet(config_sheet_id: str, tab_name: str = DEFAULT_CONFIG_SHEET_TAB) -> Dict[str, RegionSetting]:
    gc = _open_gspread_client()
    ss = gc.open_by_key(config_sheet_id)
    ws = ss.worksheet(tab_name)

    rows = ws.get_all_records()
    settings: Dict[str, RegionSetting] = {}

    for row in rows:
        region = str(row.get("地區名稱", "")).strip()
        if not region:
            continue

        settings[region] = RegionSetting(
            region_name=region,
            root_folder_id=str(row.get("根目錄ID", "")).strip(),
            billing_sheet_id=str(row.get("請款ID", "")).strip(),
            salary_sheet_id=str(row.get("薪資ID", "")).strip(),
            roster_sheet_id=str(row.get("名冊ID", "")).strip(),
        )

    return settings


def _read_from_yaml(path: str = DEFAULT_CONFIG_YAML) -> Dict[str, RegionSetting]:
    with open(path, "r", encoding="utf-8") as f:
        data = yaml.safe_load(f) or {}

    raw_regions = data.get("regions", {})
    settings: Dict[str, RegionSetting] = {}

    # 支援兩種格式：
    # regions:
    #   台北:
    #     root_folder_id: xxx
    # 或
    # regions:
    #   - region_name: 台北
    if isinstance(raw_regions, dict):
        items = raw_regions.items()
        for region, cfg in items:
            cfg = cfg or {}
            settings[str(region)] = RegionSetting(
                region_name=str(region),
                root_folder_id=str(cfg.get("root_folder_id", "")).strip(),
                billing_sheet_id=str(cfg.get("billing_sheet_id", "")).strip(),
                salary_sheet_id=str(cfg.get("salary_sheet_id", "")).strip(),
                roster_sheet_id=str(cfg.get("roster_sheet_id", "")).strip(),
            )
    elif isinstance(raw_regions, list):
        for cfg in raw_regions:
            region = str(cfg.get("region_name", "")).strip()
            if not region:
                continue
            settings[region] = RegionSetting(
                region_name=region,
                root_folder_id=str(cfg.get("root_folder_id", "")).strip(),
                billing_sheet_id=str(cfg.get("billing_sheet_id", "")).strip(),
                salary_sheet_id=str(cfg.get("salary_sheet_id", "")).strip(),
                roster_sheet_id=str(cfg.get("roster_sheet_id", "")).strip(),
            )

    return settings


def load_region_settings(
    *,
    config_sheet_id: str | None = None,
    config_yaml_path: str = DEFAULT_CONFIG_YAML,
    tab_name: str = DEFAULT_CONFIG_SHEET_TAB,
    log_fn=None,
) -> Dict[str, RegionSetting]:
    """
    讀取地區設定。

    優先順序：
    1. 參數 config_sheet_id
    2. 環境變數 CONFIG_SHEET_ID
    3. Streamlit secrets CONFIG_SHEET_ID
    4. config.yaml

    注意：
    - 本函式不會自動建立 Google Sheet。
    - 因此不會再因 Drive 空間已滿而一直報「無法建立永久設定表」。
    """
    def log(msg: str):
        if log_fn:
            log_fn(msg)

    sheet_id = config_sheet_id or os.getenv("CONFIG_SHEET_ID")

    if not sheet_id:
        try:
            import streamlit as st
            sheet_id = st.secrets.get("CONFIG_SHEET_ID", "")
        except Exception:
            sheet_id = ""

    if sheet_id:
        try:
            settings = _read_from_google_sheet(sheet_id, tab_name=tab_name)
            log(f"✅ 已讀取主控設定表：{len(settings)} 個地區")
            return settings
        except Exception as e:
            log(f"⚠️ 主控設定表讀取失敗，改用 {config_yaml_path}：{e}")

    settings = _read_from_yaml(config_yaml_path)
    log(f"✅ 已讀取 {config_yaml_path}：{len(settings)} 個地區")
    return settings


def get_region_setting(region_name: str, **kwargs) -> RegionSetting:
    settings = load_region_settings(**kwargs)
    if region_name not in settings:
        raise KeyError(f"找不到地區設定：{region_name}")
    return settings[region_name]


def to_scheduler_regions(settings: Dict[str, RegionSetting], enabled_regions: List[str] | None = None) -> list[dict]:
    """
    將主控地區設定轉成 scheduler_service.save_config_from_ui 可用格式。
    """
    enabled_set = set(enabled_regions or settings.keys())

    return [
        {
            "region_name": s.region_name,
            "root_folder_id": s.root_folder_id,
            "enabled": s.region_name in enabled_set and bool(s.root_folder_id),
        }
        for s in settings.values()
    ]
