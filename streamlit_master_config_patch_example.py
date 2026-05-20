"""
streamlit_master_config_patch_example.py

把這段整合到你的 Streamlit 主程式：

重點：
1. 不再自動建立永久設定表。
2. 直接讀取你畫面中的 LemonSalarySystem 主控檔。
3. 主控檔新增 / 編輯 / 刪除地區後，系統下次重新整理或重新執行就會讀到。
4. 排程儲存時，直接把主控檔的地區與根目錄ID轉進 scheduler config。
"""

import streamlit as st

from modules.config_manager import load_region_settings, to_scheduler_regions
from modules.scheduler_service import save_config_from_ui, start_scheduler_once

# 重要：請把 Google Sheet ID 放到 secrets 或環境變數
# .streamlit/secrets.toml:
# CONFIG_SHEET_ID = "你的 LemonSalarySystem Google Sheet ID"

start_scheduler_once(
    config_path="schedule_config.json",
    log_path="schedule_run_log.txt",
    interval_seconds=30,
)

st.subheader("地區主控設定")

settings = load_region_settings(
    tab_name="地區設定",
    config_yaml_path="config.yaml",
    log_fn=st.caption,
)

region_names = list(settings.keys())
selected_regions = st.multiselect(
    "套用地區",
    options=region_names,
    default=region_names,
)

st.dataframe([
    {
        "地區名稱": s.region_name,
        "根目錄ID": s.root_folder_id,
        "請款ID": s.billing_sheet_id,
        "薪資ID": s.salary_sheet_id,
        "名冊ID": s.roster_sheet_id,
    }
    for s in settings.values()
], use_container_width=True)

st.subheader("排程設定")

days_text = st.text_input("排程日期（每月幾號，逗號分隔）", value="10,20")
time_hhmm = st.text_input("執行時間（台北時區 HH:MM）", value="05:30")
enabled = st.checkbox("啟用排程", value=True)

if st.button("💾 儲存排程設定"):
    scheduler_regions = to_scheduler_regions(settings, enabled_regions=selected_regions)

    config = save_config_from_ui(
        days_text=days_text,
        time_hhmm=time_hhmm,
        regions=scheduler_regions,
        enabled=enabled,
        run_all_regions=True,
        path="schedule_config.json",
        timezone="Asia/Taipei",
    )

    st.success(f"排程設定已儲存：每月 {config.days} 日 {config.time_hhmm}")
