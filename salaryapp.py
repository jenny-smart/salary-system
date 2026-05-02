"""
Lemon Clean 薪資系統主控
"""

import streamlit as st
import yaml
import os
from datetime import datetime
from pathlib import Path

st.set_page_config(
    page_title="Lemon Clean 薪資系統",
    page_icon="🍋",
    layout="centered"
)

st.markdown("""
<style>
  .stApp { background: #f4f8fc; }
  #MainMenu, footer, header { visibility: hidden; }

  .app-title {
    font-size: 1.4rem;
    font-weight: 700;
    color: #0a4b6e;
    letter-spacing: 1px;
    text-align: center;
    margin-bottom: 16px;
  }

  .card {
    background: white;
    border-radius: 20px;
    padding: 16px 20px;
    margin-bottom: 14px;
    box-shadow: 0 4px 12px rgba(0,32,48,0.06);
    border: 1px solid #e2edf2;
  }

  .card-title {
    font-size: 0.95rem;
    font-weight: 700;
    color: #164a5e;
    margin-bottom: 12px;
    padding-bottom: 8px;
    border-bottom: 1.5px solid #e7f0f5;
  }

  .field-label {
    color: #2a5770;
    font-weight: 600;
    font-size: 0.75rem;
    margin-bottom: 4px;
  }

  .stButton > button {
    background: #1f6c9e !important;
    color: white !important;
    border: none !important;
    border-radius: 40px !important;
    font-weight: 600 !important;
    font-size: 0.9rem !important;
    padding: 8px 20px !important;
  }
  .stButton > button:hover { background: #135b84 !important; }

  .btn-sm > button {
    font-size: 0.75rem !important;
    padding: 4px 12px !important;
  }

  .log-section {
    background: #0c2835;
    color: #d7ecf5;
    border-radius: 20px;
    padding: 14px 16px;
    margin-bottom: 14px;
    font-family: 'Courier New', monospace;
    border: 1px solid #254f60;
  }

  .log-header {
    display: flex;
    justify-content: space-between;
    align-items: center;
    margin-bottom: 10px;
    color: #b0d1dd;
    font-size: 0.8rem;
    padding-bottom: 8px;
    border-bottom: 1px solid #2c5a6a;
  }

  .log-entry {
    padding: 4px 0;
    border-bottom: 1px solid #1c4452;
    font-size: 0.75rem;
    color: #cde3ec;
    line-height: 1.4;
  }

  .region-card {
    background: #f8fcff;
    border-radius: 16px;
    padding: 12px 14px;
    margin-bottom: 10px;
    border: 1px solid #d9eaf2;
  }

  .region-header {
    display: flex;
    justify-content: space-between;
    align-items: center;
    margin-bottom: 6px;
  }

  .badge-ok {
    background: #2a8c5a; color: white;
    padding: 2px 8px; border-radius: 20px; font-size: 0.65rem;
  }
  .badge-err {
    background: #dc2626; color: white;
    padding: 2px 8px; border-radius: 20px; font-size: 0.65rem;
  }

  .detail-row {
    font-size: 0.75rem;
    color: #3e6c87;
    margin: 3px 0;
  }
</style>
""", unsafe_allow_html=True)


# ═══════════════════════════════════════
# 設定檔讀寫
# ═══════════════════════════════════════
CONFIG_PATH = "config.yaml"

def load_config():
    with open(CONFIG_PATH, "r", encoding="utf-8") as f:
        return yaml.safe_load(f)

def save_config(cfg):
    with open(CONFIG_PATH, "w", encoding="utf-8") as f:
        yaml.dump(cfg, f, allow_unicode=True, default_flow_style=False)
    st.cache_data.clear()

config = load_config()
regions = config.get("regions", {})

# Session state
if "logs" not in st.session_state:
    st.session_state.logs = ["[--:--:--] 系統已就緒，請選擇作業..."]
if "editing_region" not in st.session_state:
    st.session_state.editing_region = None
if "adding_region" not in st.session_state:
    st.session_state.adding_region = False

def add_log(message: str, level: str = "info"):
    now = datetime.now().strftime("%H:%M:%S")
    icons = {"info": "🔵", "success": "✅", "error": "❌", "warning": "⚠️"}
    icon = icons.get(level, "🔵")
    st.session_state.logs.append(f"[{now}] {icon} {message}")
    if len(st.session_state.logs) > 100:
        st.session_state.logs = st.session_state.logs[-100:]


# ═══════════════════════════════════════
# 主標題
# ═══════════════════════════════════════
st.markdown('<div class="app-title">🍋 Lemon Clean 薪資系統</div>', unsafe_allow_html=True)


# ═══════════════════════════════════════
# 執行設定
# ═══════════════════════════════════════
st.markdown('<div class="card"><div class="card-title">⚙️ 執行設定</div>', unsafe_allow_html=True)

c1, c2 = st.columns(2)
with c1:
    st.markdown('<div class="field-label">📆 執行期別</div>', unsafe_allow_html=True)
    period = st.text_input("期別", placeholder="202504-1", label_visibility="collapsed", key="period")

with c2:
    st.markdown('<div class="field-label">🗂️ 執行系統</div>', unsafe_allow_html=True)
    system = st.selectbox(
        "系統",
        ["💰 金流對帳", "🧹 清潔承攬", "📦 其他承攬"],
        label_visibility="collapsed",
        key="system"
    )

function_map = {
    "💰 金流對帳": [
        "① 建立期別資料夾與檔案",
        "② 期別訂單轉檔",
        "③ 訂單搬運到範本",
        "④ 範本加工",
        "⑤ 分類搬運",
        "⑥ 搬運退款＋預收",
        "⑦ 搬運發票＋藍新",
    ],
    "🧹 清潔承攬": [
        "薪資表整理", "00調薪", "01專員請款", "02儲值金",
        "標注新人實境期別", "03新人實境", "04新人實習",
        "05組長津貼", "工具包押金", "元大帳戶更新", "結算整理", "產生PDF",
    ],
    "📦 其他承攬": [
        "水洗前置", "家電前置", "全部前置",
        "水洗結算", "家電結算", "全部結算", "產出全部薪資單",
    ],
}

st.markdown('<div class="field-label">🎯 執行功能</div>', unsafe_allow_html=True)
selected_function = st.selectbox("功能", function_map[system], label_visibility="collapsed", key="func")

c3, c4 = st.columns([2, 1])
with c3:
    st.markdown('<div class="field-label">🗺️ 執行區域</div>', unsafe_allow_html=True)
    region_names = {v["name"]: k for k, v in regions.items()}
    if region_names:
        selected_name = st.selectbox("區域", list(region_names.keys()), label_visibility="collapsed", key="region")
        selected_key = region_names[selected_name]
        selected_region = regions[selected_key]
    else:
        st.caption("尚未設定任何區域，請至下方新增")
        selected_name = None
        selected_region = {}

with c4:
    st.markdown('<div class="field-label">&nbsp;</div>', unsafe_allow_html=True)
    run = st.button("▶ 執行", use_container_width=True)

st.markdown('</div>', unsafe_allow_html=True)


# ═══════════════════════════════════════
# 執行邏輯
# ═══════════════════════════════════════
if run:
    if not period:
        add_log("請先輸入期別", "error")
    elif not selected_name:
        add_log("請先新增區域設定", "error")
    else:
        half = "上半月" if period.endswith("-1") else "下半月"
        add_log(f"執行【{selected_name}】{half} {selected_function}，期別：{period}")

        with st.spinner("執行中..."):
            try:
                if system == "💰 金流對帳":
                    if "建立期別" in selected_function:
                        from modules.payment_reconciliation import create_period_files
                        results = create_period_files(
                            selected_region["root_folder_id"], period, selected_name
                        )
                        add_log(f"建立完成，共 {len(results)} 個檔案", "success")

                    elif "訂單搬運" in selected_function:
                        from modules.payment_reconciliation import copy_orders_to_template
                        count = copy_orders_to_template(
                            selected_region["payment_reconciliation_id"], period
                        )
                        add_log(f"搬運完成：{count} 筆", "success")

                    elif "範本加工" in selected_function:
                        from modules.payment_reconciliation import process_template
                        result = process_template(selected_region["payment_reconciliation_id"])
                        add_log(
                            f"加工完成：排序 {result['sort_count']} 筆，"
                            f"標記異常 {result['mark_count']} 筆", "success"
                        )

                    elif "分類搬運" in selected_function:
                        from modules.payment_reconciliation import copy_classified_data
                        counts = copy_classified_data(
                            selected_region["payment_reconciliation_id"],
                            selected_region["cleaning_contract_id"],
                            selected_region["other_contract_id"],
                            period
                        )
                        add_log("分類搬運完成", "success")
                        for k, v in counts.items():
                            if v > 0:
                                add_log(f"　{k}：{v} 筆")
                    else:
                        add_log(f"{selected_function} 開發中", "warning")
                else:
                    add_log(f"{system} {selected_function} 開發中", "warning")

            except Exception as e:
                add_log(f"執行失敗：{e}", "error")

        st.rerun()


# ═══════════════════════════════════════
# 執行日誌
# ═══════════════════════════════════════
log_html = '<div class="log-section"><div class="log-header"><span>📋 執行日誌</span><span style="background:#1e4757;padding:3px 10px;border-radius:20px;font-size:0.75rem;">即時更新</span></div>'
for entry in reversed(st.session_state.logs[-15:]):
    log_html += f'<div class="log-entry">{entry}</div>'
log_html += '</div>'
st.markdown(log_html, unsafe_allow_html=True)

if st.button("🗑️ 清除日誌"):
    st.session_state.logs = ["[--:--:--] 日誌已清除"]
    st.rerun()


# ═══════════════════════════════════════
# 區域設定
# ═══════════════════════════════════════
st.markdown('<div class="card"><div class="card-title">⚙️ 區域設定</div>', unsafe_allow_html=True)

# 新增區域按鈕
col_hdr, col_add = st.columns([3, 1])
with col_add:
    if st.button("➕ 新增區域", use_container_width=True):
        st.session_state.adding_region = True
        st.session_state.editing_region = None

# 新增區域表單
if st.session_state.adding_region:
    with st.form("add_region_form"):
        st.markdown("**新增區域**")
        fc1, fc2 = st.columns(2)
        with fc1:
            new_key = st.text_input("區域代碼（英文）", placeholder="taipei")
            new_name = st.text_input("區域名稱", placeholder="台北")
        with fc2:
            new_root = st.text_input("根目錄 ID")
            new_payment = st.text_input("金流對帳 ID")
        fc3, fc4 = st.columns(2)
        with fc3:
            new_cleaning = st.text_input("清潔承攬 ID")
        with fc4:
            new_other = st.text_input("其他承攬 ID")

        s1, s2 = st.columns(2)
        with s1:
            submitted = st.form_submit_button("💾 儲存", use_container_width=True)
        with s2:
            cancelled = st.form_submit_button("✕ 取消", use_container_width=True)

        if submitted:
            if not new_key or not new_name:
                st.error("區域代碼和名稱為必填")
            else:
                config["regions"][new_key] = {
                    "name": new_name,
                    "root_folder_id": new_root,
                    "payment_reconciliation_id": new_payment,
                    "cleaning_contract_id": new_cleaning,
                    "other_contract_id": new_other,
                }
                save_config(config)
                add_log(f"新增區域：{new_name}", "success")
                st.session_state.adding_region = False
                st.rerun()

        if cancelled:
            st.session_state.adding_region = False
            st.rerun()

# 現有區域列表
fields = [
    ("根目錄", "root_folder_id"),
    ("金流對帳", "payment_reconciliation_id"),
    ("清潔承攬", "cleaning_contract_id"),
    ("其他承攬", "other_contract_id"),
]

for key, region in list(regions.items()):
    name = region.get("name", key)
    all_set = all(region.get(f[1]) for f in fields)
    badge = '<span class="badge-ok">已啟用</span>' if all_set else '<span class="badge-err">未完整</span>'

    # 編輯模式
    if st.session_state.editing_region == key:
        with st.form(f"edit_{key}"):
            st.markdown(f"**編輯：{name}**")
            ec1, ec2 = st.columns(2)
            with ec1:
                e_name = st.text_input("區域名稱", value=name)
                e_root = st.text_input("根目錄 ID", value=region.get("root_folder_id", ""))
                e_payment = st.text_input("金流對帳 ID", value=region.get("payment_reconciliation_id", ""))
            with ec2:
                e_cleaning = st.text_input("清潔承攬 ID", value=region.get("cleaning_contract_id", ""))
                e_other = st.text_input("其他承攬 ID", value=region.get("other_contract_id", ""))

            es1, es2 = st.columns(2)
            with es1:
                save_edit = st.form_submit_button("💾 儲存", use_container_width=True)
            with es2:
                cancel_edit = st.form_submit_button("✕ 取消", use_container_width=True)

            if save_edit:
                config["regions"][key] = {
                    "name": e_name,
                    "root_folder_id": e_root,
                    "payment_reconciliation_id": e_payment,
                    "cleaning_contract_id": e_cleaning,
                    "other_contract_id": e_other,
                }
                save_config(config)
                add_log(f"更新區域：{e_name}", "success")
                st.session_state.editing_region = None
                st.rerun()

            if cancel_edit:
                st.session_state.editing_region = None
                st.rerun()

    else:
        # 顯示模式
        detail_html = ""
        for label, field in fields:
            val = region.get(field, "")
            status = "✅" if val else "❌"
            short = val[:22] + "..." if len(val) > 22 else val
            detail_html += (
                f'<div class="detail-row">'
                f'<strong>{label}</strong>：{status} {short}</div>'
            )

        st.markdown(f"""
        <div class="region-card">
          <div class="region-header">
            <strong style="color:#0a4b6e;">🏷️ {name}</strong>
            {badge}
          </div>
          {detail_html}
        </div>
        """, unsafe_allow_html=True)

        rc1, rc2, rc3 = st.columns([3, 1, 1])
        with rc2:
            if st.button("📝 編輯", key=f"edit_btn_{key}", use_container_width=True):
                st.session_state.editing_region = key
                st.session_state.adding_region = False
                st.rerun()
        with rc3:
            if st.button("🗑️ 刪除", key=f"del_btn_{key}", use_container_width=True):
                del config["regions"][key]
                save_config(config)
                add_log(f"刪除區域：{name}", "warning")
                st.rerun()

st.markdown('</div>', unsafe_allow_html=True)
