"""
Lemon Clean 薪資系統主控
風格參考：清潔承攬執行控制面板
"""

import streamlit as st
import yaml
from datetime import datetime

st.set_page_config(
    page_title="Lemon Clean 薪資系統",
    page_icon="🍋",
    layout="centered"
)

# ═══════════════════════════════════════
# 自訂 CSS（對應清潔承攬面板風格）
# ═══════════════════════════════════════
st.markdown("""
<style>
  .stApp { background: #f4f8fc; }
  #MainMenu, footer, header { visibility: hidden; }

  .app-title {
    font-size: 1.6rem;
    font-weight: 700;
    color: #0a4b6e;
    letter-spacing: 1px;
    text-align: center;
    margin-bottom: 24px;
  }

  .card {
    background: white;
    border-radius: 24px;
    padding: 24px;
    margin-bottom: 20px;
    box-shadow: 0 6px 16px rgba(0,32,48,0.06);
    border: 1px solid #e2edf2;
  }

  .card-title {
    font-size: 1.1rem;
    font-weight: 700;
    color: #164a5e;
    margin-bottom: 20px;
    padding-bottom: 12px;
    border-bottom: 2px solid #e7f0f5;
  }

  .field-label {
    color: #2a5770;
    font-weight: 600;
    font-size: 0.8rem;
    margin-bottom: 6px;
  }

  .stButton > button {
    background: #1f6c9e !important;
    color: white !important;
    border: none !important;
    border-radius: 40px !important;
    font-weight: 600 !important;
    font-size: 1rem !important;
  }

  .stButton > button:hover {
    background: #135b84 !important;
  }

  .log-section {
    background: #0c2835;
    color: #d7ecf5;
    border-radius: 24px;
    padding: 20px;
    margin-bottom: 20px;
    font-family: 'Courier New', monospace;
    border: 1px solid #254f60;
  }

  .log-header {
    display: flex;
    justify-content: space-between;
    align-items: center;
    margin-bottom: 12px;
    color: #b0d1dd;
    font-size: 0.85rem;
    padding-bottom: 10px;
    border-bottom: 1px solid #2c5a6a;
  }

  .log-entry {
    padding: 6px 0;
    border-bottom: 1px solid #1c4452;
    font-size: 0.8rem;
    color: #cde3ec;
    line-height: 1.5;
  }

  .log-time { color: #58c1d9; font-weight: 600; margin-right: 8px; }

  .region-card {
    background: #f8fcff;
    border-radius: 20px;
    padding: 16px;
    margin-bottom: 12px;
    border: 1px solid #d9eaf2;
  }

  .badge-ok {
    background: #2a8c5a; color: white;
    padding: 3px 10px; border-radius: 30px; font-size: 0.7rem;
  }
  .badge-err {
    background: #dc2626; color: white;
    padding: 3px 10px; border-radius: 30px; font-size: 0.7rem;
  }
</style>
""", unsafe_allow_html=True)


# ═══════════════════════════════════════
# 載入設定
# ═══════════════════════════════════════
@st.cache_data
def load_config():
    with open("config.yaml", "r", encoding="utf-8") as f:
        return yaml.safe_load(f)

config = load_config()
regions = config.get("regions", {})

# Session state 初始化
if "logs" not in st.session_state:
    st.session_state.logs = ["[--:--:--] 系統已就緒，請選擇作業..."]

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
# 執行設定卡片
# ═══════════════════════════════════════
st.markdown('<div class="card"><div class="card-title">⚙️ 執行設定</div>', unsafe_allow_html=True)

# 第一行：期別 + 系統選擇
col1, col2 = st.columns(2)
with col1:
    st.markdown('<div class="field-label">📆 執行期別</div>', unsafe_allow_html=True)
    period = st.text_input("期別", placeholder="例如：202504-1", label_visibility="collapsed")

with col2:
    st.markdown('<div class="field-label">🗂️ 執行系統</div>', unsafe_allow_html=True)
    system = st.selectbox(
        "系統",
        ["💰 金流對帳", "🧹 清潔承攬", "📦 其他承攬"],
        label_visibility="collapsed"
    )

# 功能選單（依系統動態變化）
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
        "薪資表整理",
        "00調薪",
        "01專員請款",
        "02儲值金",
        "標注新人實境期別",
        "03新人實境",
        "04新人實習",
        "05組長津貼",
        "工具包押金",
        "元大帳戶更新",
        "結算整理",
        "產生PDF",
    ],
    "📦 其他承攬": [
        "水洗前置",
        "家電前置",
        "全部前置",
        "水洗結算",
        "家電結算",
        "全部結算",
        "產出全部薪資單",
    ],
}

st.markdown('<div class="field-label">🎯 執行功能</div>', unsafe_allow_html=True)
selected_function = st.selectbox(
    "功能",
    function_map[system],
    label_visibility="collapsed"
)

# 第二行：區域 + 執行按鈕
col3, col4 = st.columns([2, 1])
with col3:
    st.markdown('<div class="field-label">🗺️ 執行區域</div>', unsafe_allow_html=True)
    region_names = {v["name"]: k for k, v in regions.items()}
    selected_name = st.selectbox("區域", list(region_names.keys()), label_visibility="collapsed")
    selected_key = region_names[selected_name]
    selected_region = regions[selected_key]

with col4:
    st.markdown('<div class="field-label">&nbsp;</div>', unsafe_allow_html=True)
    run = st.button("▶ 執行", use_container_width=True)

st.markdown('</div>', unsafe_allow_html=True)

# ═══════════════════════════════════════
# 執行邏輯
# ═══════════════════════════════════════
if run:
    if not period:
        add_log("請先輸入期別", "error")
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
log_html = '<div class="log-section">'
log_html += '''
<div class="log-header">
  <span>📋 執行日誌</span>
  <span style="background:#1e4757;padding:4px 12px;border-radius:30px;">即時更新</span>
</div>
'''
for entry in reversed(st.session_state.logs[-20:]):
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

col_a, col_b = st.columns([3, 1])
with col_b:
    if st.button("➕ 新增區域"):
        st.info("請在 config.yaml 新增區域")

for key, region in regions.items():
    name = region.get("name", key)
    fields = [
        ("根目錄", "root_folder_id"),
        ("金流對帳", "payment_reconciliation_id"),
        ("清潔承攬", "cleaning_contract_id"),
        ("其他承攬", "other_contract_id"),
    ]
    all_set = all(region.get(f[1]) for f in fields)
    badge = '<span class="badge-ok">已啟用</span>' if all_set else '<span class="badge-err">未完整</span>'

    detail_html = ""
    for label, field in fields:
        val = region.get(field, "")
        status = "✅" if val else "❌ 未設定"
        short = val[:25] + "..." if len(val) > 25 else val
        detail_html += (
            f'<div style="font-size:0.8rem;color:#3e6c87;margin:4px 0;">'
            f'<strong>{label}</strong>：{status} {short}</div>'
        )

    st.markdown(f"""
    <div class="region-card">
      <div style="display:flex;justify-content:space-between;align-items:center;margin-bottom:10px;">
        <strong style="color:#0a4b6e;font-size:1rem;">🏷️ {name}</strong>
        {badge}
      </div>
      {detail_html}
    </div>
    """, unsafe_allow_html=True)

st.markdown('</div>', unsafe_allow_html=True)
