"""
Lemon Clean 薪資系統主控
"""

import streamlit as st
import yaml

st.set_page_config(
    page_title="Lemon Clean 薪資系統",
    page_icon="🍋",
    layout="wide"
)

# ═══════════════════════════════════════
# 載入地區設定
# ═══════════════════════════════════════
@st.cache_data
def load_config():
    with open("config.yaml", "r", encoding="utf-8") as f:
        return yaml.safe_load(f)

config = load_config()
regions = config.get("regions", {})

# ═══════════════════════════════════════
# 側邊欄
# ═══════════════════════════════════════
with st.sidebar:
    st.title("🍋 Lemon Clean")
    st.divider()

    region_options = {v["name"]: k for k, v in regions.items()}
    selected_region_name = st.selectbox("選擇地區", list(region_options.keys()))
    selected_region_key = region_options[selected_region_name]
    selected_region = regions[selected_region_key]

    period = st.text_input("執行期別", placeholder="例如：202504-1")

    if period:
        half = "上半月" if period.endswith("-1") else "下半月"
        st.caption(f"📅 {period}（{half}）")

    st.divider()
    st.caption("⚙️ 地區設定")
    for key, label in [
        ("payment_reconciliation_id", "金流對帳"),
        ("cleaning_contract_id", "清潔承攬"),
        ("other_contract_id", "其他承攬"),
        ("root_folder_id", "根目錄"),
    ]:
        val = selected_region.get(key, "")
        status = "✅" if val else "❌ 未設定"
        st.caption(f"{label}：{status}")

# ═══════════════════════════════════════
# 主要分頁
# ═══════════════════════════════════════
tab1, tab2, tab3 = st.tabs(["💰 金流對帳", "🧹 清潔承攬", "📦 其他承攬"])

# ───────────────────────────────────────
# 💰 金流對帳
# ───────────────────────────────────────
with tab1:
    st.header("💰 金流對帳")

    if not period:
        st.warning("請先在左側輸入期別")
    else:
        col1, col2 = st.columns(2)

        with col1:
            st.subheader("① 建立期別")
            if st.button("📁 建立期別資料夾與檔案", use_container_width=True):
                with st.spinner("建立中..."):
                    try:
                        from modules.payment_reconciliation import create_period_files
                        results = create_period_files(
                            selected_region["root_folder_id"],
                            period,
                            selected_region_name
                        )
                        st.success(f"✅ 建立完成，共 {len(results)} 個檔案")
                    except Exception as e:
                        st.error(f"❌ {e}")

            st.subheader("② 訂單轉檔")
            if st.button("🔄 期別訂單轉檔", use_container_width=True):
                st.info("開發中")

            st.subheader("③ 搬運到範本")
            if st.button("📥 訂單搬運到範本", use_container_width=True):
                with st.spinner("搬運中..."):
                    try:
                        from modules.payment_reconciliation import copy_orders_to_template
                        count = copy_orders_to_template(
                            selected_region["payment_reconciliation_id"],
                            period
                        )
                        st.success(f"✅ 搬運完成：{count} 筆")
                    except Exception as e:
                        st.error(f"❌ {e}")

        with col2:
            st.subheader("④ 範本加工")
            if st.button("🔧 範本加工", use_container_width=True):
                with st.spinner("加工中..."):
                    try:
                        from modules.payment_reconciliation import process_template
                        result = process_template(
                            selected_region["payment_reconciliation_id"]
                        )
                        st.success(
                            f"✅ 排序 {result['sort_count']} 筆，"
                            f"標記異常 {result['mark_count']} 筆"
                        )
                    except Exception as e:
                        st.error(f"❌ {e}")

            st.subheader("⑤ 分類搬運")
            if st.button("📂 分類搬運到明細", use_container_width=True):
                with st.spinner("分類中..."):
                    try:
                        from modules.payment_reconciliation import copy_classified_data
                        counts = copy_classified_data(
                            selected_region["payment_reconciliation_id"],
                            selected_region["cleaning_contract_id"],
                            selected_region["other_contract_id"],
                            period
                        )
                        st.success("✅ 分類完成")
                        for k, v in counts.items():
                            if v > 0:
                                st.write(f"　{k}：{v} 筆")
                    except Exception as e:
                        st.error(f"❌ {e}")

            st.subheader("⑥ 退款／預收")
            if st.button("↩️ 搬運退款＋預收", use_container_width=True):
                st.info("開發中")

            st.subheader("⑦ 發票／藍新")
            if st.button("🧾 搬運發票＋藍新", use_container_width=True):
                st.info("開發中")

# ───────────────────────────────────────
# 🧹 清潔承攬
# ───────────────────────────────────────
with tab2:
    st.header("🧹 清潔承攬")

    if not period:
        st.warning("請先在左側輸入期別")
    else:
        st.info("金流對帳完成後開始開發")

# ───────────────────────────────────────
# 📦 其他承攬
# ───────────────────────────────────────
with tab3:
    st.header("📦 其他承攬")

    if not period:
        st.warning("請先在左側輸入期別")
    else:
        st.info("清潔承攬完成後開始開發")
