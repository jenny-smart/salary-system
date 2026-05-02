"""
Lemon Clean 薪資系統主控
"""

import streamlit as st
import yaml
from datetime import datetime
from modules.period_utils import get_auto_period, is_first_half

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
    font-size: 1.4rem; font-weight: 700; color: #0a4b6e;
    letter-spacing: 1px; text-align: center; margin-bottom: 16px;
  }
  .card {
    background: white; border-radius: 20px; padding: 16px 20px;
    margin-bottom: 14px; box-shadow: 0 4px 12px rgba(0,32,48,0.06);
    border: 1px solid #e2edf2;
  }
  .card-title {
    font-size: 0.95rem; font-weight: 700; color: #164a5e;
    margin-bottom: 12px; padding-bottom: 8px;
    border-bottom: 1.5px solid #e7f0f5;
  }
  .field-label { color: #2a5770; font-weight: 600; font-size: 0.75rem; margin-bottom: 4px; }
  .stButton > button {
    background: #1f6c9e !important; color: white !important;
    border: none !important; border-radius: 40px !important;
    font-weight: 600 !important; font-size: 0.9rem !important;
  }
  .stButton > button:hover { background: #135b84 !important; }
  .log-box {
    background: #0c2835; color: #d7ecf5; border-radius: 20px;
    padding: 14px 16px; margin-bottom: 14px;
    font-family: 'Courier New', monospace; border: 1px solid #254f60;
  }
  .log-header {
    display: flex; justify-content: space-between; align-items: center;
    margin-bottom: 10px; color: #b0d1dd; font-size: 0.8rem;
    padding-bottom: 8px; border-bottom: 1px solid #2c5a6a;
  }
  .log-scroll { max-height: 300px; overflow-y: auto; }
  .log-entry {
    padding: 4px 0; border-bottom: 1px solid #1c4452;
    font-size: 0.75rem; color: #cde3ec; line-height: 1.4;
  }
  .log-entry.success { color: #6ee7b7; }
  .log-entry.error   { color: #fca5a5; }
  .log-entry.warning { color: #fcd34d; }
  .region-card {
    background: #f8fcff; border-radius: 16px; padding: 12px 14px;
    margin-bottom: 10px; border: 1px solid #d9eaf2;
  }
  .badge-ok  { background: #2a8c5a; color: white; padding: 2px 8px; border-radius: 20px; font-size: 0.65rem; }
  .badge-err { background: #dc2626; color: white; padding: 2px 8px; border-radius: 20px; font-size: 0.65rem; }
  .detail-row { font-size: 0.75rem; color: #3e6c87; margin: 3px 0; }
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
regions = config.get("regions", [])

if "logs" not in st.session_state:
    st.session_state.logs = ["[--:--:--] 系統已就緒，請選擇作業..."]
if "editing_region" not in st.session_state:
    st.session_state.editing_region = None
if "adding_region" not in st.session_state:
    st.session_state.adding_region = False
if "pending_run" not in st.session_state:
    st.session_state.pending_run = False
if "run_params" not in st.session_state:
    st.session_state.run_params = {}


def add_log(message: str, level: str = "info"):
    now = datetime.now().strftime("%H:%M:%S")
    icons = {"info": "🔵", "success": "✅", "error": "❌", "warning": "⚠️"}
    icon = icons.get(level, "🔵")
    st.session_state.logs.append(f"[{now}] {icon} {message}")
    if len(st.session_state.logs) > 500:
        st.session_state.logs = st.session_state.logs[-500:]
    if "log_placeholder" in st.session_state:
        _render_log(st.session_state.log_placeholder)


def _render_log(placeholder):
    entries = list(st.session_state.logs)
    html = '<div class="log-box"><div class="log-header"><span>📋 執行日誌</span><span style="background:#1e4757;padding:3px 10px;border-radius:20px;font-size:0.75rem;">即時更新</span></div><div class="log-scroll">'
    for entry in reversed(entries):
        css = "log-entry"
        if "✅" in entry:
            css += " success"
        elif "❌" in entry:
            css += " error"
        elif "⚠️" in entry:
            css += " warning"
        html += f'<div class="{css}">{entry}</div>'
    html += '</div></div>'
    placeholder.markdown(html, unsafe_allow_html=True)


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
    period = st.text_input("期別", value=get_auto_period(), label_visibility="collapsed", key="period")

with c2:
    st.markdown('<div class="field-label">🗂️ 執行系統</div>', unsafe_allow_html=True)
    system = st.selectbox(
        "系統",
        ["💰 金流對帳", "🧹 清潔承攬", "📦 其他承攬"],
        label_visibility="collapsed", key="system"
    )

function_map = {
    "💰 金流對帳": [
        "① 建立期別資料夾與檔案",
        "② 期別訂單轉檔（xlsx → Google Sheet）",
        "② 金流對帳轉檔（已退款/預收/發票/藍新）",
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
selected_function = st.selectbox(
    "功能", function_map[system], label_visibility="collapsed", key="func"
)

c3, c4 = st.columns([2, 1])
with c3:
    st.markdown('<div class="field-label">🗺️ 執行地區</div>', unsafe_allow_html=True)
    region_names = [r["name"] for r in regions]
    if region_names:
        selected_name = st.selectbox(
            "地區", region_names, label_visibility="collapsed", key="region"
        )
        selected_region = next((r for r in regions if r["name"] == selected_name), {})
    else:
        st.caption("尚未設定任何地區")
        selected_name = None
        selected_region = {}

with c4:
    st.markdown('<div class="field-label">&nbsp;</div>', unsafe_allow_html=True)
    if st.button("▶ 執行", use_container_width=True):
        st.session_state.pending_run = True
        st.session_state.run_params = {
            "period": period,
            "system": system,
            "selected_function": selected_function,
            "selected_name": selected_name,
            "selected_region": selected_region,
        }

st.markdown('</div>', unsafe_allow_html=True)


# ═══════════════════════════════════════
# 日誌區塊
# ═══════════════════════════════════════
log_placeholder = st.empty()
st.session_state.log_placeholder = log_placeholder
_render_log(log_placeholder)

col_clear, _ = st.columns([1, 3])
with col_clear:
    if st.button("🗑️ 清除日誌"):
        st.session_state.logs = ["[--:--:--] 日誌已清除"]
        _render_log(log_placeholder)


# ═══════════════════════════════════════
# 執行邏輯
# ═══════════════════════════════════════
if st.session_state.pending_run:
    st.session_state.pending_run = False

    p = st.session_state.run_params
    _period = p.get("period", "")
    _system = p.get("system", "")
    _func = p.get("selected_function", "")
    _name = p.get("selected_name")
    _region = p.get("selected_region", {})

    if not _period:
        add_log("請先輸入期別", "error")
    elif not _name:
        add_log("請先新增地區設定", "error")
    else:
        root_id = _region.get("root_folder_id", "")
        if not root_id:
            add_log(f"【{_name}】尚未設定根目錄 ID", "error")
        else:
            half = "上半月" if is_first_half(_period) else "下半月"
            add_log(f"執行【{_name}】{half} {_func}，期別：{_period}")

            try:
                if _system == "💰 金流對帳":

                    if "① 建立期別" in _func:
                        from modules.payment_reconciliation import create_period
                        result = create_period(root_id, _period, _name, add_log)
                        ok = len([v for v in result.values() if v and v != result.get("period_folder_id")])
                        add_log(f"建立完成，共複製 {ok} 個檔案", "success")

                    elif "期別訂單轉檔" in _func:
                        from modules.payment_reconciliation import convert_order_file
                        convert_order_file(root_id, _period, _name, add_log)
                        add_log("期別訂單轉檔完成", "success")

                    elif "金流對帳轉檔" in _func:
                        from modules.payment_reconciliation import convert_payment_file
                        result = convert_payment_file(root_id, _period, _name, add_log)
                        ok = len([v for v in result.values() if v])
                        add_log(f"金流對帳轉檔完成，共 {ok} 個檔案", "success")

                    elif "③ 訂單搬運" in _func:
                        from modules.payment_reconciliation import copy_orders_to_template
                        count = copy_orders_to_template(root_id, _period, _name, add_log)
                        add_log(f"搬運完成：{count} 筆", "success")

                    elif "④ 範本加工" in _func:
                        from modules.payment_reconciliation import process_template
                        result = process_template(root_id, _period, _name, add_log)
                        add_log(
                            f"加工完成：排序 {result['sort_count']} 筆，"
                            f"異常 {result['mark_count']} 筆，"
                            f"拆解新增 {result['expand_count']} 列",
                            "success"
                        )
                        for w in result.get("warnings", []):
                            add_log(w, "warning")

                    elif "⑤ 分類搬運" in _func:
                        from modules.payment_reconciliation import copy_classified_data
                        counts = copy_classified_data(root_id, _period, _name, add_log)
                        add_log("分類搬運完成", "success")
                        for k, v in counts.items():
                            if v > 0:
                                add_log(f"　{k}：{v} 筆")

                    elif "⑥ 搬運退款" in _func:
                        from modules.payment_reconciliation import move_refund_and_prepaid
                        counts = move_refund_and_prepaid(root_id, _period, _name, add_log)
                        add_log("退款＋預收搬運完成", "success")
                        for k, v in counts.items():
                            add_log(f"　{k}：{v} 筆")

                    elif "⑦ 搬運發票" in _func:
                        from modules.payment_reconciliation import move_invoice_and_bluenew
                        counts = move_invoice_and_bluenew(root_id, _period, _name, add_log)
                        add_log("發票＋藍新搬運完成", "success")
                        for k, v in counts.items():
                            if v > 0:
                                add_log(f"　{k}：{v} 筆")

                else:
                    add_log(f"{_system} {_func} 開發中", "warning")

            except Exception as e:
                import traceback
                add_log(f"執行失敗：{e}", "error")
                add_log(traceback.format_exc(), "error")


# ═══════════════════════════════════════
# 排程設定
# ═══════════════════════════════════════
st.markdown('<div class="card"><div class="card-title">⏰ 排程設定</div>', unsafe_allow_html=True)

schedule = config.get("schedule", {})
sc1, sc2 = st.columns(2)
with sc1:
    st.markdown('<div class="field-label">📅 排程日期（每月幾號，逗號分隔）</div>', unsafe_allow_html=True)
    sched_days = st.text_input(
        "日期", value=",".join(str(d) for d in schedule.get("days", [10, 25])),
        label_visibility="collapsed", key="sched_days"
    )
with sc2:
    st.markdown('<div class="field-label">🕘 執行時間（台北時區 HH:MM）</div>', unsafe_allow_html=True)
    sched_time = st.text_input(
        "時間", value=schedule.get("time", "09:00"),
        label_visibility="collapsed", key="sched_time"
    )

sc3, sc4 = st.columns(2)
with sc3:
    sched_all = st.checkbox("套用全部地區", value=schedule.get("all_regions", True))
with sc4:
    sched_enabled = st.checkbox("啟用排程", value=schedule.get("enabled", False))

if st.button("💾 儲存排程設定", use_container_width=True):
    try:
        days = [int(d.strip()) for d in sched_days.split(",") if d.strip()]
        if not days:
            raise ValueError("請輸入排程日期")
        config["schedule"] = {
            "enabled": sched_enabled,
            "days": days,
            "time": sched_time,
            "timezone": "Asia/Taipei",
            "task": "建立期別資料夾與檔案",
            "all_regions": sched_all,
        }
        save_config(config)
        add_log(f"排程設定已儲存：每月 {days} 日 {sched_time}", "success")
        st.success("✅ 排程設定已儲存")
    except Exception as e:
        st.error(f"❌ {e}")

st.markdown('</div>', unsafe_allow_html=True)


# ═══════════════════════════════════════
# 地區設定
# ═══════════════════════════════════════
st.markdown('<div class="card"><div class="card-title">⚙️ 地區設定</div>', unsafe_allow_html=True)

REGION_FIELDS = [
    ("root_folder_id", "根目錄 ID"),
    ("allowance_id",   "請款 ID"),
    ("salary_id",      "薪資 ID"),
    ("roster_id",      "名冊 ID"),
]

col_hdr, col_add = st.columns([3, 1])
with col_add:
    if st.button("➕ 新增地區", use_container_width=True):
        st.session_state.adding_region = True
        st.session_state.editing_region = None

if st.session_state.adding_region:
    with st.form("add_region_form"):
        st.markdown("**新增地區**")
        new_name = st.text_input("地區名稱", placeholder="台北")
        new_root = st.text_input("根目錄 ID")
        new_allowance = st.text_input("請款 ID")
        new_salary = st.text_input("薪資 ID")
        new_roster = st.text_input("名冊 ID")

        s1, s2 = st.columns(2)
        with s1:
            submitted = st.form_submit_button("💾 儲存", use_container_width=True)
        with s2:
            cancelled = st.form_submit_button("✕ 取消", use_container_width=True)

        if submitted:
            if not new_name or not new_root:
                st.error("地區名稱和根目錄 ID 為必填")
            else:
                regions.append({
                    "name": new_name,
                    "root_folder_id": new_root,
                    "allowance_id": new_allowance,
                    "salary_id": new_salary,
                    "roster_id": new_roster,
                })
                config["regions"] = regions
                save_config(config)
                add_log(f"新增地區：{new_name}", "success")
                st.session_state.adding_region = False
                st.rerun()

        if cancelled:
            st.session_state.adding_region = False
            st.rerun()

for i, region in enumerate(regions):
    name = region.get("name", f"地區{i+1}")
    all_set = all(region.get(f) for f, _ in REGION_FIELDS)
    badge = '<span class="badge-ok">已設定</span>' if all_set else '<span class="badge-err">未完整</span>'

    if st.session_state.editing_region == name:
        with st.form(f"edit_{name}_{i}"):
            st.markdown(f"**編輯：{name}**")
            e_name = st.text_input("地區名稱", value=name)
            e_root = st.text_input("根目錄 ID", value=region.get("root_folder_id", ""))
            e_allowance = st.text_input("請款 ID", value=region.get("allowance_id", ""))
            e_salary = st.text_input("薪資 ID", value=region.get("salary_id", ""))
            e_roster = st.text_input("名冊 ID", value=region.get("roster_id", ""))

            es1, es2 = st.columns(2)
            with es1:
                save_edit = st.form_submit_button("💾 儲存", use_container_width=True)
            with es2:
                cancel_edit = st.form_submit_button("✕ 取消", use_container_width=True)

            if save_edit:
                regions[i] = {
                    "name": e_name,
                    "root_folder_id": e_root,
                    "allowance_id": e_allowance,
                    "salary_id": e_salary,
                    "roster_id": e_roster,
                }
                config["regions"] = regions
                save_config(config)
                add_log(f"更新地區：{e_name}", "success")
                st.session_state.editing_region = None
                st.rerun()

            if cancel_edit:
                st.session_state.editing_region = None
                st.rerun()
    else:
        detail_html = ""
        for field, label in REGION_FIELDS:
            val = region.get(field, "")
            status = "✅" if val else "❌ 未設定"
            short = val[:22] + "..." if len(val) > 22 else val
            detail_html += f'<div class="detail-row"><strong>{label}</strong>：{status} {short}</div>'

        st.markdown(f"""
        <div class="region-card">
          <div style="display:flex;justify-content:space-between;align-items:center;margin-bottom:8px;">
            <strong style="color:#0a4b6e;">🏷️ {name}</strong>{badge}
          </div>
          {detail_html}
        </div>
        """, unsafe_allow_html=True)

        rc1, rc2, rc3 = st.columns([3, 1, 1])
        with rc2:
            if st.button("📝 編輯", key=f"edit_{i}", use_container_width=True):
                st.session_state.editing_region = name
                st.session_state.adding_region = False
                st.rerun()
        with rc3:
            if st.button("🗑️ 刪除", key=f"del_{i}", use_container_width=True):
                regions.pop(i)
                config["regions"] = regions
                save_config(config)
                add_log(f"刪除地區：{name}", "warning")
                st.rerun()

st.markdown('</div>', unsafe_allow_html=True)


# ═══════════════════════════════════════
# 系統維護（清理 Service Account 空間）
# ═══════════════════════════════════════
with st.expander("🔧 系統維護"):
    st.caption("Service Account Drive 空間滿時，點此清理佔用的檔案")

    if "sa_files_list" not in st.session_state:
        st.session_state.sa_files_list = []

    if st.button("🔍 列出 Service Account 擁有的檔案", use_container_width=True):
        with st.spinner("查詢中..."):
            try:
                from modules.auth import get_drive_service
                drive = get_drive_service()
                res = drive.files().list(
                    q="'me' in owners and trashed=false",
                    fields="files(id, name, mimeType)",
                    pageSize=100,
                    includeItemsFromAllDrives=True,
                    supportsAllDrives=True
                ).execute()
                files = res.get("files", [])
                st.session_state.sa_files_list = files
                if files:
                    st.warning(f"找到 {len(files)} 個 Service Account 擁有的檔案：")
                    for f in files:
                        st.text(f"- {f['name']}")
                else:
                    st.success("Service Account 沒有擁有任何檔案，空間正常")
            except Exception as e:
                st.error(f"查詢失敗：{e}")

    if st.session_state.sa_files_list:
        if st.button("🗑️ 刪除以上所有檔案（清理空間）", type="primary", use_container_width=True):
            with st.spinner("清理中..."):
                try:
                    from modules.auth import get_drive_service
                    drive = get_drive_service()
                    for f in st.session_state.sa_files_list:
                        drive.files().delete(
                            fileId=f["id"],
                            supportsAllDrives=True
                        ).execute()
                        add_log(f"已刪除：{f['name']}", "warning")
                    st.session_state.sa_files_list = []
                    add_log("Service Account 空間清理完成，可以重新執行建立期別", "success")
                    st.success("✅ 清理完成！")
                except Exception as e:
                    st.error(f"清理失敗：{e}")
