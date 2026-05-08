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


# ═══════════════════════════════════════════════════════════
# █ 區塊1：設定檔讀寫
# ═══════════════════════════════════════════════════════════
CONFIG_PATH = "config.yaml"

def load_config():
    with open(CONFIG_PATH, "r", encoding="utf-8") as f:
        return yaml.safe_load(f)

def save_config(cfg):
    with open(CONFIG_PATH, "w", encoding="utf-8") as f:
        yaml.dump(cfg, f, allow_unicode=True, default_flow_style=False)
    st.cache_data.clear()

config  = load_config()
regions = config.get("regions", [])


# ═══════════════════════════════════════════════════════════
# █ 區塊2：Session State 初始化
# ═══════════════════════════════════════════════════════════
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


# ═══════════════════════════════════════════════════════════
# █ 區塊3：日誌工具
# ═══════════════════════════════════════════════════════════
def add_log(message: str, level: str = "info"):
    import pytz
    now = datetime.now(pytz.timezone("Asia/Taipei")).strftime("%H:%M:%S")
    icons = {"info": "🔵", "success": "✅", "error": "❌", "warning": "⚠️"}
    icon  = icons.get(level, "🔵")
    st.session_state.logs.append(f"[{now}] {icon} {message}")
    if len(st.session_state.logs) > 500:
        st.session_state.logs = st.session_state.logs[-500:]
    if "log_placeholder" in st.session_state:
        _render_log(st.session_state.log_placeholder)


def _render_log(placeholder):
    entries = list(st.session_state.logs)
    html = (
        '<div class="log-box">'
        '<div class="log-header">'
        '<span>📋 執行日誌</span>'
        '<span style="background:#1e4757;padding:3px 10px;border-radius:20px;font-size:0.75rem;">即時更新</span>'
        '</div><div class="log-scroll">'
    )
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


# ═══════════════════════════════════════════════════════════
# █ 區塊4：執行功能選單設定
# ═══════════════════════════════════════════════════════════
FUNCTION_MAP = {
    "💰 金流對帳": [
        "① 建立期別資料夾與檔案（手動）",
        "① 建立期別資料夾與檔案（排程）",
        "② 期別訂單轉檔（xls/xlsx → Google Sheet）",
        "③ 期別訂單搬運",
        "④ 期別訂單加工",
        "⑤ 期別訂單分類",
        "⑥ 金流對帳轉檔（zip/csv/xlsx → Google Sheet）",
        "⑦ 搬運退款＋預收",
        "⑧ 搬運發票＋藍新",
    ],
    "🧹 清潔承攬": [
        "前置作業",
        "00調薪",
        "01專員請款",
        "02儲值獎金",
        "03新人實境",
        "04新人實習",
        "05組長津貼",
        "06季獎金",
        "結算作業",
        "一鍵執行",
        "新人實境實習期別",
        "工具包押金",
        "元大帳戶",
        "產生PDF",
    ],
    "📦 其他承攬": [
        "水洗前置", "家電前置", "全部前置",
        "水洗結算", "家電結算", "全部結算", "產出全部薪資單",
    ],
}


# ═══════════════════════════════════════════════════════════
# █ 區塊5：UI — 主標題
# ═══════════════════════════════════════════════════════════
st.markdown('<div class="app-title">🍋 Lemon Clean 薪資系統</div>', unsafe_allow_html=True)


# ═══════════════════════════════════════════════════════════
# █ 區塊6：UI — 執行設定卡片
# ═══════════════════════════════════════════════════════════
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

st.markdown('<div class="field-label">🎯 執行功能</div>', unsafe_allow_html=True)
selected_function = st.selectbox(
    "功能", FUNCTION_MAP[system], label_visibility="collapsed", key="func"
)

c3, c4 = st.columns([2, 1])
with c3:
    st.markdown('<div class="field-label">🗺️ 執行地區</div>', unsafe_allow_html=True)
    region_names = [r["name"] for r in regions]
    if region_names:
        selected_name   = st.selectbox("地區", region_names, label_visibility="collapsed", key="region")
        selected_region = next((r for r in regions if r["name"] == selected_name), {})
    else:
        st.caption("尚未設定任何地區")
        selected_name   = None
        selected_region = {}
with c4:
    st.markdown('<div class="field-label">&nbsp;</div>', unsafe_allow_html=True)
    run_clicked = st.button("▶ 執行", use_container_width=True)

st.markdown('</div>', unsafe_allow_html=True)

if run_clicked:
    st.info("⏳ 執行中，請稍候...")


# ═══════════════════════════════════════════════════════════
# █ 區塊7：UI — 日誌區塊
# ═══════════════════════════════════════════════════════════
log_placeholder = st.empty()
st.session_state.log_placeholder = log_placeholder
_render_log(log_placeholder)

col_clear, _ = st.columns([1, 3])
with col_clear:
    if st.button("🗑️ 清除日誌"):
        st.session_state.logs = ["[--:--:--] 日誌已清除"]
        _render_log(log_placeholder)


# ═══════════════════════════════════════════════════════════
# █ 區塊8：執行邏輯 — 共用前置檢查
# ═══════════════════════════════════════════════════════════
if run_clicked:
    _period = period
    _system = system
    _func   = selected_function
    _name   = selected_name
    _region = selected_region

    if not _period:
        add_log("請先輸入期別", "error")
    elif not _name:
        add_log("請先新增地區設定", "error")
    else:
        root_id = _region.get("root_folder_id", "")
        if not root_id:
            add_log(f"【{_name}】尚未設定根目錄 ID", "error")
        else:
            _is_first_half = is_first_half(_period)
            half = "上半月" if _is_first_half else "下半月"
            add_log(f"執行【{_name}】{half} {_func}，期別：{_period}")
            _render_log(log_placeholder)

            try:
                from modules.master_sheet import record_execution, record_batch

                # ───────────────────────────────────────────────
                # █ 區塊8-A：金流對帳執行邏輯
                # ───────────────────────────────────────────────
                if _system == "💰 金流對帳":

                    if "① 建立期別" in _func:
                        from modules.payment_reconciliation import create_period
                        is_scheduled  = "排程" in _func
                        result        = create_period(root_id, _period, _name, add_log)
                        copied        = result.get('copied', 0)
                        add_log(f"建立完成，共複製 {copied} 個檔案", "success")
                        file_ids      = result.get("fileIds", {})
                        folder_id_val = result.get("folderId", None)
                        prefix        = "排程" if is_scheduled else "手動"
                        record_batch(_name, _period, [
                            {"task_key": f"{prefix}期別資料夾",   "count": folder_id_val},
                            {"task_key": f"{prefix}期別金流對帳", "count": file_ids.get("金流對帳")},
                            {"task_key": f"{prefix}期別清潔承攬", "count": file_ids.get("清潔承攬")},
                            {"task_key": f"{prefix}期別其他承攬", "count": file_ids.get("其他承攬")},
                            {"task_key": f"{prefix}期別元大帳戶", "count": file_ids.get("元大帳戶")},
                        ])

                    elif "② 期別訂單轉檔" in _func:
                        from modules.payment_reconciliation import convert_order_file
                        result      = convert_order_file(root_id, _period, _name, add_log)
                        add_log("期別訂單轉檔完成", "success")
                        order_count = None
                        try:
                            from modules.auth import open_spreadsheet
                            file_id = result.get("fileId")
                            if file_id:
                                ss_order    = open_spreadsheet(file_id)
                                ws          = ss_order.worksheets()[0]
                                b_vals      = ws.col_values(2)
                                order_count = len([v for v in b_vals[1:] if v and v.strip()])
                                add_log(f"🔵 訂單筆數：{order_count} 筆")
                        except Exception as e:
                            add_log(f"⚠️ 讀取筆數失敗：{e}", "warning")
                        record_execution(_name, _period, "期別訂單轉檔", order_count)

                    elif "③ 期別訂單搬運" in _func:
                        from modules.payment_reconciliation import copy_orders_to_template
                        result    = copy_orders_to_template(root_id, _period, _name, add_log)
                        count     = result["count"]
                        start_row = result["start_row"]
                        add_log(f"搬運完成：{count} 筆，起始列：{start_row}", "success")
                        key = f"start_row_{_period}_{_name}"
                        st.session_state[key] = start_row
                        record_batch(_name, _period, [
                            {"task_key": "訂單起始列",   "count": start_row},
                            {"task_key": "複製期別訂單", "count": count},
                        ])

                    elif "④ 期別訂單加工" in _func:
                        from modules.payment_reconciliation import process_template
                        key       = f"start_row_{_period}_{_name}"
                        start_row = st.session_state.get(key)
                        try:
                            from modules.master_sheet import get_recorded_value
                            recorded_start = get_recorded_value(_name, _period, "訂單起始列")
                            if recorded_start and start_row and int(recorded_start) != int(start_row):
                                add_log(f"⚠️ Double check 不一致：session={start_row}，打卡表={recorded_start}，使用打卡表的值", "warning")
                                start_row = int(recorded_start)
                            elif recorded_start and not start_row:
                                start_row = int(recorded_start)
                                add_log(f"🔵 從打卡表讀取起始列：{start_row}")
                        except Exception:
                            pass
                        result          = process_template(root_id, _period, _name, start_row, add_log)
                        sort_count      = result['sort_count']
                        mark_count      = result['mark_count']
                        expand_count    = result['expand_count']
                        category_counts = result.get('category_counts', {})
                        before_main     = result.get('before_main', {})
                        after_main      = result.get('after_main', {})
                        after_rows      = result.get('after_rows', {})
                        add_log(f"加工完成：排序 {sort_count} 筆，異常 {mark_count} 筆，拆解新增 {expand_count} 列", "success")
                        for w in result.get("warnings", []):
                            add_log(w, "warning")
                        st.session_state[f"category_counts_{_period}_{_name}"] = category_counts
                        st.session_state[f"after_rows_{_period}_{_name}"]      = after_rows
                        svc_list = ["清潔", "水洗", "家電", "收納", "座椅", "地毯"]
                        batch = [
                            {"task_key": "加工-排序",            "count": sort_count},
                            {"task_key": "加工-K欄標註異常標橘底", "count": mark_count},
                        ]
                        for svc in svc_list:
                            batch.append({"task_key": f"加工前-{svc}主單數", "count": before_main.get(svc, 0)})
                        for svc in svc_list:
                            batch.append({"task_key": f"加工後-{svc}主單數", "count": after_main.get(svc, 0)})
                        for svc in svc_list:
                            batch.append({"task_key": f"加工-{svc}加工列數", "count": after_rows.get(svc, 0)})
                        batch.append({"task_key": "加工-儲值金列數", "count": after_rows.get("儲值金", 0)})
                        record_batch(_name, _period, batch)

                    elif "⑤ 期別訂單分類" in _func:
                        from modules.payment_reconciliation import copy_classified_data
                        from modules.master_sheet import get_recorded_value
                        key             = f"start_row_{_period}_{_name}"
                        start_row       = st.session_state.get(key)
                        category_counts = st.session_state.get(f"category_counts_{_period}_{_name}", {})
                        try:
                            recorded_start = get_recorded_value(_name, _period, "訂單起始列")
                            recorded_count = get_recorded_value(_name, _period, "複製期別訂單")
                            if recorded_start:
                                recorded_start = int(recorded_start)
                            if recorded_count:
                                recorded_count = int(recorded_count)
                            if start_row and recorded_start and start_row != recorded_start:
                                add_log(f"⚠️ Double check：起始列不一致 session={start_row}，打卡表={recorded_start}，使用打卡表", "warning")
                                start_row = recorded_start
                            elif not start_row and recorded_start:
                                start_row = recorded_start
                                add_log(f"🔵 從打卡表讀取起始列：{start_row}")
                            if start_row and recorded_count:
                                add_log(f"🔵 Double check：起始列={start_row}，③筆數={recorded_count} ✅")
                        except Exception as e:
                            add_log(f"⚠️ Double check 失敗：{e}", "warning")
                        counts = copy_classified_data(root_id, _period, _name, start_row, category_counts, add_log)
                        add_log("分類搬運完成", "success")
                        for k, v in counts.items():
                            if v > 0:
                                add_log(f"　{k}：{v} 筆")
                        svc_task_map = {
                            "清潔": "複製清潔訂單列數",
                            "水洗": "複製水洗訂單列數",
                            "家電": "複製家電訂單列數",
                            "收納": "複製收納訂單列數",
                            "座椅": "複製座椅訂單列數",
                            "地毯": "複製地毯訂單列數",
                        }
                        batch = []
                        for label, task_key in svc_task_map.items():
                            batch.append({"task_key": task_key, "count": counts.get(label, 0)})
                        record_batch(_name, _period, batch)

                    elif "⑥ 金流對帳轉檔" in _func:
                        from modules.payment_reconciliation import convert_payment_file
                        from modules.auth import open_spreadsheet
                        result   = convert_payment_file(root_id, _period, _name, add_log)
                        file_ids = result.get("fileIds", {})
                        add_log("金流對帳轉檔完成", "success")

                        def _get_sheet_count(fid):
                            if not fid:
                                return None
                            try:
                                ss     = open_spreadsheet(fid)
                                ws     = ss.worksheets()[0]
                                b_vals = ws.col_values(2)
                                count  = len([v for v in b_vals[1:] if v and v.strip()])
                                if count == 0:
                                    a_vals = ws.col_values(1)
                                    count  = len([v for v in a_vals[1:] if v and v.strip()])
                                return count if count > 0 else None
                            except Exception:
                                return None

                        record_batch(_name, _period, [
                            {"task_key": "期別發票解壓縮",        "count": None},
                            {"task_key": "期別發票轉檔",          "count": _get_sheet_count(file_ids.get("發票"))},
                            {"task_key": "期別已退款全部加收轉檔", "count": _get_sheet_count(file_ids.get("已退款全部加收"))},
                            {"task_key": "期別已退款全部退款轉檔", "count": _get_sheet_count(file_ids.get("已退款全部退款"))},
                            {"task_key": "期別預收轉檔",           "count": _get_sheet_count(file_ids.get("預收"))},
                            {"task_key": "期別藍新收款轉檔",       "count": _get_sheet_count(file_ids.get("藍新收款"))},
                            {"task_key": "期別藍新退款轉檔",       "count": _get_sheet_count(file_ids.get("藍新退款"))},
                        ])

                    elif "⑦ 搬運退款" in _func:
                        from modules.payment_reconciliation import move_refund_and_prepaid
                        counts = move_refund_and_prepaid(root_id, _period, _name, add_log)
                        add_log("退款＋預收搬運完成", "success")
                        for k, v in counts.items():
                            add_log(f"　{k}：{v} 筆")
                        record_batch(_name, _period, [
                            {"task_key": "複製已退款全部加收", "count": counts.get("已退款全部加收")},
                            {"task_key": "複製已退款全部退款", "count": counts.get("已退款全部退款")},
                            {"task_key": "複製預收",           "count": counts.get("預收")},
                        ])

                    elif "⑧ 搬運發票" in _func:
                        from modules.payment_reconciliation import move_invoice_and_bluenew
                        counts = move_invoice_and_bluenew(root_id, _period, _name, add_log)
                        add_log("發票＋藍新搬運完成", "success")
                        for k, v in counts.items():
                            if v > 0:
                                add_log(f"　{k}：{v} 筆")
                        record_batch(_name, _period, [
                            {"task_key": "複製發票",     "count": counts.get("發票")},
                            {"task_key": "複製藍新收款", "count": counts.get("藍新收款")},
                            {"task_key": "複製藍新退款", "count": counts.get("藍新退款")},
                        ])

                # ───────────────────────────────────────────────
                # █ 區塊8-B：清潔承攬執行邏輯
                # ───────────────────────────────────────────────
                elif _system == "🧹 清潔承攬":
                    from modules.cleaning_process_1 import find_cleaning_file, find_payment_file

                    try:
                        cleaning_file_id = find_cleaning_file(root_id, _period, _name)
                        add_log(f"找到清潔承攬檔案：{cleaning_file_id}")
                    except FileNotFoundError as e:
                        add_log(str(e), "error")
                        cleaning_file_id = None

                    # 儲值獎金需要金流對帳 ID，提前取得（其他作業不需要）
                    _payment_file_id = None
                    if cleaning_file_id and _func == "02儲值獎金":
                        try:
                            _payment_file_id = find_payment_file(root_id, _period, _name)
                            add_log(f"找到金流對帳檔案：{_payment_file_id}")
                        except FileNotFoundError as e:
                            add_log(str(e), "error")
                            cleaning_file_id = None  # 找不到就中止

                    if cleaning_file_id:

                        def _make_live_log():
                            """建立即時寫入 add_log 的 log list（每次 append 就立即顯示）。"""
                            class LiveLog(list):
                                def append(self, msg):
                                    super().append(msg)
                                    lvl = "success" if msg.startswith("✅") else \
                                          "error"   if msg.startswith("❌") else \
                                          "warning" if msg.startswith("⚠️") else "info"
                                    add_log(msg, lvl)
                            return LiveLog()

                        def _run(fn, **kwargs):
                            """執行清潔承攬函數，log 即時顯示在 Streamlit 日誌區。"""
                            live = _make_live_log()
                            success = fn(
                                cleaning_file_id=cleaning_file_id,
                                region=_name,
                                period=_period,
                                is_first_half=_is_first_half,
                                log=live,
                                region_cfg=_region,
                                **kwargs,
                            )
                            return success

                        if _func == "前置作業":
                            from modules.cleaning_process_1 import run_preparation
                            success = _run(run_preparation)

                        elif _func == "00調薪":
                            from modules.cleaning_process_1 import run_adjustment
                            success = _run(run_adjustment)

                        elif _func == "01專員請款":
                            from modules.cleaning_process_2 import run_allowance
                            success = _run(run_allowance)

                        elif _func == "02儲值獎金":
                            from modules.cleaning_process_2 import run_voucher
                            success = _run(run_voucher, payment_file_id=_payment_file_id)

                        elif _func == "03新人實境":
                            from modules.cleaning_process_2 import run_newcomer
                            success = _run(run_newcomer)

                        elif _func == "04新人實習":
                            from modules.cleaning_process_2 import run_intern
                            success = _run(run_intern)

                        elif _func == "05組長津貼":
                            from modules.cleaning_process_2 import run_leader
                            success = _run(run_leader)

                        elif _func == "06季獎金":
                            from modules.cleaning_process_3 import run_season_bonus
                            success = _run(run_season_bonus)

                        elif _func == "結算作業":
                            from modules.cleaning_process_3 import run_settlement
                            success = _run(run_settlement)

                        elif _func == "一鍵執行":
                            add_log("一鍵執行尚未實作", "warning")
                            success = False

                        elif _func == "新人實境實習期別":
                            from modules.cleaning_process_2 import run_newcomer_label
                            success = _run(run_newcomer_label)

                        elif _func == "工具包押金":
                            from modules.cleaning_process_4 import run_tool_deposit
                            success = _run(run_tool_deposit)


                        elif _func == "元大帳戶":
                            from modules.cleaning_process_4 import run_yuanta
                            success = _run(run_yuanta)

                        else:
                            add_log(f"{_func} 尚未實作", "warning")
                            success = False

                # ───────────────────────────────────────────────
                # █ 區塊8-C：其他承攬執行邏輯（待開發）
                # ───────────────────────────────────────────────
                else:
                    add_log(f"{_system} {_func} 開發中", "warning")

            except Exception as e:
                import traceback
                add_log(f"執行失敗：{e}", "error")
                add_log(traceback.format_exc(), "error")

    # 執行完後 rerun，讓日誌立即顯示
    st.rerun()


# ═══════════════════════════════════════════════════════════
# █ 區塊9：UI — 排程設定
# ═══════════════════════════════════════════════════════════
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
    sched_all     = st.checkbox("套用全部地區", value=schedule.get("all_regions", True))
with sc4:
    sched_enabled = st.checkbox("啟用排程",     value=schedule.get("enabled",     False))

if st.button("💾 儲存排程設定", use_container_width=True):
    try:
        days = [int(d.strip()) for d in sched_days.split(",") if d.strip()]
        if not days:
            raise ValueError("請輸入排程日期")
        config["schedule"] = {
            "enabled":     sched_enabled,
            "days":        days,
            "time":        sched_time,
            "timezone":    "Asia/Taipei",
            "task":        "建立期別資料夾與檔案",
            "all_regions": sched_all,
        }
        save_config(config)
        add_log(f"排程設定已儲存：每月 {days} 日 {sched_time}", "success")
        st.success("✅ 排程設定已儲存")
    except Exception as e:
        st.error(f"❌ {e}")

st.markdown('</div>', unsafe_allow_html=True)


# ═══════════════════════════════════════════════════════════
# █ 區塊10：UI — 地區設定
# ═══════════════════════════════════════════════════════════
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
        st.session_state.adding_region  = True
        st.session_state.editing_region = None

if st.session_state.adding_region:
    with st.form("add_region_form"):
        st.markdown("**新增地區**")
        new_name      = st.text_input("地區名稱", placeholder="台北")
        new_root      = st.text_input("根目錄 ID")
        new_allowance = st.text_input("請款 ID")
        new_salary    = st.text_input("薪資 ID")
        new_roster    = st.text_input("名冊 ID")

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
                    "name":           new_name,
                    "root_folder_id": new_root,
                    "allowance_id":   new_allowance,
                    "salary_id":      new_salary,
                    "roster_id":      new_roster,
                })
                config["regions"] = regions
                save_config(config)
                init_msg = ""
                try:
                    from modules.master_sheet import init_region_sheet
                    is_new   = init_region_sheet(new_name)
                    init_msg = f"主控試算表：【{new_name}】工作表{'已建立' if is_new else '已存在'}"
                except Exception as e:
                    init_msg = f"主控試算表初始化失敗：{e}"
                add_log(f"新增地區：{new_name}", "success")
                add_log(init_msg, "success" if "已建立" in init_msg or "已存在" in init_msg else "warning")
                st.session_state.adding_region = False
                st.rerun()

        if cancelled:
            st.session_state.adding_region = False
            st.rerun()

for i, region in enumerate(regions):
    name    = region.get("name", f"地區{i+1}")
    all_set = all(region.get(f) for f, _ in REGION_FIELDS)
    badge   = '<span class="badge-ok">已設定</span>' if all_set else '<span class="badge-err">未完整</span>'

    if st.session_state.editing_region == name:
        with st.form(f"edit_{name}_{i}"):
            st.markdown(f"**編輯：{name}**")
            e_name      = st.text_input("地區名稱", value=name)
            e_root      = st.text_input("根目錄 ID",  value=region.get("root_folder_id", ""))
            e_allowance = st.text_input("請款 ID",    value=region.get("allowance_id",   ""))
            e_salary    = st.text_input("薪資 ID",    value=region.get("salary_id",      ""))
            e_roster    = st.text_input("名冊 ID",    value=region.get("roster_id",      ""))

            es1, es2 = st.columns(2)
            with es1:
                save_edit   = st.form_submit_button("💾 儲存", use_container_width=True)
            with es2:
                cancel_edit = st.form_submit_button("✕ 取消", use_container_width=True)

            if save_edit:
                regions[i] = {
                    "name":           e_name,
                    "root_folder_id": e_root,
                    "allowance_id":   e_allowance,
                    "salary_id":      e_salary,
                    "roster_id":      e_roster,
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
            val    = region.get(field, "")
            status = "✅" if val else "❌ 未設定"
            short  = val[:22] + "..." if len(val) > 22 else val
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
                st.session_state.adding_region  = False
                st.rerun()
        with rc3:
            if st.button("🗑️ 刪除", key=f"del_{i}", use_container_width=True):
                regions.pop(i)
                config["regions"] = regions
                save_config(config)
                add_log(f"刪除地區：{name}", "warning")
                st.rerun()

st.markdown('</div>', unsafe_allow_html=True)


# ═══════════════════════════════════════════════════════════
# █ 區塊11：UI — 系統維護
# ═══════════════════════════════════════════════════════════
with st.expander("🔧 系統維護"):
    st.caption("Service Account Drive 空間滿時，點此清理佔用的檔案")

    if "sa_files_list" not in st.session_state:
        st.session_state.sa_files_list = []

    if st.button("🔍 列出 Service Account 擁有的檔案", use_container_width=True):
        with st.spinner("查詢中..."):
            try:
                from modules.auth import get_drive_service
                drive = get_drive_service()
                res   = drive.files().list(
                    q="'me' in owners and trashed=false",
                    fields="files(id, name, mimeType)",
                    pageSize=100,
                    includeItemsFromAllDrives=True,
                    supportsAllDrives=True,
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
                        drive.files().delete(fileId=f["id"], supportsAllDrives=True).execute()
                        add_log(f"已刪除：{f['name']}", "warning")
                    st.session_state.sa_files_list = []
                    add_log("Service Account 空間清理完成，可以重新執行建立期別", "success")
                    st.success("✅ 清理完成！")
                except Exception as e:
                    st.error(f"清理失敗：{e}")
