"""承攬費通知信：同步各區「YYYY承攬服務費mail」試算表。"""

from __future__ import annotations

import datetime as dt
import re
from typing import Callable, Iterable, Optional

import gspread

from modules.auth import get_gspread_client, get_drive_service, get_jenny_gspread_client
from modules.master_sheet import MASTER_SHEET_ID, record_execution


PERIOD_RE = re.compile(r"^(\d{6})-([12])$")
DEPOSIT_RE = re.compile(r"^(\d{6}-[12])工具包押金$")


def _emit(log, message: str) -> None:
    if log is None:
        return
    if callable(log):
        log(message)
    elif hasattr(log, "append"):
        log.append(message)


def _region_ids(gc, region: str) -> dict:
    rows = gc.open_by_key(MASTER_SHEET_ID).worksheet("地區設定").get_all_values()
    if not rows:
        return {}
    headers = [str(v).strip() for v in rows[0]]
    for row in rows[1:]:
        values = dict(zip(headers, row))
        if str(values.get("name", "")).strip() == region:
            return values
    return {}


def _find_period_file(root_id: str, period: str, label: str, region: str) -> str:
    drive = get_drive_service()
    folders = drive.files().list(
        q=(
            f"'{root_id}' in parents and name='{period}' "
            "and mimeType='application/vnd.google-apps.folder' and trashed=false"
        ),
        fields="files(id)",
        pageSize=2,
        supportsAllDrives=True,
        includeItemsFromAllDrives=True,
    ).execute().get("files", [])
    if not folders:
        return ""

    safe_region = region.replace("區", "").strip()
    name = f"{period}{label}-{safe_region}"
    files = drive.files().list(
        q=f"'{folders[0]['id']}' in parents and name='{name}' and trashed=false",
        fields="files(id)",
        pageSize=2,
        supportsAllDrives=True,
        includeItemsFromAllDrives=True,
    ).execute().get("files", [])
    return files[0]["id"] if files else ""


def _previous_period(period: str) -> str:
    m = PERIOD_RE.fullmatch(str(period).strip())
    if not m:
        raise ValueError(f"期別格式錯誤：{period!r}，應為 YYYYMM-1 或 YYYYMM-2")

    yyyymm, half = m.groups()
    year = int(yyyymm[:4])
    month = int(yyyymm[4:6])

    if half == "2":
        return f"{year:04d}{month:02d}-1"

    if month == 1:
        year -= 1
        month = 12
    else:
        month -= 1
    return f"{year:04d}{month:02d}-2"


def _sheet_or_none(ss: gspread.Spreadsheet, title: str):
    try:
        return ss.worksheet(title)
    except gspread.WorksheetNotFound:
        return None


def _copy_period_sheet(mail_ss: gspread.Spreadsheet, period: str, log=None):
    current = _sheet_or_none(mail_ss, period)
    if current is not None:
        _emit(log, f"目前期別工作表已存在：{period}，直接重整")
        return current

    previous = _previous_period(period)
    src = _sheet_or_none(mail_ss, previous)
    if src is None:
        raise gspread.WorksheetNotFound(
            f"找不到上個期別工作表「{previous}」，無法建立「{period}」"
        )

    ws = mail_ss.duplicate_sheet(
        source_sheet_id=src.id,
        new_sheet_name=period,
    )
    _emit(log, f"已複製 {previous} → {period}")
    return ws


def _pairs_with_service(ss: gspread.Spreadsheet, sheet_name: str, include_service=False):
    ws = _sheet_or_none(ss, sheet_name)
    if ws is None:
        return []

    if include_service:
        rows = ws.get("B2:I", value_render_option="UNFORMATTED_VALUE") or []
        out = []
        for r in rows:
            name = str(r[0]).strip() if len(r) > 0 and r[0] is not None else ""
            link = r[3] if len(r) > 3 else ""
            service = str(r[7]).strip() if len(r) > 7 and r[7] is not None else ""
            if name and str(link).strip():
                out.append((name, link, service))
        return out

    rows = ws.get("B2:E", value_render_option="UNFORMATTED_VALUE") or []
    out = []
    for r in rows:
        name = str(r[0]).strip() if len(r) > 0 and r[0] is not None else ""
        link = r[3] if len(r) > 3 else ""
        if name and str(link).strip():
            out.append((name, link))
    return out


def _service_label(raw: str) -> str:
    text = str(raw or "").strip()
    if "水洗" in text:
        return "水洗"
    if "家電" in text:
        return "家電"
    return "其他服務"


def _replace_cleaning_text_for_other_rows(
    ws: gspread.Worksheet,
    start_row: int,
    other_rows,
    period: str,
    log=None,
):
    """把其他承攬列 D/E 中的「清潔」替換為水洗 / 家電 / 其他服務。"""
    if not other_rows:
        return

    end_row = start_row + len(other_rows) - 1
    formula_rows = ws.get(
        f"D{start_row}:E{end_row}",
        value_render_option="FORMULA",
    ) or []
    display_rows = ws.get(
        f"D{start_row}:E{end_row}",
        value_render_option="FORMATTED_VALUE",
    ) or []

    updates = []
    for i, (name, _link, raw_service) in enumerate(other_rows):
        row_num = start_row + i
        label = _service_label(raw_service)

        for j, col in enumerate(("D", "E")):
            formula = ""
            if i < len(formula_rows) and j < len(formula_rows[i]):
                formula = str(formula_rows[i][j] or "")

            if formula.startswith("="):
                if "清潔" in formula:
                    value = formula.replace("清潔", label)
                else:
                    value = formula
            else:
                shown = ""
                if i < len(display_rows) and j < len(display_rows[i]):
                    shown = str(display_rows[i][j] or "")
                if "清潔" in shown:
                    value = shown.replace("清潔", label)
                elif col == "D":
                    value = f"檸檬家事｜{label}"
                else:
                    value = f"{period} 檸檬家事｜{label}承攬服務費_{name}"

            updates.append({"range": f"{col}{row_num}", "values": [[value]]})

    if updates:
        ws.spreadsheet.values_batch_update({
            "valueInputOption": "USER_ENTERED",
            "data": [
                {"range": f"'{ws.title}'!{u['range']}", "values": u["values"]}
                for u in updates
            ],
        })
    _emit(log, f"其他承攬通知文字已依服務類型更新：{len(other_rows)} 列")


def _setup_mail_sheet(mail_ss: gspread.Spreadsheet, roster_id: str, period: str, log=None):
    ws = mail_ss.worksheet("mail")
    ws.update_cell(1, 1, roster_id)
    yyyymm = period[:6]
    formula = (
        f'=CHOOSECOLS('
        f'IMPORTRANGE(A1,"{yyyymm}專員名冊!B2:I120"),'
        f'1,8)'
    )
    ws.update_cell(2, 1, formula)
    _emit(log, f"mail!A1 已設定 roster_id；A2 已更新 {yyyymm} 專員名冊公式")


def _write_period_rows(
    period_ws: gspread.Worksheet,
    period: str,
    cleaning_rows,
    project_rows,
    other_rows,
    log=None,
):
    period_ws.update_cell(1, 4, period)  # D1
    period_ws.batch_clear(["B2:C", "F2:F"])

    data = []
    data.extend([[name, link] for name, link in cleaning_rows])
    data.extend([[name, link] for name, link in project_rows])
    data.extend([[name, link] for name, link, _service in other_rows])

    if data:
        period_ws.update(
            f"B2:C{1 + len(data)}",
            data,
            value_input_option="USER_ENTERED",
        )

    cleaning_count = len(cleaning_rows)
    project_count = len(project_rows)
    other_count = len(other_rows)
    other_start = 2 + cleaning_count + project_count
    _replace_cleaning_text_for_other_rows(
        period_ws,
        other_start,
        other_rows,
        period,
        log=log,
    )

    _emit(
        log,
        f"{period} 通知名單完成：清潔 {cleaning_count}、專案 {project_count}、其他 {other_count}，共 {len(data)} 筆",
    )
    return len(data)


def _deposit_source_rows(cleaning_ss: gspread.Spreadsheet):
    ws = cleaning_ss.worksheet("PDF產出")
    rows = ws.get("A121:B", value_render_option="UNFORMATTED_VALUE") or []
    out = []
    for r in rows:
        a = str(r[0]).strip() if len(r) > 0 and r[0] is not None else ""
        b = str(r[1]).strip() if len(r) > 1 and r[1] is not None else ""
        if a or b:
            out.append((a, b))
    return out


def _period_sort_key(period: str):
    m = PERIOD_RE.fullmatch(period)
    if not m:
        return (-1, -1, -1)
    yyyymm, half = m.groups()
    return (int(yyyymm[:4]), int(yyyymm[4:6]), int(half))


def _latest_deposit_sheet(mail_ss: gspread.Spreadsheet, current_period: str):
    candidates = []
    current_key = _period_sort_key(current_period)
    for ws in mail_ss.worksheets():
        m = DEPOSIT_RE.fullmatch(ws.title)
        if not m:
            continue
        p = m.group(1)
        key = _period_sort_key(p)
        if key < current_key:
            candidates.append((key, ws))
    if not candidates:
        return None
    candidates.sort(key=lambda x: x[0], reverse=True)
    return candidates[0][1]


def _ensure_deposit_sheet(mail_ss: gspread.Spreadsheet, period: str, log=None):
    title = f"{period}工具包押金"
    current = _sheet_or_none(mail_ss, title)
    if current is not None:
        _emit(log, f"工具包押金工作表已存在：{title}，直接重整")
        return current

    src = _latest_deposit_sheet(mail_ss, period)
    if src is None:
        raise gspread.WorksheetNotFound("找不到最近一期「期別工具包押金」工作表")

    ws = mail_ss.duplicate_sheet(
        source_sheet_id=src.id,
        new_sheet_name=title,
    )
    _emit(log, f"已複製工具包押金工作表 {src.title} → {title}")
    return ws


def _next_month_tenth(period: str) -> dt.date:
    year = int(period[:4])
    month = int(period[4:6])
    if month == 12:
        return dt.date(year + 1, 1, 10)
    return dt.date(year, month + 1, 10)


def _previous_business_day(day: dt.date, log=None) -> dt.date:
    """
    10 日若為週末或台灣國定假日，往前移至最近工作日。
    若環境未安裝 holidays，仍至少處理六日。
    """
    tw_holidays = set()
    try:
        import holidays  # requirements 建議加入 holidays
        tw = holidays.country_holidays("TW", years=[day.year])
        tw_holidays = set(tw.keys())
    except Exception:
        _emit(log, "⚠️ holidays 套件未啟用，例假日僅先依六日判斷")

    result = day
    while result.weekday() >= 5 or result in tw_holidays:
        result -= dt.timedelta(days=1)
    return result


def _deposit_amount(region: str, log=None):
    name = str(region or "").replace("區", "").strip()
    amounts = {
        "台北": 2000,
        "桃園": 2000,
        "新竹": 2000,
        "台中": 1500,
    }
    amount = amounts.get(name)
    if amount is None:
        _emit(log, f"⚠️ {region} 未設定工具包押金金額，E欄將留空")
    return amount


def _sync_tool_deposit(
    mail_ss: gspread.Spreadsheet,
    cleaning_ss: gspread.Spreadsheet,
    period: str,
    region: str,
    log=None,
) -> int:
    rows = _deposit_source_rows(cleaning_ss)
    if not rows:
        _emit(log, "PDF產出第121列起無資料，略過工具包押金")
        return 0

    ws = _ensure_deposit_sheet(mail_ss, period, log=log)
    ws.batch_clear(["B2:C", "G2:G"])

    due = _previous_business_day(_next_month_tenth(period), log=log)
    due_text = due.strftime("%Y/%m/%d")
    amount = _deposit_amount(region, log=log)

    values = [
        [a, b, due_text, "" if amount is None else amount]
        for a, b in rows
    ]
    ws.update(
        f"B2:E{1 + len(values)}",
        values,
        value_input_option="USER_ENTERED",
    )
    _emit(log, f"工具包押金完成：{len(values)} 筆，日期 {due_text}")
    return len(values)


def sync_service_fee_mail(
    root_folder_id: str,
    period: str,
    region: str,
    mail_id: str = "",
    roster_id: str = "",
    cleaning_file_id: str = "",
    other_file_id: str = "",
    log=None,
) -> int:
    """
    承攬費通知信主流程。

    1. 依地區 mail_id 開啟承攬服務費 mail
    2. mail!A1/A2 更新 roster_id 與當月名冊 IMPORTRANGE
    3. 複製前一期工作表為目前期別；D1=目前期別；清 B2:C、F2:F
    4. 依序寫入 清潔PDF、專案PDF、其他承攬PDF 的姓名/連結
    5. 其他承攬列依 I 欄服務名稱把「清潔」改為水洗/家電/其他服務
    6. 若 清潔承攬 PDF產出!A121:B 有資料，建立目前期別工具包押金
    """
    # Service Account：主控表、清潔承攬、其他承攬
    gc = get_gspread_client()

    # Jenny OAuth：各區「YYYY承攬服務費mail」
    # mail_id 試算表可能沒有分享給 Service Account，因此必須用 Jenny 本人權限。
    mail_gc = get_jenny_gspread_client()

    cfg = _region_ids(gc, region)

    mail_id = str(mail_id or cfg.get("mail_id", "") or "").strip()
    roster_id = str(roster_id or cfg.get("roster_id", "") or "").strip()
    if not mail_id:
        raise ValueError(f"【{region}】地區設定 mail_id 為空")
    if not roster_id:
        raise ValueError(f"【{region}】地區設定 roster_id 為空")

    cleaning_file_id = cleaning_file_id or _find_period_file(
        root_folder_id, period, "清潔承攬", region
    )
    other_file_id = other_file_id or _find_period_file(
        root_folder_id, period, "其他承攬", region
    )
    if not cleaning_file_id:
        raise FileNotFoundError(f"找不到 {period}清潔承攬-{region}")

    _emit(log, f"▶ 承攬費通知信 {region} {period} 開始")

    mail_ss = mail_gc.open_by_key(mail_id)
    cleaning_ss = gc.open_by_key(cleaning_file_id)
    other_ss = gc.open_by_key(other_file_id) if other_file_id else None

    _setup_mail_sheet(mail_ss, roster_id, period, log=log)

    cleaning_rows = _pairs_with_service(cleaning_ss, "PDF產出")
    project_rows = _pairs_with_service(cleaning_ss, "專案PDF產出")
    other_rows = (
        _pairs_with_service(other_ss, "PDF產出", include_service=True)
        if other_ss is not None else []
    )

    period_ws = _copy_period_sheet(mail_ss, period, log=log)
    total = _write_period_rows(
        period_ws,
        period,
        cleaning_rows,
        project_rows,
        other_rows,
        log=log,
    )

    deposit_count = _sync_tool_deposit(
        mail_ss,
        cleaning_ss,
        period,
        region,
        log=log,
    )

    # 舊打卡鍵保留，另新增本功能總打卡。
    record_execution(region, period, "清潔承攬mail", len(cleaning_rows) + len(project_rows))
    record_execution(region, period, "其他承攬mail", len(other_rows))
    record_execution(region, period, "承攬費通知信", total)

    _emit(
        log,
        f"✅ 承攬費通知信完成：通知 {total} 筆；工具包押金 {deposit_count} 筆",
    )
    return total
