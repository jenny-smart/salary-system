"""同步各區「2026承攬服務費mail」試算表。"""

import datetime
import gspread

from modules.auth import get_gspread_client, get_drive_service
from modules.master_sheet import MASTER_SHEET_ID, record_execution

MAIL_IDS = {
    "台北": "1vbtsEF5_WjQGgmBuVNq9t1zhtgGqsKHpabEVQnuYCNY",
    "台中": "1FaPQhwSV5Qws2do5DSCYYRfFJNyIEN82QId4BbB-Dyw",
    "桃園": "1WTZ_P_WfQq1Vlpe4YYeH6oKoCJhmbaXb7P1AcIhXc6A",
    "新竹": "1XvGuKHY2EXxpAULPvMab4uk0-GbvfezlnbtjbPP2hZA",
    "高雄": "1hCckNt44gBpjh5fO0EITWdIg_NLbjANVG2haWHVswGI",
}


def _region_ids(gc, region):
    rows = gc.open_by_key(MASTER_SHEET_ID).worksheet("地區設定").get_all_values()
    if not rows:
        return {}
    headers = [str(v).strip() for v in rows[0]]
    for row in rows[1:]:
        values = dict(zip(headers, row))
        if str(values.get("name", "")).strip() == region:
            return values
    return {}


def _find_period_file(root_id, period, label, region):
    drive = get_drive_service()
    folders = drive.files().list(
        q=f"'{root_id}' in parents and name='{period}' and mimeType='application/vnd.google-apps.folder' and trashed=false",
        fields="files(id)", pageSize=2,
    ).execute().get("files", [])
    if not folders:
        return ""
    name = f"{period}{label}-{region.replace('區', '').strip()}"
    files = drive.files().list(
        q=f"'{folders[0]['id']}' in parents and name='{name}' and trashed=false",
        fields="files(id)", pageSize=2,
    ).execute().get("files", [])
    return files[0]["id"] if files else ""


def _pairs(ss, sheet_name):
    try:
        rows = ss.worksheet(sheet_name).get("B2:E") or []
    except gspread.WorksheetNotFound:
        return []
    return [
        [str(r[0]).strip(), r[3] if len(r) > 3 else ""]
        for r in rows if r and str(r[0]).strip()
    ]


def _worksheet(ss, title, rows=500, cols=8):
    try:
        return ss.worksheet(title)
    except gspread.WorksheetNotFound:
        return ss.add_worksheet(title=title, rows=rows, cols=cols)


def sync_service_fee_mail(
    root_folder_id, period, region, mail_id="", roster_id="",
    cleaning_file_id="", other_file_id="", log=None,
):
    log = log or (lambda _msg: None)
    gc = get_gspread_client()
    cfg = _region_ids(gc, region)
    mail_id = mail_id or cfg.get("mail_id") or MAIL_IDS.get(region, "")
    roster_id = roster_id or cfg.get("roster_id", "")
    if not mail_id:
        return 0
    cleaning_file_id = cleaning_file_id or _find_period_file(
        root_folder_id, period, "清潔承攬", region
    )
    other_file_id = other_file_id or _find_period_file(
        root_folder_id, period, "其他承攬", region
    )
    mail_ss = gc.open_by_key(mail_id)
    data = []
    if cleaning_file_id:
        cleaning = gc.open_by_key(cleaning_file_id)
        data += _pairs(cleaning, "PDF產出")
        data += _pairs(cleaning, "專案PDF產出")
    if other_file_id:
        data += _pairs(gc.open_by_key(other_file_id), "PDF產出")

    period_ws = _worksheet(mail_ss, period)
    period_ws.batch_clear(["B2:C"])
    period_ws.update("B1:C1", [["專員", "PDF連結"]])
    if data:
        period_ws.update(
            f"B2:C{1 + len(data)}", data, value_input_option="USER_ENTERED"
        )

    mail_ws = _worksheet(mail_ss, "mail")
    mail_ws.update_cell(1, 1, roster_id)
    yyyymm = period[:6]
    mail_ws.update_cell(
        2, 1,
        f'=CHOOSECOLS(IMPORTRANGE(A1,"{yyyymm}專員名冊!A2:I120"),2,9)'
    )

    # 下半月工具包押金
    if period.endswith("-2") and cleaning_file_id:
        summary = cleaning.worksheet("場次時數薪資總表")
        names = [
            str(r[0]).strip() for r in (summary.get("AE4:AE120") or [])
            if r and str(r[0]).strip() and str(r[0]).strip() != "0"
        ]
        if names:
            ab = summary.get("A4:B120") or []
            count_map = {
                str(r[0]).strip(): (r[1] if len(r) > 1 else "")
                for r in ab if r and str(r[0]).strip()
            }
            year, month = int(period[:4]), int(period[4:6])
            due = (
                datetime.date(year + 1, 1, 10) if month == 12
                else datetime.date(year, month + 1, 10)
            ).strftime("%Y/%m/%d")
            amount = 1500 if "台中" in region else 2000
            dep_ws = _worksheet(mail_ss, f"{period}工具包押金")
            dep_ws.batch_clear(["B2:E"])
            dep_ws.update("B1:E1", [["專員", "場次數", "發放日", "金額"]])
            dep_ws.update(
                f"B2:E{1 + len(names)}",
                [[name, count_map.get(name, ""), due, amount] for name in names],
                value_input_option="USER_ENTERED",
            )
    log(f"承攬服務費 mail 已同步 {len(data)} 筆")
    record_execution(region, period, "承攬mail", len(data))
    return len(data)
