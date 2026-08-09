"""
Lemon Clean 清潔承攬 — 工具包押金 / 介紹獎金 / 元大帳戶
檔案：modules/cleaning_process_4.py

依賴：
    modules/auth.py         — get_gspread_client()
    modules/master_sheet.py — record_execution()

打卡：統一寫入主控試算表（record_execution），不寫入 exec 工作表。

主控表 task_key：
    工具包押金 → "工具包押金"
    介紹獎金   → "介紹獎金"
    元大帳戶   → "元大帳戶"

工具包押金 & 介紹獎金邏輯（來自 GAS executeFullToolDepositProcess）：

    上半月：
        清空 場次時數薪資總表 A121:E（startRow=121）
        清空 場次時數薪資總表 AB4:AE（col 28:31）
        清空 介紹獎金工作表 A2:C

    下半月：
        工具包押金：
            篩選「工具包押金」工作表：
                I 欄 >= 80 且 J 欄非空白
                → 場次時數薪資總表 A121:B（A=姓名, B=工具包押金 I欄場次）
                台中地區押金=1500，其餘=2000
        介紹獎金：
            篩選「工具包押金」工作表：
                I >= 80 且 J 欄空白
                → 介紹獎金工作表 A2:C（A=J欄, B=A欄, C=1000）

元大帳戶邏輯（來自 GAS runBankAccountUpdate）：

    上半月：
        從 場次時數薪資總表 N4:Q 讀取資料
        寫入期別元大帳戶試算表 A3:E
        存檔為 xlsx：{period}元大承攬費-{region}.xlsx
        目標日期 = 當月10日（週六提前到週五，週日提前到週五）

    下半月：
        從 場次時數薪資總表 U4:X 讀取資料
        寫入期別元大帳戶試算表 A3:E
        存檔為 xlsx：{period}元大承攬費-{region}.xlsx
        若 場次時數薪資總表 AB4:AE 有資料：
            另存 xlsx：{period}元大工具包押金-{region}.xlsx
        目標日期 = 當月20日（週六提前到週五，週日提前到週五）
"""

from __future__ import annotations

import datetime
import io
from typing import List, Optional, Tuple

import gspread

from modules.auth import get_gspread_client
from modules.master_sheet import record_execution
from modules.period_utils import format_taipei_time, get_current_taipei_time


# ──────────────────────────────────────────────────────────────
# 常數
# ──────────────────────────────────────────────────────────────

TS_FMT = "%Y/%m/%d %H:%M"

DEPOSIT_THRESHOLD = 80    # I 欄 >= 80
DEPOSIT_BY_REGION = {
    "台北": 2000,
    "桃園": 2000,
    "新竹": 2000,
    "台中": 1500,
}
DEPOSIT_OTHER = 2000  # 未列地區沿用既有金額
INTRO_BONUS       = 1000

TOOL_DEPOSIT_START_ROW = 121   # 場次時數薪資總表：工具包押金資料寫入起始列
AB_COL = 28                    # AB 欄（1-based）


# ──────────────────────────────────────────────────────────────
# 工具
# ──────────────────────────────────────────────────────────────

def _now_ts() -> str:
    return format_taipei_time(fmt=TS_FMT)


def _log(log: List[str], msg: str) -> None:
    log.append(msg)


def _punch(task_key: str, region: str, period: str) -> str:
    """打卡至主控試算表。"""
    ts = _now_ts()
    record_execution(region, period, task_key, None)
    return ts


def _to_num(val) -> float:
    try:
        return float(str(val).replace(",", "").strip())
    except (ValueError, TypeError):
        return 0.0


def _target_date(is_first_half: bool) -> datetime.date:
    """
    上半月：當月10日；下半月：當月20日。
    若落在週六提前到週五，週日提前到週五。
    """
    today = get_current_taipei_time().date()
    day   = 10 if is_first_half else 20
    d     = today.replace(day=day)
    if d.weekday() == 5:      # 週六
        d = d - datetime.timedelta(days=1)
    elif d.weekday() == 6:    # 週日
        d = d - datetime.timedelta(days=2)
    return d


def _col_letter(n: int) -> str:
    result = ""
    while n > 0:
        n, r = divmod(n - 1, 26)
        result = chr(65 + r) + result
    return result


# ──────────────────────────────────────────────────────────────
# 工具包押金 & 介紹獎金
# ──────────────────────────────────────────────────────────────

def run_tool_deposit(
    cleaning_file_id: str,
    region: str,
    period: str,
    is_first_half: bool,
    log: List[str],
    region_cfg: dict = None,
    **kwargs,
) -> bool:
    """
    工具包押金 & 介紹獎金。
    兩者來自同一工作表篩選，打卡分開。
    """
    label = "上半月" if is_first_half else "下半月"
    _log(log, f"▶ 工具包押金 & 介紹獎金 {label} 開始")
    try:
        gc = get_gspread_client()
        ss = gc.open_by_key(cleaning_file_id)

        ws_summary = ss.worksheet("場次時數薪資總表")

        if is_first_half:
            ws_intro = ss.worksheet("介紹獎金")
            _tool_clear(ws_summary, ws_intro, log)
            dep_count = 0
        else:
            salary_id = str((region_cfg or {}).get("salary_id", "") or "").strip()
            if not salary_id:
                raise ValueError("地區設定缺少 salary_id")
            salary_ss = gc.open_by_key(salary_id)
            ws_deposit = salary_ss.worksheet("工具包押金")
            ws_counts = salary_ss.worksheet("場次和時數")
            ws_intro = ss.worksheet("介紹獎金")

            month = int(period[4:6])
            id_col = 5 + (month - 1) * 3  # 1月E、2月H、3月K...
            ws_counts.update_cell(1, id_col, cleaning_file_id)
            _log(log, f"  清潔承攬 ID 已寫入場次和時數 {_col_letter(id_col)}1")

            deposit_amount = next(
                (amount for key, amount in DEPOSIT_BY_REGION.items() if key in region),
                DEPOSIT_OTHER,
            )
            dep_count = _tool_process_v2(
                ws_deposit, ws_summary, ws_intro,
                deposit_amount, period, log
            )
            _log(log, f"  工具包押金：{dep_count} 筆")

        ts = _now_ts()
        record_execution(region, period, "工具包押金", dep_count)
        _log(log, f"✅ 工具包押金 {label} 完成｜{ts}")
        return True

    except Exception as e:
        detail = str(e).strip() or f"{type(e).__name__}: {e!r}"
        _log(log, f"❌ 工具包押金失敗：{detail}")
        return False


def _tool_process_v2(
    ws_deposit: gspread.Worksheet,
    ws_summary: gspread.Worksheet,
    ws_intro: gspread.Worksheet,
    deposit_amount: int,
    period: str,
    log: List[str],
) -> int:
    """
    依 salary_id 的「工具包押金」工作表處理下半月押金。

    規則：
    - 不動「場次時數薪資總表」第 1~120 列既有資料；工具包押金固定從第 121 列寫入。
    - 符合工具包押金資格者，自第 121 列起寫入：
        A 欄 = 姓名（薪資檔工具包押金 A 欄）
        B 欄 = 場次（薪資檔工具包押金 I 欄）
    - 不再把 G 欄提領日期寫到總表 B 欄。
    - G 欄提領日期仍依既有邏輯在薪資檔「工具包押金」內補寫，不影響上方總表資料。
    """
    rows = ws_deposit.get("A2:J") or []
    year, month = int(period[:4]), int(period[4:6])
    if month == 12:
        due = datetime.date(year + 1, 1, 10)
    else:
        due = datetime.date(year, month + 1, 10)
    due_text = due.strftime("%Y/%m/%d")

    # selected: (姓名, I欄場次)
    selected = []
    updates = []
    for offset, row in enumerate(rows, start=2):
        row += [""] * (10 - len(row))
        name = str(row[0]).strip()
        i_value = _to_num(row[8])
        current_due = str(row[6]).strip().replace("-", "/")
        try:
            current_due = datetime.datetime.strptime(
                current_due, "%Y/%m/%d"
            ).strftime("%Y/%m/%d")
        except ValueError:
            pass

        if (
            name
            and i_value >= DEPOSIT_THRESHOLD
            and (not current_due or current_due == due_text)
        ):
            # I 欄若為整數，寫入整數，避免 80.0 之類顯示。
            i_out = int(i_value) if float(i_value).is_integer() else i_value
            selected.append((name, i_out))
            if not current_due:
                updates.append({
                    "range": f"'{ws_deposit.title}'!G{offset}",
                    "values": [[due_text]],
                })

    if updates:
        ws_deposit.spreadsheet.values_batch_update({
            "valueInputOption": "USER_ENTERED", "data": updates
        })

    # 符合押金資格且 J 欄有介紹人：A=本人、B=介紹人、C=1000。
    intro_rows = []
    selected_names = {name for name, _i in selected}
    for row in rows:
        row += [""] * (10 - len(row))
        name = str(row[0]).strip()
        introducer = str(row[9]).strip()
        if name in selected_names and introducer:
            intro_rows.append([name, introducer, INTRO_BONUS])
    ws_intro.batch_clear(["A2:C"])
    if intro_rows:
        ws_intro.update(
            f"A2:C{len(intro_rows) + 1}",
            intro_rows,
            value_input_option="USER_ENTERED",
        )
    _log(log, f"  介紹獎金回填 {len(intro_rows)} 筆")

    # 工具包押金固定寫入「場次時數薪資總表」第 121 列起。
    # 嚴禁依姓名比對去清除、搬動或重排上方既有資料；A1:B120 完全不動。
    # 本次符合者依「工具包押金」工作表原列序寫入：A=姓名、B=I欄場次。
    append_start = 121

    if selected:
        end_row = append_start + len(selected) - 1
        ws_summary.update(
            f"A{append_start}:B{end_row}",
            [[name, i_value] for name, i_value in selected],
            value_input_option="USER_ENTERED",
        )
        _log(
            log,
            f"  工具包押金名單固定寫入 A{append_start}:B{end_row}："
            f"A=姓名、B=薪資檔 I 欄，共 {len(selected)} 筆；A1:B120 完全未變更",
        )
    else:
        _log(log, "  工具包押金無符合資料；A1:B120 完全不變更")

    # 元大工具包押金區：
    #   AD = 押金金額
    #   AE = 工具包押金姓名
    #   AB/AC = 以同列 AE 姓名去比對 H 欄姓名，取該列 I/J。
    # 例如 AE4=潘玟均，若 H73=潘玟均，則 AB4=I73、AC4=J73。
    # 這裡只處理 AB:AE，不改動 H:J 或其他既有資料。
    ws_summary.batch_clear(["AB4:AE120"])

    if selected:
        n = len(selected)
        end = 3 + n

        # 先固定寫入 AD/AE，之後 AB/AC 一律以 AE 的實際內容為比對依據。
        ws_summary.update(
            f"AD4:AE{end}",
            [[deposit_amount, name] for name, _i_value in selected],
            value_input_option="USER_ENTERED",
        )

        hij = ws_summary.get("H4:J120") or []
        account = {}
        for r in hij:
            if not r:
                continue
            h_name = str(r[0]).strip() if len(r) > 0 else ""
            if not h_name:
                continue
            i_val = r[1] if len(r) > 1 else ""
            j_val = r[2] if len(r) > 2 else ""
            account[h_name] = (i_val, j_val)

        ae_values = ws_summary.get(f"AE4:AE{end}") or []
        ab_ac = []
        missing = []
        for offset in range(n):
            ae_name = (
                str(ae_values[offset][0]).strip()
                if offset < len(ae_values) and ae_values[offset]
                else ""
            )
            i_val, j_val = account.get(ae_name, ("", ""))
            ab_ac.append([i_val, j_val])
            if ae_name and ae_name not in account:
                missing.append(ae_name)

        ws_summary.update(
            f"AB4:AC{end}", ab_ac, value_input_option="USER_ENTERED"
        )
        _log(log, f"  AB/AC 已依 AE 姓名比對 H 欄並帶入 I/J：{n} 筆")
        if missing:
            _log(log, "  ⚠️ H欄找不到姓名：" + "、".join(missing))

    _log(log, f"  工具包押金回填完成：{len(selected)} 筆，寫入起始列 A{append_start}")
    return len(selected)


def _tool_clear(
    ws_summary: gspread.Worksheet,
    ws_intro: gspread.Worksheet,
    log: List[str],
) -> None:
    """上半月：清空相關欄位。"""
    last_col = ws_summary.col_count
    last_ltr = _col_letter(last_col)

    # 清空 場次時數薪資總表 A121:E
    ws_summary.batch_clear([
        f"A{TOOL_DEPOSIT_START_ROW}:E",
        f"AB4:{_col_letter(AB_COL + 3)}",   # AB:AE = col28:31
    ])

    # 清空 介紹獎金 A2:C
    ws_intro.batch_clear(["A2:C"])

    _log(log, "    上半月：保留場次時數薪資總表 A:E；僅清空 AB4:AE 及介紹獎金 A2:C")


def _tool_process(
    ws_deposit: gspread.Worksheet,
    ws_summary: gspread.Worksheet,
    ws_intro: gspread.Worksheet,
    deposit_amount: int,
    log: List[str],
) -> Tuple[int, int]:
    """
    下半月：
    讀取「工具包押金」工作表，
        A欄=姓名, I欄=次數, J欄=備註（空白=介紹獎金，非空=工具包押金）
    工具包押金：I >= 80 且 J 非空白 → 場次時數薪資總表 A121起（A=J, B=A, C/D=空, E=押金）
    介紹獎金：  I >= 80 且 J 空白   → 介紹獎金工作表 A2起（A=J欄=空, B=A欄姓名, C=1000）

    注意：GAS 原版的 A=J欄, B=A欄 意思是：
        場次時數薪資總表 A欄 = 工具包押金 A欄（姓名）
        場次時數薪資總表 B欄 = 工具包押金 I欄（場次）
    """
    all_vals = ws_deposit.get("A2:J") or []

    dep_rows   = []
    intro_rows = []

    for row in all_vals:
        if not row:
            continue
        while len(row) < 10:
            row.append("")

        name   = str(row[0]).strip()   # A 欄（姓名）
        i_val  = _to_num(row[8])       # I 欄（次數）
        j_val  = str(row[9]).strip()   # J 欄（備註）

        if not name or i_val < DEPOSIT_THRESHOLD:
            continue

        if j_val:
            # 工具包押金
            dep_rows.append([j_val, name, "", "", deposit_amount])
        else:
            # 介紹獎金
            intro_rows.append([j_val, name, INTRO_BONUS])

    # 寫入場次時數薪資總表 A151 起
    if dep_rows:
        end_row = TOOL_DEPOSIT_START_ROW + len(dep_rows) - 1
        ws_summary.update(
            f"A{TOOL_DEPOSIT_START_ROW}:E{end_row}",
            dep_rows, value_input_option="USER_ENTERED"
        )
        _log(log, f"    工具包押金寫入 A{TOOL_DEPOSIT_START_ROW}:E{end_row}，共 {len(dep_rows)} 筆")

    # 寫入介紹獎金工作表 A2 起
    if intro_rows:
        ws_intro.batch_clear(["A2:C"])
        ws_intro.update(
            f"A2:C{1 + len(intro_rows)}",
            intro_rows, value_input_option="USER_ENTERED"
        )
        _log(log, f"    介紹獎金寫入 A2:C{1 + len(intro_rows)}，共 {len(intro_rows)} 筆")

    return len(dep_rows), len(intro_rows)


# ──────────────────────────────────────────────────────────────
# 元大帳戶
# ──────────────────────────────────────────────────────────────

def _yuanta_business_day(date_value: datetime.date, region_cfg: dict | None = None) -> datetime.date:
    """遇週末或設定的例假日，往前推至上一個工作日。"""
    cfg = region_cfg or {}
    holiday_values = cfg.get("holiday_dates", []) or []
    holidays = set()
    for value in holiday_values:
        text = str(value).strip().replace("-", "/")
        try:
            holidays.add(datetime.datetime.strptime(text, "%Y/%m/%d").date())
        except ValueError:
            continue

    d = date_value
    while d.weekday() >= 5 or d in holidays:
        d -= datetime.timedelta(days=1)
    return d


def _yuanta_target_date(period: str, is_first_half: bool, region_cfg: dict | None = None) -> datetime.date:
    """
    期別 YYYYMM-1：YYYYMM 當月 20 日。
    期別 YYYYMM-2：隔月 10 日。
    例如 202607-1 -> 20260720；202607-2 -> 20260810。
    遇週末／holiday_dates 中的例假日，往前提前至最近工作日。
    """
    year = int(period[:4])
    month = int(period[4:6])

    if is_first_half:
        target = datetime.date(year, month, 20)
    else:
        if month == 12:
            target = datetime.date(year + 1, 1, 10)
        else:
            target = datetime.date(year, month + 1, 10)

    return _yuanta_business_day(target, region_cfg)


def _yuanta_find_period_file(root_folder_id: str, period: str, file_name: str) -> tuple[str, str]:
    """回傳 (period_folder_id, spreadsheet_id)。"""
    from modules.auth import get_drive_service
    drive = get_drive_service()

    def _query(parent_id: str, name: str, mime_type: str):
        q = (
            f"'{parent_id}' in parents and name = '{name}' "
            f"and mimeType = '{mime_type}' and trashed = false"
        )
        resp = drive.files().list(
            q=q,
            fields="files(id,name)",
            supportsAllDrives=True,
            includeItemsFromAllDrives=True,
            pageSize=10,
        ).execute()
        files = resp.get("files", [])
        return files[0]["id"] if files else None

    period_folder_id = _query(
        root_folder_id, period, "application/vnd.google-apps.folder"
    )
    if not period_folder_id:
        raise FileNotFoundError(f"找不到期別資料夾：{period}")

    file_id = _query(
        period_folder_id, file_name, "application/vnd.google-apps.spreadsheet"
    )
    if not file_id:
        raise FileNotFoundError(f"找不到元大帳戶檔案：{file_name}")
    return period_folder_id, file_id


def _yuanta_export_xlsx(spreadsheet_id: str, folder_id: str, output_name: str, log: List[str] | None = None) -> None:
    """將 Google 試算表匯出 xlsx，並由 Jenny OAuth 寫入期別資料夾。

    Service Account 只負責 export 原始 Google Sheet；
    搜尋／覆蓋／建立 xlsx 一律使用 Jenny OAuth，避免 Service Account storageQuotaExceeded。
    """
    from modules.auth import get_drive_service, get_jenny_drive_service
    from googleapiclient.http import MediaIoBaseUpload

    def _elog(message: str) -> None:
        if log is not None:
            _log(log, message)

    def _detail(exc: Exception) -> str:
        parts = [type(exc).__name__]
        text = str(exc).strip()
        if text:
            parts.append(text)
        resp = getattr(exc, "resp", None)
        if resp is not None:
            status = getattr(resp, "status", None)
            reason = getattr(resp, "reason", None)
            if status is not None:
                parts.append(f"HTTP status={status}")
            if reason:
                parts.append(f"HTTP reason={reason}")
        content = getattr(exc, "content", None)
        if content:
            try:
                decoded = content.decode("utf-8", errors="replace") if isinstance(content, (bytes, bytearray)) else str(content)
                if decoded.strip():
                    parts.append(f"API content={decoded.strip()}")
            except Exception:
                parts.append(f"API content={content!r}")
        details = getattr(exc, "error_details", None)
        if details:
            parts.append(f"error_details={details}")
        return " | ".join(parts)

    source_drive = get_drive_service()          # Service Account: 只下載既有 Google Sheet
    drive = get_jenny_drive_service()           # Jenny OAuth: 搜尋/覆蓋/建立實體 xlsx

    try:
        about = drive.about().get(fields="user").execute()
        user = about.get("user", {}) or {}
        identity = user.get("emailAddress") or user.get("displayName") or "未知"
        _elog(f"  xlsx 寫入身分（Jenny OAuth）：{identity}")
    except Exception as exc:
        raise RuntimeError(f"Jenny OAuth Drive 驗證失敗｜{_detail(exc)}") from exc

    try:
        _elog(f"  匯出 xlsx：下載並準備存成 {output_name}")
        request = source_drive.files().export_media(
            fileId=spreadsheet_id,
            mimeType="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        )
        data = request.execute()
        _elog(f"  匯出 xlsx：下載完成（{len(data)} bytes）")
    except Exception as exc:
        raise RuntimeError(f"匯出 Google 試算表失敗｜{_detail(exc)}") from exc

    safe_name = output_name.replace("'", "\\'")
    q = (
        f"'{folder_id}' in parents and name = '{safe_name}' "
        "and trashed = false"
    )
    try:
        active = drive.files().list(
            q=q,
            fields="files(id,name,modifiedTime)",
            supportsAllDrives=True,
            includeItemsFromAllDrives=True,
            orderBy="modifiedTime desc",
            pageSize=100,
        ).execute().get("files", [])
    except Exception as exc:
        raise RuntimeError(f"Jenny OAuth 查詢同名 xlsx 失敗｜{_detail(exc)}") from exc

    if active:
        target = active[0]
        _elog(f"  匯出 xlsx：找到同名檔，Jenny OAuth 直接覆蓋 {target.get('name', output_name)}")
        media = MediaIoBaseUpload(
            io.BytesIO(data),
            mimetype="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            resumable=False,
        )
        try:
            updated = drive.files().update(
                fileId=target["id"],
                media_body=media,
                body={"name": output_name},
                supportsAllDrives=True,
                fields="id,name",
            ).execute()
            _elog(f"  匯出 xlsx：覆蓋完成 {updated.get('name', output_name)}")
            return
        except Exception as exc:
            raise RuntimeError(f"Jenny OAuth 覆蓋同名 xlsx 失敗（{output_name}）｜{_detail(exc)}") from exc

    _elog(f"  匯出 xlsx：無同名檔，Jenny OAuth 建立新檔 {output_name}")
    try:
        folder_meta = drive.files().get(
            fileId=folder_id,
            fields="id,name,driveId,capabilities(canAddChildren,canEdit)",
            supportsAllDrives=True,
        ).execute()
        caps = folder_meta.get("capabilities", {}) or {}
        _elog(
            f"  Jenny OAuth 目標資料夾：name={folder_meta.get('name','')}；"
            f"id={folder_meta.get('id',folder_id)}；driveId={folder_meta.get('driveId','MyDrive/無')}；"
            f"canAddChildren={caps.get('canAddChildren')}；canEdit={caps.get('canEdit')}"
        )
    except Exception as exc:
        _elog(f"  ⚠️ Jenny OAuth 無法取得目標資料夾資訊：{_detail(exc)}")

    media = MediaIoBaseUpload(
        io.BytesIO(data),
        mimetype="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        resumable=False,
    )
    try:
        created = drive.files().create(
            body={"name": output_name, "parents": [folder_id]},
            media_body=media,
            fields="id,name,parents,driveId",
            supportsAllDrives=True,
        ).execute()
        _elog(f"  匯出 xlsx：Jenny OAuth 新檔建立完成 {created.get('name', output_name)}")
    except Exception as exc:
        raise RuntimeError(f"Jenny OAuth 建立新 xlsx 失敗（{output_name}）｜{_detail(exc)}") from exc

def _yuanta_nonempty_rows(rows: list[list], width: int = 4) -> list[list]:
    """保留至少一格有值的資料列，並補齊固定欄數。"""
    result = []
    for row in rows:
        padded = list(row[:width]) + [""] * max(0, width - len(row))
        if any(str(v).strip() for v in padded):
            result.append(padded[:width])
    return result


def _yuanta_wait_values(
    ws: gspread.Worksheet,
    a1_range: str,
    expected_rows: list[list],
    log: List[str],
    timeout: int = 30,
) -> None:
    """等待 Sheets 寫入可被重新讀取，避免 Drive export 抓到寫入前的舊版本。"""
    import time

    deadline = time.time() + timeout
    expected_count = len(expected_rows)
    while time.time() < deadline:
        actual = ws.get(a1_range, value_render_option="UNFORMATTED_VALUE") or []
        actual_count = sum(1 for row in actual if any(str(v).strip() for v in row))
        if actual_count >= expected_count:
            _log(log, f"  已確認 {ws.title}!{a1_range} 寫入完成（{actual_count} 筆）")
            time.sleep(2)  # Drive export 與 Sheets API 之間仍可能有短暫同步延遲
            return
        _log(log, f"  等待 {ws.title}!{a1_range} 同步：{actual_count}/{expected_count}")
        time.sleep(2)
    raise TimeoutError(
        f"{ws.title}!{a1_range} 寫入後 {timeout} 秒仍未同步完成，取消匯出 xlsx"
    )


def _yuanta_find_other_file(
    root_folder_id: str, period: str, region: str
) -> tuple[str, str]:
    """找到當期其他承攬試算表，回傳 (period_folder_id, file_id)。"""
    other_name = f"{period}其他承攬-{region}"
    try:
        return _yuanta_find_period_file(root_folder_id, period, other_name)
    except FileNotFoundError as exc:
        raise FileNotFoundError(f"找不到其他承攬檔案：{other_name}") from exc


def run_yuanta(
    cleaning_file_id: str,
    region: str,
    period: str,
    is_first_half: bool,
    log: List[str],
    region_cfg: dict = None,
    **kwargs,
) -> bool:
    """
    元大帳戶。上／下半月完全依 period 的 -1 / -2 判斷。

    承攬費流程：
      1. 清潔承攬：-1 取 N4:Q；-2 取 U4:X -> all!A2:D
      2. 其他承攬：從「薪資總表」-1 取 N3:Q；-2 取 U3:X，接在 all 的最後一筆非空白列之後
      3. all C 欄必須非空白且不等於「現金」 -> 元大!B3:E
      4. 元大 A 欄：-1=YYYYMM20；-2=YYYYMM10；非工作日往前移
      5. 元大 H 欄 = YYYYMM
      6. 驗證 all / 元大 寫入完成後才匯出 xlsx

    下半月工具包押金另依 A121 與 AB4:AE 產出。
    """
    if period.endswith("-1"):
        is_first_half = True
    elif period.endswith("-2"):
        is_first_half = False
    else:
        _log(log, f"❌ 元大帳戶失敗：期別格式錯誤：{period}（應為 YYYYMM-1 或 YYYYMM-2）")
        return False

    label = "上半月" if is_first_half else "下半月"
    _log(log, f"▶ 元大帳戶 {label} 開始（依期別 {period} 判斷）")
    try:
        cfg = region_cfg or {}
        root_folder_id = str(cfg.get("root_folder_id", "") or "").strip()
        if not root_folder_id:
            raise ValueError("config 地區設定缺少 root_folder_id")

        gc = get_gspread_client()
        cleaning_ss = gc.open_by_key(cleaning_file_id)
        ws_summary = cleaning_ss.worksheet("場次時數薪資總表")

        yuanta_name = f"{period}元大帳戶-{region}"
        period_folder_id, yuanta_file_id = _yuanta_find_period_file(
            root_folder_id, period, yuanta_name
        )
        yuanta_ss = gc.open_by_key(yuanta_file_id)
        ws_all = yuanta_ss.worksheet("all")
        ws_yuanta = yuanta_ss.worksheet("元大")
        _log(log, f"  找到元大帳戶檔案：{yuanta_name}")

        source_range = "N4:Q" if is_first_half else "U4:X"
        other_source_range = "N3:Q" if is_first_half else "U3:X"

        # 1) 清潔承攬先放 all A2:D
        cleaning_rows = _yuanta_nonempty_rows(
            ws_summary.get(source_range, value_render_option="UNFORMATTED_VALUE") or []
        )
        ws_all.batch_clear(["A2:D"])
        next_row = 2
        if cleaning_rows:
            end = next_row + len(cleaning_rows) - 1
            ws_all.update(
                f"A{next_row}:D{end}", cleaning_rows, value_input_option="USER_ENTERED"
            )
            next_row = end + 1
        _log(log, f"  清潔承攬 {source_range} -> all!A2:D，共 {len(cleaning_rows)} 筆")

        # 2) 其他承攬接續寫在 all 第一個空白列
        _period_folder_id2, other_file_id = _yuanta_find_other_file(
            root_folder_id, period, region
        )
        other_ss = gc.open_by_key(other_file_id)
        other_ws = other_ss.worksheet("薪資總表")
        other_rows = _yuanta_nonempty_rows(
            other_ws.get(other_source_range, value_render_option="UNFORMATTED_VALUE") or []
        )
        if other_rows:
            other_start = next_row
            other_end = other_start + len(other_rows) - 1
            ws_all.update(
                f"A{other_start}:D{other_end}",
                other_rows,
                value_input_option="USER_ENTERED",
            )
            next_row = other_end + 1
            _log(
                log,
                f"  其他承攬 {other_source_range} -> all!A{other_start}:D{other_end}，共 {len(other_rows)} 筆",
            )
        else:
            _log(log, f"  其他承攬 {other_source_range} 無有效資料")

        all_rows = cleaning_rows + other_rows
        if all_rows:
            _yuanta_wait_values(
                ws_all, f"A2:D{1 + len(all_rows)}", all_rows, log
            )

        # 3) B 欄：排除空白與「現金」
        bank_rows = []
        for row in all_rows:
            b_value = str(row[1] if len(row) > 1 else "").strip()
            if b_value and b_value != "現金":
                bank_rows.append(row)

        ws_yuanta.batch_clear(["A3:H"])
        if bank_rows:
            n = len(bank_rows)
            end = 2 + n
            target_date = _yuanta_target_date(period, is_first_half, cfg)
            yyyymm = period[:6]
            ws_yuanta.update(
                f"B3:E{end}", bank_rows, value_input_option="USER_ENTERED"
            )
            ws_yuanta.update(
                f"A3:A{end}",
                [[target_date.strftime("%Y%m%d")]] * n,
                value_input_option="USER_ENTERED",
            )
            ws_yuanta.update(
                f"H3:H{end}", [[yyyymm]] * n, value_input_option="USER_ENTERED"
            )
            _log(
                log,
                f"  all B欄非空白且≠現金：{n} 筆 -> 元大 B3:E；"
                f"A欄={target_date:%Y%m%d}；H欄={yyyymm}",
            )
            _yuanta_wait_values(ws_yuanta, f"B3:E{end}", bank_rows, log)
        else:
            _log(log, "  all B欄非空白且≠現金：0 筆")

        # 4) 確認 all / 元大 都同步完成後才匯出，避免 xlsx 抓到空白舊版本。
        fee_name = f"{period}元大承攬費-{region}.xlsx"
        _yuanta_export_xlsx(yuanta_file_id, period_folder_id, fee_name, log)
        _log(log, f"  ✅ 已另存：{fee_name}")

        # 5) 下半月工具包押金
        if not is_first_half:
            a121 = str(ws_summary.acell("A121").value or "").strip()
            if a121:
                deposit_rows = _yuanta_nonempty_rows(
                    ws_summary.get(
                        "AB4:AE", value_render_option="UNFORMATTED_VALUE"
                    ) or []
                )
                if deposit_rows:
                    ws_yuanta.batch_clear(["A4:H"])
                    end = 3 + len(deposit_rows)
                    ws_yuanta.update(
                        f"B4:E{end}",
                        deposit_rows,
                        value_input_option="USER_ENTERED",
                    )
                    _yuanta_wait_values(
                        ws_yuanta, f"B4:E{end}", deposit_rows, log
                    )
                    deposit_name = f"{period}元大工具包押金-{region}.xlsx"
                    _yuanta_export_xlsx(
                        yuanta_file_id, period_folder_id, deposit_name, log
                    )
                    _log(
                        log,
                        f"  ✅ A121 非空白；AB4:AE -> 元大 B4:E，共 {len(deposit_rows)} 筆；"
                        f"已另存：{deposit_name}",
                    )
                else:
                    _log(log, "  A121 非空白，但 AB4:AE 無有效資料，略過工具包押金 xlsx")
            else:
                _log(log, "  A121 空白，略過元大工具包押金 xlsx")

        ts = _now_ts()
        record_execution(region, period, "元大帳戶", None)
        _log(log, f"✅ 元大帳戶 {label} 完成｜{ts}")
        return True

    except Exception as e:
        detail = str(e).strip() or f"{type(e).__name__}: {e!r}"
        _log(log, f"❌ 元大帳戶失敗：{detail}")
        return False
