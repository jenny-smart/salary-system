"""
Lemon Clean 清潔承攬 — PDF 產出
檔案：modules/cleaning_pdf.py

原理：
    用 Service Account credentials 取得 OAuth token，
    呼叫 Google Sheets export API 將「薪資單」工作表輸出為 PDF，
    存至 Drive：{地區根目錄}/{期別}/{期別} 子資料夾。

流程（對應 GAS generateSalaryPDFsByConfig_）：
    1. 讀取 PDF產出 / 專案PDF產出 工作表（H=Y 的姓名）
    2. 逐人：薪資單 AD2 寫入姓名 → 等待公式計算 → export API → 存 Drive
    3. 成功：D欄寫時間、E欄寫連結、H欄清空 Y
    4. 失敗：保留 H=Y 以便重跑

PDF工作設定：
    CLEANING：PDF產出 / 薪資單 / AB1:AH{last_row}
    PROJECT ：專案PDF產出 / 專案薪資單 / AB1:AH{last_row}

資料夾路徑：{根目錄}/{期別}/{期別}（與 GAS 相同）
"""

from __future__ import annotations

import datetime
import io
import json
import time
from typing import List, Optional

import gspread
import requests

from modules.auth import get_gspread_client, get_drive_service
import streamlit as st


# ──────────────────────────────────────────────────────────────
# 常數
# ──────────────────────────────────────────────────────────────

TS_FMT    = "%Y/%m/%d %H:%M"
TIMEZONE  = "Asia/Taipei"

PDF_JOBS = {
    "CLEANING": {
        "list_sheet":   "PDF產出",
        "salary_sheet": "薪資單",
        "file_title":   "清潔承攬服務費",
    },
    "PROJECT": {
        "list_sheet":   "專案PDF產出",
        "salary_sheet": "專案薪資單",
        "file_title":   "清潔專案承攬服務費",
    },
}


# ──────────────────────────────────────────────────────────────
# 主函數（salaryapp.py 呼叫）
# ──────────────────────────────────────────────────────────────

def run_pdf(
    cleaning_file_id: str,
    root_folder_id: str,
    region: str,
    period: str,
    job_type: str,
    log: List[str],
    region_cfg: dict = None,
    **kwargs,
) -> dict:
    """
    產出 PDF bytes，回傳 {name: bytes} 供 Streamlit 下載。

    Service Account 沒有 Drive 儲存空間，無法直接建立檔案，
    改為把 PDF bytes 回傳給 salaryapp.py 提供下載按鈕。

    Returns:
        {"pdfs": {name: bytes}, "failed": [name, ...]}
    """
    job   = PDF_JOBS.get(job_type, PDF_JOBS["CLEANING"])
    label = job["file_title"]
    _log(log, f"▶ PDF產出 [{label}] {region} {period} 開始")

    result = {"pdfs": {}, "failed": []}

    try:
        gc = get_gspread_client()
        ss = gc.open_by_key(cleaning_file_id)

        ws_list   = ss.worksheet(job["list_sheet"])

        raw = ws_list.get("A2:H", value_render_option="UNFORMATTED_VALUE") or []
        targets = []
        for i, row in enumerate(raw):
            name = str(row[1]).strip() if len(row) > 1 else ""
            flag = str(row[7]).strip() if len(row) > 7 else ""
            if name and flag == "Y":
                targets.append({"name": name, "row": i + 2})

        if not targets:
            _log(log, f"    [{job['list_sheet']}] 無 H=Y 的待產出人員")
            return result

        _log(log, f"    待產出：{len(targets)} 人")

        token = _get_access_token()
        oauth_drive, folder_id = _prepare_drive_output(root_folder_id, period, log)

        # 讀取來源工作表
        if job_type == "PROJECT":
            src_sheet_name = "專案薪資表"
            salary_sheet_name = "專案薪資單"
        else:
            src_sheet_name = "薪資表"
            salary_sheet_name = "薪資單"

        _log(log, f"    讀取{src_sheet_name}資料...")
        ws_main   = ss.worksheet(src_sheet_name)
        ws_salary = ss.worksheet(salary_sheet_name)
        salary_sheet_id = ws_salary.id

        main_raw  = ws_main.get(
            "A2:G",
            value_render_option="UNFORMATTED_VALUE",
            date_time_render_option="FORMATTED_STRING"
        ) or []

        for i, target in enumerate(targets):
            name = target["name"]
            row  = target["row"]
            _log(log, f"    [{i+1}/{len(targets)}] 產出：{name}")

            try:
                # 1. 清空舊明細（AB31:AF，不動 AD1/AD2）
                ws_salary.batch_clear(["AB31:AF"])

                # 2. 篩選來源工作表 F 欄包含此姓名的列
                # A=0, B=1, C=2, D=3, E=4, F=5, G=6
                detail_rows = []
                for r in main_raw:
                    while len(r) < 7:
                        r.append("")
                    f_val = str(r[5]).strip()
                    if name in f_val:
                        b_val = str(r[1]).strip()
                        c_val = str(r[2]).strip()
                        e_val = str(r[4]).strip()
                        g_val = r[6]
                        detail_rows.append([
                            f"{b_val}（{c_val}）",  # AC
                            e_val,                   # AD
                            g_val,                   # AE
                            f_val,                   # AF
                        ])

                if detail_rows:
                    n = len(detail_rows)
                    end_r = 30 + n

                    # AB 欄序號（只有 AC 有資料才寫，從 AB31 起）
                    ab_data = [[idx + 1] for idx in range(n)]
                    ws_salary.update(f"AB31:AB{end_r}", ab_data, value_input_option="USER_ENTERED")

                    # AC:AF 明細
                    ws_salary.update(f"AC31:AF{end_r}", detail_rows, value_input_option="USER_ENTERED")
                    _log(log, f"      明細寫入：{n} 筆")
                else:
                    _log(log, f"      ⚠️ {src_sheet_name}中找不到含「{name}」的資料")

                # 3. 寫入 AD2 姓名（不動 AD1）
                ws_salary.update_cell(2, 30, name)
                time.sleep(3.0)

                # 4. 匯出範圍
                last_export_row = 30 + len(detail_rows) + 3 if detail_rows else 40
                _log(log, f"      匯出範圍：AB1:AH{last_export_row}")

                pdf_bytes = _export_pdf(
                    token          = token,
                    spreadsheet_id = cleaning_file_id,
                    sheet_gid      = salary_sheet_id,
                    export_range   = f"AB1:AH{last_export_row}",
                )

                if len(pdf_bytes) < 1000:
                    raise ValueError(f"PDF 過小（{len(pdf_bytes)} bytes）")

                file_title = f"{period}_{label}_{name}.pdf"
                result["pdfs"][file_title] = pdf_bytes

                now_str = datetime.datetime.now().strftime(TS_FMT)
                updates = [
                    {"range": f"D{target['row']}", "values": [[now_str]]},
                    {"range": f"H{target['row']}", "values": [[""]]},
                ]
                uploaded = False

                if oauth_drive and folder_id:
                    try:
                        # 上傳至 Drive（OAuth）。若 Drive 失敗，仍保留下載檔，不讓 PDF 產出歸零。
                        existing_url = _get_cell(ws_list, target["row"], 5)  # E欄
                        drive_url    = _upload_or_update_drive(
                            oauth_drive, folder_id, pdf_bytes, file_title, existing_url
                        )
                        if not existing_url:
                            updates.append({"range": f"E{target['row']}", "values": [[drive_url]]})
                        uploaded = True
                    except Exception as upload_error:
                        _log(log, f"      ⚠️ Drive 上傳失敗，已保留下載檔：{_format_error(upload_error)}")
                else:
                    _log(log, "      ⚠️ Drive 未啟用，已保留下載檔")

                # 回寫 D（產出時間）與 H（清空待產出）。E 僅在 Drive 上傳成功且原本空白時寫入。
                ws_list.spreadsheet.values_batch_update({
                    "valueInputOption": "USER_ENTERED",
                    "data": [
                        {"range": f"'{ws_list.title}'!{u['range']}", "values": u["values"]}
                        for u in updates
                    ],
                })
                if uploaded:
                    _log(log, f"      ✅ {name} PDF 產出並上傳完成")
                else:
                    _log(log, f"      ✅ {name} PDF 產出成功（請用下方下載按鈕儲存）")

            except Exception as e:
                _log(log, f"      ❌ {name} 失敗：{_format_error(e)}")
                result["failed"].append(name)

            time.sleep(0.8)

        total   = len(result["pdfs"])
        failed  = len(result["failed"])
        _log(log, f"✅ PDF產出完成：成功 {total} 份，失敗 {failed} 份")
        if total > 0:
            _log(log, f"    請點擊下方下載按鈕儲存 PDF")
        return result

    except Exception as e:
        _log(log, f"❌ PDF產出失敗：{_format_error(e)}")
        return result


def _prepare_drive_output(root_folder_id: str, period: str, log: List[str]):
    """
    嘗試準備 Drive 上傳目的地。

    PDF 生成本身不應依賴 Drive/OAuth；Drive 失敗時回傳 (None, None)，
    run_pdf 仍會把 PDF bytes 放進 Streamlit 下載區。
    """
    try:
        drive = _get_oauth_drive_service()
        about = drive.about().get(fields="user(emailAddress,displayName)").execute()
        user = about.get("user", {})
        _log(log, f"    Drive OAuth 使用者：{user.get('emailAddress') or user.get('displayName')}")
        folder_id = _get_or_create_pdf_folder(root_folder_id, period, drive)
        _log(log, "    Drive 資料夾準備完成")
        return drive, folder_id
    except Exception as e:
        _log(log, f"    ⚠️ Drive 上傳未啟用：{_format_error(e)}")
        return None, None


def _get_or_create_pdf_folder(root_id: str, period: str, drive=None) -> str:
    """取得或建立 {根目錄}/{期別}/{期別} 資料夾，回傳最內層資料夾 ID。
    drive：優先用傳入的 OAuth drive，否則用 Service Account drive。
    """
    if drive is None:
        drive = get_drive_service()

    def _find_or_create(parent_id: str, name: str) -> str:
        q = (
            f"'{parent_id}' in parents"
            f" and name = '{name}'"
            f" and mimeType = 'application/vnd.google-apps.folder'"
            f" and trashed = false"
        )
        resp = drive.files().list(
            q=q,
            fields="files(id, name)",
            supportsAllDrives=True,
            includeItemsFromAllDrives=True,
            pageSize=5,
        ).execute()
        files = resp.get("files", [])
        if files:
            return files[0]["id"]
        # 建立新資料夾
        meta = {
            "name":     name,
            "mimeType": "application/vnd.google-apps.folder",
            "parents":  [parent_id],
        }
        f = drive.files().create(
            body=meta,
            fields="id",
            supportsAllDrives=True,
        ).execute()
        return f["id"]

    period_id = _find_or_create(root_id, period)
    sub_id    = _find_or_create(period_id, period)
    return sub_id


# ──────────────────────────────────────────────────────────────
# Google Sheets → PDF export
# ──────────────────────────────────────────────────────────────



# ──────────────────────────────────────────────────────────────
# OAuth Drive 操作（用 jenny@hers.com.tw 的權限上傳/更新檔案）
# ──────────────────────────────────────────────────────────────

def _get_oauth_drive_service():
    """
    用 Streamlit Secrets 的 [oauth_drive] 建立 OAuth Drive 服務。
    使用 refresh_token 自動更新 access token。
    """
    from google.oauth2.credentials import Credentials
    from googleapiclient.discovery import build

    cfg = st.secrets["oauth_drive"]
    creds = Credentials(
        token         = None,
        refresh_token = cfg["refresh_token"],
        token_uri     = cfg["token_uri"],
        client_id     = cfg["client_id"],
        client_secret = cfg["client_secret"],
        scopes        = ["https://www.googleapis.com/auth/drive"],
    )
    # 自動 refresh
    import google.auth.transport.requests
    creds.refresh(google.auth.transport.requests.Request())
    return build("drive", "v3", credentials=creds)


def _upload_or_update_drive(
    oauth_drive,
    folder_id: str,
    pdf_bytes: bytes,
    file_name: str,
    existing_url: str = "",
) -> str:
    """
    E欄空白 → 建立新檔，回傳連結
    E欄已有連結 → 更新原檔內容，回傳原連結
    """
    from googleapiclient.http import MediaIoBaseUpload
    import io

    existing_file_id = _extract_file_id(existing_url)
    media = MediaIoBaseUpload(io.BytesIO(pdf_bytes), mimetype="application/pdf")

    if existing_file_id:
        # 更新原檔內容（不改連結）
        oauth_drive.files().update(
            fileId     = existing_file_id,
            body       = {"name": file_name},
            media_body = media,
            supportsAllDrives = True,
        ).execute()
        return existing_url
    else:
        # 建立新檔
        meta   = {"name": file_name, "parents": [folder_id]}
        result = oauth_drive.files().create(
            body       = meta,
            media_body = media,
            fields     = "id",
            supportsAllDrives = True,
        ).execute()
        file_id = result["id"]
        # 設為任何人可讀（方便發連結給人員）
        oauth_drive.permissions().create(
            fileId = file_id,
            body   = {"type": "anyone", "role": "reader"},
            supportsAllDrives = True,
        ).execute()
        return f"https://drive.google.com/file/d/{file_id}/view"

def _get_access_token() -> str:
    """
    從 Service Account credentials 取得有效的 OAuth2 access token。
    auth.py 的 get_credentials() 有 @st.cache_resource，
    credentials 物件可能 token 已過期，需要先 refresh。
    """
    import google.auth.transport.requests
    from modules.auth import get_credentials

    creds = get_credentials()

    # Service Account credentials 不需要 refresh，直接取 token
    # 但若 token 為空（第一次）需要先執行一次請求來取得
    if not creds.token or not creds.valid:
        request = google.auth.transport.requests.Request()
        creds.refresh(request)

    return creds.token


def _export_pdf(
    token: str,
    spreadsheet_id: str,
    sheet_gid: int,
    export_range: str,
) -> bytes:
    """
    呼叫 Google Sheets export API 產出 PDF bytes。
    對應 GAS：UrlFetchApp.fetch(exportUrl, {headers: {Authorization: "Bearer " + token}})
    """
    base_url = f"https://docs.google.com/spreadsheets/d/{spreadsheet_id}/export"
    params = {
        "exportFormat":  "pdf",
        "format":        "pdf",
        "gid":           str(sheet_gid),
        "range":         export_range,
        "size":          "A4",
        "portrait":      "true",
        "fitw":          "true",
        "sheetnames":    "false",
        "printtitle":    "false",
        "pagenum":       "false",
        "gridlines":     "false",
        "fzr":           "false",
        "top_margin":    "0.5",
        "bottom_margin": "0.5",
        "left_margin":   "0.5",
        "right_margin":  "0.5",
    }
    headers = {"Authorization": f"Bearer {token}"}
    resp = requests.get(base_url, params=params, headers=headers, timeout=60)
    if resp.status_code != 200:
        raise ValueError(f"PDF export 失敗，HTTP {resp.status_code}: {resp.text[:200]}")
    if not resp.content.startswith(b"%PDF"):
        content_type = resp.headers.get("content-type", "")
        preview = resp.text[:200].replace("\n", " ")
        raise ValueError(f"PDF export 回傳非 PDF 內容（{content_type}）：{preview}")
    return resp.content


def _find_last_export_row(ws_salary: gspread.Worksheet) -> int:
    """找 AB 欄最後有值的列（確保不匯出空白頁）。"""
    ab_vals = ws_salary.col_values(28)   # AB = col 28
    last = 1
    for i in range(len(ab_vals) - 1, -1, -1):
        if str(ab_vals[i]).strip():
            last = i + 1
            break
    return max(last, 20)


# ──────────────────────────────────────────────────────────────
# Drive 檔案操作
# ──────────────────────────────────────────────────────────────

def _create_drive_file(drive, folder_id: str, pdf_bytes: bytes, name: str) -> str:
    """在 Drive 建立新 PDF 檔案，回傳 file ID。"""
    from googleapiclient.http import MediaIoBaseUpload
    meta   = {"name": name, "parents": [folder_id]}
    media  = MediaIoBaseUpload(io.BytesIO(pdf_bytes), mimetype="application/pdf")
    result = drive.files().create(
        body=meta, media_body=media, fields="id", supportsAllDrives=True
    ).execute()
    return result["id"]


def _update_drive_file(drive, file_id: str, pdf_bytes: bytes, name: str):
    """更新既有 Drive 檔案內容。"""
    from googleapiclient.http import MediaIoBaseUpload
    media = MediaIoBaseUpload(io.BytesIO(pdf_bytes), mimetype="application/pdf")
    drive.files().update(
        fileId=file_id, body={"name": name}, media_body=media,
        supportsAllDrives=True
    ).execute()


def _extract_file_id(url: str) -> Optional[str]:
    """從 Drive 連結取出 file ID。"""
    if not url:
        return None
    import re
    m = re.search(r"/d/([a-zA-Z0-9_-]+)", str(url))
    return m.group(1) if m else None


def _get_cell(ws: gspread.Worksheet, row: int, col: int) -> str:
    """安全讀取儲存格值。"""
    try:
        return str(ws.cell(row, col).value or "").strip()
    except Exception:
        return ""


def _format_error(error: Exception) -> str:
    """把 Google/Streamlit 例外轉成日誌可讀的訊息，避免只看到空白錯誤。"""
    if hasattr(error, "resp") and getattr(error, "resp", None):
        status = getattr(error.resp, "status", "")
        reason = getattr(error.resp, "reason", "")
        content = getattr(error, "content", b"")
        if isinstance(content, bytes):
            content = content.decode("utf-8", errors="replace")
        try:
            payload = json.loads(content) if content else {}
            message = payload.get("error", {}).get("message") or content
        except Exception:
            message = content
        return f"HTTP {status} {reason} {str(message)[:300]}".strip()

    if hasattr(error, "status_code"):
        text = getattr(error, "text", "")
        return f"HTTP {error.status_code}: {str(text)[:300]}"

    message = str(error).strip()
    if message:
        return message
    return repr(error)


# ──────────────────────────────────────────────────────────────
# 工具
# ──────────────────────────────────────────────────────────────

def _log(log: List[str], msg: str) -> None:
    log.append(msg)
