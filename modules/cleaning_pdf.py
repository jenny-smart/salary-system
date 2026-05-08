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
import time
from typing import List, Optional

import gspread
import requests

from modules.auth import get_gspread_client, get_drive_service


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
        ws_salary = ss.worksheet(job["salary_sheet"])

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

        token           = _get_access_token()
        salary_sheet_id = ws_salary.id

        for i, target in enumerate(targets):
            name = target["name"]
            row  = target["row"]
            _log(log, f"    [{i+1}/{len(targets)}] 產出：{name}")

            try:
                ws_salary.update_cell(2, 30, name)
                time.sleep(2.5)

                last_export_row = _find_last_export_row(ws_salary)
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
                _log(log, f"      ✅ {name} PDF 產出成功（{len(pdf_bytes):,} bytes）")

            except Exception as e:
                if hasattr(e, 'reason'):
                    err_msg = f"HttpError {e.status_code}: {e.reason}"
                else:
                    err_msg = str(e) or repr(e)
                _log(log, f"      ❌ {name} 失敗：{err_msg}")
                result["failed"].append(name)

            time.sleep(0.8)

        total   = len(result["pdfs"])
        failed  = len(result["failed"])
        _log(log, f"✅ PDF產出完成：成功 {total} 份，失敗 {failed} 份")
        if total > 0:
            _log(log, f"    請點擊下方下載按鈕儲存 PDF")
        return result

    except Exception as e:
        _log(log, f"❌ PDF產出失敗：{e}")
        return result
    """
    產出 PDF。

    Args:
        cleaning_file_id : 清潔承攬試算表 ID
        root_folder_id   : 地區根目錄 Drive ID（config.yaml root_folder_id）
        region           : 地區名稱（用於檔名）
        period           : 期別，如 "202604-2"
        job_type         : "CLEANING" 或 "PROJECT"
        log              : 日誌列表
    """
    job   = PDF_JOBS.get(job_type, PDF_JOBS["CLEANING"])
    label = job["file_title"]
    _log(log, f"▶ PDF產出 [{label}] {region} {period} 開始")

    try:
        gc = get_gspread_client()
        ss = gc.open_by_key(cleaning_file_id)

        ws_list   = ss.worksheet(job["list_sheet"])
        ws_salary = ss.worksheet(job["salary_sheet"])

        # 讀取待產出名單（B欄=姓名, H欄=Y）
        last_row = ws_list.row_count
        raw = ws_list.get("A2:H", value_render_option="UNFORMATTED_VALUE") or []
        targets = []
        for i, row in enumerate(raw):
            name = str(row[1]).strip() if len(row) > 1 else ""
            flag = str(row[7]).strip() if len(row) > 7 else ""
            if name and flag == "Y":
                targets.append({"name": name, "row": i + 2})

        if not targets:
            _log(log, f"    [{job['list_sheet']}] 無 H=Y 的待產出人員")
            return True

        _log(log, f"    待產出：{len(targets)} 人")

        # 取得 Drive 目標資料夾
        folder_id = _get_or_create_pdf_folder(root_folder_id, period)
        _log(log, f"    Drive 資料夾：{folder_id}")

        # 取得 OAuth token（Service Account）
        token = _get_access_token()

        # 逐人產出
        salary_sheet_id = ws_salary.id
        count   = 0
        skipped = 0

        for i, target in enumerate(targets):
            name = target["name"]
            row  = target["row"]
            _log(log, f"    [{i+1}/{len(targets)}] 產出：{name}")

            try:
                # 1. 寫入姓名到 AD2（col 30 = AD）
                ws_salary.update_cell(2, 30, name)
                time.sleep(2.5)   # 等公式計算

                # 2. 找最後一列（AB欄）
                last_export_row = _find_last_export_row(ws_salary)
                _log(log, f"      匯出範圍：AB1:AH{last_export_row}")

                # 3. export PDF
                _log(log, f"      呼叫 export API...")
                export_range = f"AB1:AH{last_export_row}"
                pdf_bytes    = _export_pdf(
                    token         = token,
                    spreadsheet_id= cleaning_file_id,
                    sheet_gid     = salary_sheet_id,
                    export_range  = export_range,
                )
                _log(log, f"      PDF 大小：{len(pdf_bytes)} bytes")

                if len(pdf_bytes) < 1000:
                    raise ValueError(f"PDF 過小（{len(pdf_bytes)} bytes），可能為空白頁")

                # 4. 存到 Drive
                file_title = f"{period}_{label}_{name}.pdf"
                _log(log, f"      上傳至 Drive：{file_title}")

                existing_link    = _get_cell(ws_list, row, 5)
                existing_file_id = _extract_file_id(existing_link)
                drive = get_drive_service()

                if existing_file_id:
                    try:
                        _update_drive_file(drive, existing_file_id, pdf_bytes, file_title)
                        file_id = existing_file_id
                    except Exception:
                        file_id = _create_drive_file(drive, folder_id, pdf_bytes, file_title)
                else:
                    file_id = _create_drive_file(drive, folder_id, pdf_bytes, file_title)

                file_url = f"https://drive.google.com/file/d/{file_id}/view"

                # 5. 回寫 D/E 欄，清除 H 欄
                now_str = datetime.datetime.now().strftime(TS_FMT)
                ws_list.update(
                    f"D{row}:E{row}",
                    [[now_str, file_url]],
                    value_input_option="USER_ENTERED"
                )
                ws_list.update_cell(row, 8, "")
                _log(log, f"      ✅ {name} PDF 已存至 Drive")
                count += 1

            except Exception as e:
                import traceback
                # googleapiclient.errors.HttpError 需要特別處理
                if hasattr(e, 'reason'):
                    err_msg = f"HttpError {e.status_code}: {e.reason}"
                elif hasattr(e, 'content'):
                    err_msg = f"HttpError: {e.content[:200]}"
                else:
                    err_msg = str(e) or repr(e) or "未知錯誤"
                _log(log, f"      ❌ {name} 失敗：{err_msg}")
                _log(log, f"      traceback: {traceback.format_exc().splitlines()[-2]}")
                skipped += 1

            time.sleep(0.8)   # 避免 API 速率限制

        _log(log, f"✅ PDF產出完成：成功 {count} 份，失敗 {skipped} 份")
        return count > 0 or skipped == 0

    except Exception as e:
        _log(log, f"❌ PDF產出失敗：{e}")
        return False


# ──────────────────────────────────────────────────────────────
# Drive 資料夾
# ──────────────────────────────────────────────────────────────

def _get_or_create_pdf_folder(root_id: str, period: str) -> str:
    """取得或建立 {根目錄}/{期別}/{期別} 資料夾，回傳最內層資料夾 ID。"""
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


# ──────────────────────────────────────────────────────────────
# 工具
# ──────────────────────────────────────────────────────────────

def _log(log: List[str], msg: str) -> None:
    log.append(msg)
