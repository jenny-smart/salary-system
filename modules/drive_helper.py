"""
Google Drive 操作共用模組
"""

import io
import zipfile
import streamlit as st
from googleapiclient.http import MediaIoBaseUpload
from modules.auth import get_drive_service, get_credentials
from modules.period_utils import get_file_name, PERIOD_FILE_LABELS

GOOGLE_SHEET_MIME = "application/vnd.google-apps.spreadsheet"
FOLDER_MIME = "application/vnd.google-apps.folder"

DRIVE_PARAMS = {
    "includeItemsFromAllDrives": True,
    "supportsAllDrives": True,
}

# 複製後把擁有者轉移給這個帳號
OWNER_EMAIL = "jenny@lemonclean.com.tw"


def _http_error_detail(e) -> str:
    """從 HttpError 取得詳細訊息"""
    status = getattr(e, 'resp', {}).get('status', 'unknown')
    content = getattr(e, 'content', b'')
    if isinstance(content, bytes):
        content = content.decode('utf-8')
    return f"HTTP {status}: {content}"


# ═══════════════════════════════════════
# 資料夾操作
# ═══════════════════════════════════════

def get_folder_by_name(drive, parent_id: str, name: str) -> dict | None:
    q = (
        f"name='{name}' and "
        f"'{parent_id}' in parents and "
        f"mimeType='{FOLDER_MIME}' and "
        f"trashed=false"
    )
    res = drive.files().list(q=q, fields="files(id, name)", **DRIVE_PARAMS).execute()
    files = res.get("files", [])
    return files[0] if files else None


def get_or_create_folder(drive, parent_id: str, name: str) -> str:
    folder = get_folder_by_name(drive, parent_id, name)
    if folder:
        return folder["id"]
    meta = {"name": name, "mimeType": FOLDER_MIME, "parents": [parent_id]}
    try:
        created = drive.files().create(
            body=meta, fields="id", supportsAllDrives=True
        ).execute()
        return created["id"]
    except Exception as e:
        raise Exception(f"建立資料夾失敗：{_http_error_detail(e)}")


# ═══════════════════════════════════════
# 檔案查找
# ═══════════════════════════════════════

def find_file_in_folder(drive, folder_id: str, file_name: str) -> dict | None:
    q = (
        f"name='{file_name}' and "
        f"'{folder_id}' in parents and "
        f"trashed=false"
    )
    res = drive.files().list(
        q=q, fields="files(id, name, mimeType)", **DRIVE_PARAMS
    ).execute()
    files = res.get("files", [])
    return files[0] if files else None


def find_file_by_keyword(drive, folder_id: str, keyword: str, mime_type: str = None) -> dict | None:
    q = f"'{folder_id}' in parents and trashed=false"
    if mime_type:
        q += f" and mimeType='{mime_type}'"
    res = drive.files().list(
        q=q, fields="files(id, name, mimeType)", **DRIVE_PARAMS
    ).execute()
    for f in res.get("files", []):
        if keyword in f["name"]:
            return f
    return None


def list_folder_names(drive, parent_id: str) -> list[str]:
    q = f"'{parent_id}' in parents and mimeType='{FOLDER_MIME}' and trashed=false"
    res = drive.files().list(q=q, fields="files(id, name)", **DRIVE_PARAMS).execute()
    return [f["name"] for f in res.get("files", [])]


# ═══════════════════════════════════════
# 刪除同名檔案
# ═══════════════════════════════════════

def trash_files_by_name(drive, folder_id: str, name: str):
    q = f"name='{name}' and '{folder_id}' in parents and trashed=false"
    res = drive.files().list(q=q, fields="files(id)", **DRIVE_PARAMS).execute()
    for f in res.get("files", []):
        drive.files().update(
            fileId=f["id"], body={"trashed": True}, supportsAllDrives=True
        ).execute()


# ═══════════════════════════════════════
# 複製檔案（複製後轉移擁有者，解決空間問題）
# ═══════════════════════════════════════

def copy_file_to_folder(drive, source_file_id: str, dest_folder_id: str, new_name: str) -> str:
    """
    複製 Google Sheet 到目標資料夾
    複製後立刻轉移擁有者給 OWNER_EMAIL
    這樣空間算在 OWNER_EMAIL 的帳號，不是 Service Account
    """
    trash_files_by_name(drive, dest_folder_id, new_name)

    # 複製檔案
    try:
        copied = drive.files().copy(
            fileId=source_file_id,
            body={"name": new_name, "parents": [dest_folder_id]},
            supportsAllDrives=True
        ).execute()
    except Exception as e:
        raise Exception(f"複製失敗：{_http_error_detail(e)}")

    new_file_id = copied["id"]

    # 立刻轉移擁有者（必須成功，否則 Service Account 空間會被佔滿）
    try:
        drive.permissions().create(
            fileId=new_file_id,
            body={
                "type": "user",
                "role": "owner",
                "emailAddress": OWNER_EMAIL,
            },
            transferOwnership=True,
            supportsAllDrives=True,
            sendNotificationEmail=False,
        ).execute()
    except Exception as e:
        # 轉移失敗：刪掉複製的檔案避免佔用空間，然後報錯
        try:
            drive.files().delete(
                fileId=new_file_id, supportsAllDrives=True
            ).execute()
        except Exception:
            pass
        raise Exception(f"複製成功但轉移擁有者失敗，已刪除複製檔案：{_http_error_detail(e)}")

    return new_file_id


# ═══════════════════════════════════════
# 轉換為 Google Sheet（蓋舊檔）
# ═══════════════════════════════════════

def convert_to_google_sheet(drive, folder_id: str, source_file_id: str, new_name: str) -> str:
    # 刪除同名舊 Google Sheet
    q = (
        f"name='{new_name}' and "
        f"'{folder_id}' in parents and "
        f"mimeType='{GOOGLE_SHEET_MIME}' and "
        f"trashed=false"
    )
    existing = drive.files().list(q=q, fields="files(id)", **DRIVE_PARAMS).execute()
    for f in existing.get("files", []):
        drive.files().update(
            fileId=f["id"], body={"trashed": True}, supportsAllDrives=True
        ).execute()

    try:
        content = drive.files().get_media(fileId=source_file_id).execute()
    except Exception as e:
        raise Exception(f"下載原始檔案失敗：{_http_error_detail(e)}")

    file_meta = drive.files().get(
        fileId=source_file_id, fields="mimeType", supportsAllDrives=True
    ).execute()
    src_mime = file_meta.get("mimeType", "application/octet-stream")

    media = MediaIoBaseUpload(io.BytesIO(content), mimetype=src_mime)
    try:
        converted = drive.files().create(
            body={
                "name": new_name,
                "mimeType": GOOGLE_SHEET_MIME,
                "parents": [folder_id],
            },
            media_body=media,
            fields="id",
            supportsAllDrives=True
        ).execute()
    except Exception as e:
        raise Exception(f"上傳轉換失敗：{_http_error_detail(e)}")

    return converted["id"]


# ═══════════════════════════════════════
# ① 建立期別資料夾與檔案
# ═══════════════════════════════════════

def create_period_folder_and_files(
    root_folder_id: str, period: str, region_name: str, log_fn=None
) -> dict:
    from modules.period_utils import get_previous_period

    def log(msg):
        if log_fn:
            log_fn(msg)
        else:
            st.write(msg)

    drive = get_drive_service()
    previous_period = get_previous_period(period)
    results = {}

    # 建立或確認期別資料夾
    log(f"🔍 建立期別資料夾：{period}")
    existing = get_folder_by_name(drive, root_folder_id, period)
    if existing:
        period_folder_id = existing["id"]
        log(f"📁 {period} 已存在，繼續執行")
    else:
        period_folder_id = get_or_create_folder(drive, root_folder_id, period)
        log(f"✅ 期別資料夾已建立：{period}")

    results["period_folder_id"] = period_folder_id

    # 找上一期資料夾
    log(f"🔍 尋找上一期資料夾：{previous_period}")
    prev_folder = get_folder_by_name(drive, root_folder_id, previous_period)
    if not prev_folder:
        found = list_folder_names(drive, root_folder_id)
        raise Exception(f"找不到上一期資料夾：{previous_period}，根目錄下找到：{found}")

    prev_folder_id = prev_folder["id"]
    log(f"✅ 找到上一期：{previous_period}")

    # 複製四類檔案
    for label in PERIOD_FILE_LABELS:
        old_name = get_file_name(previous_period, label, region_name)
        new_name = get_file_name(period, label, region_name)

        # 已存在就跳過
        existing_file = find_file_in_folder(drive, period_folder_id, new_name)
        if existing_file:
            log(f"📄 {label} 已存在：{new_name}")
            results[label] = existing_file["id"]
            continue

        log(f"🔍 尋找：{old_name}")
        src = find_file_in_folder(drive, prev_folder_id, old_name)
        if not src:
            log(f"⚠️ 找不到：{old_name}")
            results[label] = None
            continue

        log(f"📋 複製：{old_name} → {new_name}")
        try:
            new_id = copy_file_to_folder(drive, src["id"], period_folder_id, new_name)
            results[label] = new_id
            log(f"✅ 完成：{new_name}")
        except Exception as e:
            log(f"⚠️ {e}")
            results[label] = None

    return results


# ═══════════════════════════════════════
# ② 期別訂單轉檔
# ═══════════════════════════════════════

def convert_period_order_file(
    root_folder_id: str, period: str, region_name: str, log_fn=None
) -> str:
    def log(msg):
        if log_fn:
            log_fn(msg)
        else:
            st.write(msg)

    drive = get_drive_service()

    log(f"🔍 尋找期別資料夾：{period}")
    period_folder = get_folder_by_name(drive, root_folder_id, period)
    if not period_folder:
        raise Exception(f"找不到期別資料夾：{period}，請先執行「建立期別資料夾」")

    folder_id = period_folder["id"]
    log(f"✅ 找到期別資料夾：{period}")

    # 支援 .xlsx 和 .xls 兩種格式
    src = None
    found_name = None
    for ext in [".xlsx", ".xls"]:
        candidate = f"{period}訂單-{region_name}{ext}"
        log(f"🔍 尋找訂單檔案：{candidate}")
        src = find_file_in_folder(drive, folder_id, candidate)
        if src:
            found_name = candidate
            break

    if not src:
        raise Exception(f"找不到訂單檔案：{period}訂單-{region_name}.xlsx 或 .xls，請確認檔案已上傳到 {period} 資料夾")

    log(f"🔄 轉檔中：{found_name}")
    sheet_name = f"{period}訂單-{region_name}"
    new_id = convert_to_google_sheet(drive, folder_id, src["id"], sheet_name)
    log(f"✅ 轉檔完成：{sheet_name}")
    return new_id


# ═══════════════════════════════════════
# ② 金流對帳轉檔（下半月）
# ═══════════════════════════════════════

PAYMENT_FILE_CONFIGS = [
    ("已退款全部加收", "xlsx", False),
    ("已退款全部退款", "xlsx", False),
    ("預收",           "xlsx", False),
    ("發票",           "zip",  True),
    ("藍新收款",       "csv",  False),
    ("藍新退款",       "csv",  False),
]


def convert_payment_files(
    root_folder_id: str, period: str, region_name: str, log_fn=None
) -> dict:
    def log(msg):
        if log_fn:
            log_fn(msg)
        else:
            st.write(msg)

    drive = get_drive_service()

    log(f"🔍 尋找期別資料夾：{period}")
    period_folder = get_folder_by_name(drive, root_folder_id, period)
    if not period_folder:
        raise Exception(f"找不到期別資料夾：{period}")

    folder_id = period_folder["id"]
    results = {}

    for keyword, ext, is_zip in PAYMENT_FILE_CONFIGS:
        file_name = f"{period}{keyword}-{region_name}.{ext}"
        log(f"🔍 尋找：{file_name}")
        src = find_file_in_folder(drive, folder_id, file_name)

        if not src:
            log(f"⚠️ 找不到：{file_name}")
            results[keyword] = None
            continue

        if is_zip:
            log(f"📦 解壓縮：{file_name}")
            ids = _unzip_and_convert(drive, folder_id, src["id"], period, keyword, region_name, log)
            results[keyword] = ids
        else:
            sheet_name = file_name.rsplit(".", 1)[0]
            log(f"🔄 轉檔：{file_name}")
            new_id = convert_to_google_sheet(drive, folder_id, src["id"], sheet_name)
            results[keyword] = new_id
            log(f"✅ 完成：{sheet_name}")

    return results


def _unzip_and_convert(
    drive, folder_id: str, zip_file_id: str,
    period: str, keyword: str, region_name: str, log_fn
) -> list:
    request = drive.files().get_media(fileId=zip_file_id)
    zip_bytes = io.BytesIO(request.execute())
    uploaded_ids = []

    with zipfile.ZipFile(zip_bytes) as zf:
        names = zf.namelist()
        for i, inner_name in enumerate(names):
            ext = "." + inner_name.rsplit(".", 1)[-1] if "." in inner_name else ""
            if len(names) == 1:
                out_base = f"{period}{keyword}-{region_name}"
            else:
                out_base = f"{period}{keyword}-{region_name}-{i + 1}"

            out_name_with_ext = out_base + ext
            content = zf.read(inner_name)
            trash_files_by_name(drive, folder_id, out_name_with_ext)
            media = MediaIoBaseUpload(io.BytesIO(content), mimetype="application/octet-stream")
            uploaded = drive.files().create(
                body={"name": out_name_with_ext, "parents": [folder_id]},
                media_body=media,
                fields="id",
                supportsAllDrives=True
            ).execute()

            new_id = convert_to_google_sheet(drive, folder_id, uploaded["id"], out_base)
            uploaded_ids.append(new_id)
            log_fn(f"✅ 解壓縮並轉檔：{out_base}")

    return uploaded_ids
