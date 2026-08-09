"""Google Drive 操作共用模組（金流對帳流程固定使用 Jenny OAuth）。"""

import io
import zipfile

import streamlit as st
from googleapiclient.http import MediaIoBaseUpload

from modules.auth import get_jenny_drive_service
from modules.period_utils import PERIOD_FILE_LABELS, get_file_name


GOOGLE_SHEET_MIME = "application/vnd.google-apps.spreadsheet"
FOLDER_MIME = "application/vnd.google-apps.folder"

DRIVE_PARAMS = {
    "includeItemsFromAllDrives": True,
    "supportsAllDrives": True,
}


def _http_error_detail(e) -> str:
    status = getattr(getattr(e, "resp", None), "status", "unknown")
    content = getattr(e, "content", b"")
    if isinstance(content, bytes):
        content = content.decode("utf-8", errors="replace")
    return f"HTTP {status}: {content}"


def _drive():
    """只建立 Jenny OAuth Drive service，絕不退回 Service Account。"""
    return get_jenny_drive_service()


def get_folder_by_name(drive, parent_id: str, name: str) -> dict | None:
    q = (
        f"name='{name}' and '{parent_id}' in parents and "
        f"mimeType='{FOLDER_MIME}' and trashed=false"
    )
    res = drive.files().list(q=q, fields="files(id, name)", **DRIVE_PARAMS).execute()
    files = res.get("files", [])
    return files[0] if files else None


def get_or_create_folder(drive, parent_id: str, name: str) -> str:
    folder = get_folder_by_name(drive, parent_id, name)
    if folder:
        return folder["id"]
    try:
        created = drive.files().create(
            body={"name": name, "mimeType": FOLDER_MIME, "parents": [parent_id]},
            fields="id",
            supportsAllDrives=True,
        ).execute()
        return created["id"]
    except Exception as e:
        raise Exception(f"建立資料夾失敗：{_http_error_detail(e)}") from e


def find_file_in_folder(drive, folder_id: str, file_name: str) -> dict | None:
    q = f"name='{file_name}' and '{folder_id}' in parents and trashed=false"
    res = drive.files().list(
        q=q, fields="files(id, name, mimeType)", **DRIVE_PARAMS
    ).execute()
    files = res.get("files", [])
    return files[0] if files else None


def find_file_by_keyword(
    drive, folder_id: str, keyword: str, mime_type: str = None
) -> dict | None:
    q = f"'{folder_id}' in parents and trashed=false"
    if mime_type:
        q += f" and mimeType='{mime_type}'"
    res = drive.files().list(
        q=q, fields="files(id, name, mimeType)", **DRIVE_PARAMS
    ).execute()
    return next((f for f in res.get("files", []) if keyword in f["name"]), None)


def list_folder_names(drive, parent_id: str) -> list[str]:
    q = f"'{parent_id}' in parents and mimeType='{FOLDER_MIME}' and trashed=false"
    res = drive.files().list(q=q, fields="files(id, name)", **DRIVE_PARAMS).execute()
    return [f["name"] for f in res.get("files", [])]


def trash_files_by_name(drive, folder_id: str, name: str):
    q = f"name='{name}' and '{folder_id}' in parents and trashed=false"
    res = drive.files().list(q=q, fields="files(id)", **DRIVE_PARAMS).execute()
    for item in res.get("files", []):
        drive.files().update(
            fileId=item["id"], body={"trashed": True}, supportsAllDrives=True
        ).execute()


def copy_file_to_folder(
    drive, source_file_id: str, dest_folder_id: str, new_name: str
) -> str:
    """以 Jenny OAuth 複製檔案；不再做 Service Account 擁有權轉移。"""
    trash_files_by_name(drive, dest_folder_id, new_name)
    try:
        copied = drive.files().copy(
            fileId=source_file_id,
            body={"name": new_name, "parents": [dest_folder_id]},
            fields="id",
            supportsAllDrives=True,
        ).execute()
        return copied["id"]
    except Exception as e:
        raise Exception(f"複製失敗：{_http_error_detail(e)}") from e


def convert_to_google_sheet(
    drive, folder_id: str, source_file_id: str, new_name: str
) -> str:
    q = (
        f"name='{new_name}' and '{folder_id}' in parents and "
        f"mimeType='{GOOGLE_SHEET_MIME}' and trashed=false"
    )
    existing = drive.files().list(q=q, fields="files(id)", **DRIVE_PARAMS).execute()
    for item in existing.get("files", []):
        drive.files().update(
            fileId=item["id"], body={"trashed": True}, supportsAllDrives=True
        ).execute()

    try:
        content = drive.files().get_media(fileId=source_file_id).execute()
        file_meta = drive.files().get(
            fileId=source_file_id,
            fields="mimeType",
            supportsAllDrives=True,
        ).execute()
    except Exception as e:
        raise Exception(f"下載原始檔案失敗：{_http_error_detail(e)}") from e

    src_mime = file_meta.get("mimeType", "application/octet-stream")
    media = MediaIoBaseUpload(io.BytesIO(content), mimetype=src_mime, resumable=False)
    try:
        converted = drive.files().create(
            body={
                "name": new_name,
                "mimeType": GOOGLE_SHEET_MIME,
                "parents": [folder_id],
            },
            media_body=media,
            fields="id",
            supportsAllDrives=True,
        ).execute()
        return converted["id"]
    except Exception as e:
        raise Exception(f"上傳轉換失敗：{_http_error_detail(e)}") from e


def create_period_folder_and_files(
    root_folder_id: str, period: str, region_name: str, log_fn=None
) -> dict:
    from modules.period_utils import get_previous_period

    def log(message):
        log_fn(message) if log_fn else st.write(message)

    drive = _drive()
    previous_period = get_previous_period(period)
    results = {}

    log(f"🔐 Jenny OAuth：建立期別 {period}")
    existing = get_folder_by_name(drive, root_folder_id, period)
    if existing:
        period_folder_id = existing["id"]
        log(f"📁 {period} 已存在，繼續執行")
    else:
        period_folder_id = get_or_create_folder(drive, root_folder_id, period)
        log(f"✅ 期別資料夾已建立：{period}")
    results["period_folder_id"] = period_folder_id

    log(f"🔍 尋找上一期資料夾：{previous_period}")
    prev_folder = get_folder_by_name(drive, root_folder_id, previous_period)
    if not prev_folder:
        found = list_folder_names(drive, root_folder_id)
        raise Exception(
            f"找不到上一期資料夾：{previous_period}，根目錄下找到：{found}"
        )

    for label in PERIOD_FILE_LABELS:
        old_name = get_file_name(previous_period, label, region_name)
        new_name = get_file_name(period, label, region_name)
        existing_file = find_file_in_folder(drive, period_folder_id, new_name)
        if existing_file:
            log(f"📄 {label} 已存在：{new_name}")
            results[label] = existing_file["id"]
            continue

        log(f"🔍 尋找：{old_name}")
        src = find_file_in_folder(drive, prev_folder["id"], old_name)
        if not src:
            log(f"⚠️ 找不到：{old_name}")
            results[label] = None
            continue

        try:
            results[label] = copy_file_to_folder(
                drive, src["id"], period_folder_id, new_name
            )
            log(f"✅ 完成：{new_name}")
        except Exception as e:
            log(f"⚠️ {e}")
            results[label] = None
    return results


def convert_period_order_file(
    root_folder_id: str, period: str, region_name: str, log_fn=None
) -> str:
    def log(message):
        log_fn(message) if log_fn else st.write(message)

    drive = _drive()
    log("🔐 使用 Jenny OAuth")
    period_folder = get_folder_by_name(drive, root_folder_id, period)
    if not period_folder:
        raise Exception(f"找不到期別資料夾：{period}，請先建立期別資料夾")

    folder_id = period_folder["id"]
    src = None
    found_name = None
    for ext in (".xlsx", ".xls"):
        candidate = f"{period}訂單-{region_name}{ext}"
        log(f"🔍 尋找訂單檔案：{candidate}")
        src = find_file_in_folder(drive, folder_id, candidate)
        if src:
            found_name = candidate
            break
    if not src:
        raise Exception(
            f"找不到訂單檔案：{period}訂單-{region_name}.xlsx 或 .xls"
        )

    log(f"🔄 轉檔中：{found_name}")
    sheet_name = f"{period}訂單-{region_name}"
    new_id = convert_to_google_sheet(drive, folder_id, src["id"], sheet_name)
    log(f"✅ 轉檔完成：{sheet_name}")
    return new_id


PAYMENT_FILE_CONFIGS = [
    ("已退款全部加收", "xlsx", False),
    ("已退款全部退款", "xlsx", False),
    ("預收", "xlsx", False),
    ("發票", "zip", True),
    ("藍新收款", "csv", False),
    ("藍新退款", "csv", False),
]


def convert_payment_files(
    root_folder_id: str, period: str, region_name: str, log_fn=None
) -> dict:
    def log(message):
        log_fn(message) if log_fn else st.write(message)

    drive = _drive()
    log("🔐 使用 Jenny OAuth")
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
            results[keyword] = _unzip_and_convert(
                drive, folder_id, src["id"], period, keyword, region_name, log
            )
        else:
            sheet_name = file_name.rsplit(".", 1)[0]
            log(f"🔄 轉檔：{file_name}")
            results[keyword] = convert_to_google_sheet(
                drive, folder_id, src["id"], sheet_name
            )
            log(f"✅ 完成：{sheet_name}")
    return results


def _unzip_and_convert(
    drive,
    folder_id: str,
    zip_file_id: str,
    period: str,
    keyword: str,
    region_name: str,
    log_fn,
) -> list:
    zip_bytes = io.BytesIO(drive.files().get_media(fileId=zip_file_id).execute())
    uploaded_ids = []
    with zipfile.ZipFile(zip_bytes) as zf:
        file_names = [name for name in zf.namelist() if not name.endswith("/")]
        for index, inner_name in enumerate(file_names):
            ext = "." + inner_name.rsplit(".", 1)[-1] if "." in inner_name else ""
            out_base = (
                f"{period}{keyword}-{region_name}"
                if len(file_names) == 1
                else f"{period}{keyword}-{region_name}-{index + 1}"
            )
            out_name = out_base + ext
            trash_files_by_name(drive, folder_id, out_name)
            media = MediaIoBaseUpload(
                io.BytesIO(zf.read(inner_name)),
                mimetype="application/octet-stream",
                resumable=False,
            )
            uploaded = drive.files().create(
                body={"name": out_name, "parents": [folder_id]},
                media_body=media,
                fields="id",
                supportsAllDrives=True,
            ).execute()
            uploaded_ids.append(
                convert_to_google_sheet(drive, folder_id, uploaded["id"], out_base)
            )
            log_fn(f"✅ 解壓縮並轉檔：{out_base}")
    return uploaded_ids
