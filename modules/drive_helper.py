"""
Google Drive 操作共用模組
"""

import io
import zipfile
import streamlit as st
from googleapiclient.http import MediaIoBaseUpload
from modules.auth import get_drive_service
from modules.period_utils import get_file_name, PERIOD_FILE_LABELS

GOOGLE_SHEET_MIME = "application/vnd.google-apps.spreadsheet"
FOLDER_MIME = "application/vnd.google-apps.folder"


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
    res = drive.files().list(
        q=q,
        fields="files(id, name)",
        includeItemsFromAllDrives=True,
        supportsAllDrives=True
    ).execute()
    files = res.get("files", [])
    return files[0] if files else None

def get_or_create_folder(drive, parent_id: str, name: str) -> str:
    """取得或建立子資料夾，回傳資料夾 ID"""
    folder = get_folder_by_name(drive, parent_id, name)
    if folder:
        return folder["id"]
    meta = {
        "name": name,
        "mimeType": FOLDER_MIME,
        "parents": [parent_id],
    }
    created = drive.files().create(body=meta, fields="id").execute()
    return created["id"]


# ═══════════════════════════════════════
# 檔案查找
# ═══════════════════════════════════════

def find_file_in_folder(drive, folder_id: str, file_name: str) -> dict | None:
    """在指定資料夾中找檔案，回傳 {id, name, mimeType} 或 None"""
    q = (
        f"name='{file_name}' and "
        f"'{folder_id}' in parents and "
        f"trashed=false"
    )
    res = drive.files().list(q=q, fields="files(id, name, mimeType)").execute()
    files = res.get("files", [])
    return files[0] if files else None


def find_file_by_keyword(drive, folder_id: str, keyword: str, mime_type: str = None) -> dict | None:
    """在資料夾中找包含關鍵字的檔案"""
    q = f"'{folder_id}' in parents and trashed=false"
    if mime_type:
        q += f" and mimeType='{mime_type}'"
    res = drive.files().list(q=q, fields="files(id, name, mimeType)").execute()
    for f in res.get("files", []):
        if keyword in f["name"]:
            return f
    return None


# ═══════════════════════════════════════
# 刪除同名檔案
# ═══════════════════════════════════════

def trash_files_by_name(drive, folder_id: str, name: str):
    """刪除資料夾中所有同名檔案"""
    q = f"name='{name}' and '{folder_id}' in parents and trashed=false"
    res = drive.files().list(q=q, fields="files(id)").execute()
    for f in res.get("files", []):
        drive.files().update(fileId=f["id"], body={"trashed": True}).execute()


# ═══════════════════════════════════════
# 複製檔案
# ═══════════════════════════════════════

def copy_file_to_folder(drive, source_file_id: str, dest_folder_id: str, new_name: str) -> str:
    trash_files_by_name(drive, dest_folder_id, new_name)
    try:
        copied = drive.files().copy(
            fileId=source_file_id,
            body={"name": new_name, "parents": [dest_folder_id]}
        ).execute()
        return copied["id"]
    except Exception as e:
        raise Exception(f"複製失敗 [{new_name}] 來源ID:{source_file_id} 錯誤:{e}")


# ═══════════════════════════════════════
# 轉換為 Google Sheet（蓋舊檔）
# ═══════════════════════════════════════

def convert_to_google_sheet(drive, folder_id: str, source_file_id: str, new_name: str) -> str:
    """
    將 Excel/CSV 轉換為 Google Sheet
    存在同一資料夾，同名蓋舊檔
    回傳新 Google Sheet ID
    """
    # 刪除同名舊 Google Sheet
    q = (
        f"name='{new_name}' and "
        f"'{folder_id}' in parents and "
        f"mimeType='{GOOGLE_SHEET_MIME}' and "
        f"trashed=false"
    )
    existing = drive.files().list(q=q, fields="files(id)").execute()
    for f in existing.get("files", []):
        drive.files().update(fileId=f["id"], body={"trashed": True}).execute()

    # 下載原始內容
    content = drive.files().get_media(fileId=source_file_id).execute()

    # 取得 mimeType
    file_meta = drive.files().get(fileId=source_file_id, fields="mimeType").execute()
    src_mime = file_meta.get("mimeType", "application/octet-stream")

    # 上傳並轉換
    media = MediaIoBaseUpload(io.BytesIO(content), mimetype=src_mime)
    converted = drive.files().create(
        body={
            "name": new_name,
            "mimeType": GOOGLE_SHEET_MIME,
            "parents": [folder_id],
        },
        media_body=media,
        fields="id"
    ).execute()

    return converted["id"]


# ═══════════════════════════════════════
# ① 建立期別資料夾與檔案
# ═══════════════════════════════════════

def create_period_folder_and_files(
    root_folder_id: str,
    period: str,
    region_name: str,
    log_fn=None
) -> dict:
    """
    建立期別資料夾並複製上一期四類檔案
    log_fn：呼叫端的 log 函數（選用）
    """
    from modules.period_utils import get_previous_period

    def log(msg):
        if log_fn:
            log_fn(msg)
        else:
            st.write(msg)

    drive = get_drive_service()
    previous_period = get_previous_period(period)
    results = {}

    # 建立期別資料夾
    period_folder_id = get_or_create_folder(drive, root_folder_id, period)
    results["period_folder_id"] = period_folder_id
    log(f"📁 期別資料夾：{period}")

    # 找上一期資料夾
    prev_folder = get_folder_by_name(drive, root_folder_id, previous_period)
    if not prev_folder:
        raise Exception(f"找不到上一期資料夾：{previous_period}")

    prev_folder_id = prev_folder["id"]
    log(f"📋 複製來源：{previous_period}")

    # 複製四類檔案
    for label in PERIOD_FILE_LABELS:
        old_name = get_file_name(previous_period, label, region_name)
        new_name = get_file_name(period, label, region_name)

        src = find_file_in_folder(drive, prev_folder_id, old_name)
        if not src:
            log(f"⚠️ 找不到上期 {label}：{old_name}")
            results[label] = None
            continue

        new_id = copy_file_to_folder(drive, src["id"], period_folder_id, new_name)
        results[label] = new_id
        log(f"✅ {label}：{new_name}")

    return results


# ═══════════════════════════════════════
# ② 期別訂單轉檔（只轉 xlsx → Google Sheet）
# ═══════════════════════════════════════

def convert_period_order_file(
    root_folder_id: str,
    period: str,
    region_name: str,
    log_fn=None
) -> str:
    """
    在期別資料夾中找 {期別}訂單-{地區}.xlsx
    轉成 Google Sheet，存在同一資料夾，同名蓋舊檔
    回傳新 Google Sheet ID
    """
    def log(msg):
        if log_fn:
            log_fn(msg)
        else:
            st.write(msg)

    drive = get_drive_service()

    # 找期別資料夾
    period_folder = get_folder_by_name(drive, root_folder_id, period)
    if not period_folder:
        raise Exception(f"找不到期別資料夾：{period}")

    folder_id = period_folder["id"]

    # 找訂單 xlsx 檔案（檔名：{期別}訂單-{地區}.xlsx）
    xlsx_name = f"{period}訂單-{region_name}.xlsx"
    src = find_file_in_folder(drive, folder_id, xlsx_name)
    if not src:
        raise Exception(f"找不到訂單檔案：{xlsx_name}")

    log(f"🔄 轉檔：{xlsx_name}")

    # 轉換後的 Google Sheet 名稱（去掉 .xlsx）
    sheet_name = f"{period}訂單-{region_name}"
    new_id = convert_to_google_sheet(drive, folder_id, src["id"], sheet_name)

    log(f"✅ 轉檔完成：{sheet_name}")
    return new_id


# ═══════════════════════════════════════
# ② 金流對帳轉檔（下半月：已退款/預收/發票/藍新）
# ═══════════════════════════════════════

PAYMENT_FILE_CONFIGS = [
    # (關鍵字, 副檔名, 是否ZIP)
    ("已退款全部加收", "xlsx", False),
    ("已退款全部退款", "xlsx", False),
    ("預收",           "xlsx", False),
    ("發票",           "zip",  True),
    ("藍新收款",       "csv",  False),
    ("藍新退款",       "csv",  False),
]


def convert_payment_files(
    root_folder_id: str,
    period: str,
    region_name: str,
    log_fn=None
) -> dict:
    """
    轉換下半月金流相關檔案：
    - 已退款全部加收-地區.xlsx → Google Sheet
    - 已退款全部退款-地區.xlsx → Google Sheet
    - 預收-地區.xlsx → Google Sheet
    - 發票-地區.zip → 解壓縮 → Google Sheet
    - 藍新收款-地區.csv → Google Sheet
    - 藍新退款-地區.csv → Google Sheet
    存在同一資料夾，同名蓋舊檔
    """
    def log(msg):
        if log_fn:
            log_fn(msg)
        else:
            st.write(msg)

    drive = get_drive_service()

    period_folder = get_folder_by_name(drive, root_folder_id, period)
    if not period_folder:
        raise Exception(f"找不到期別資料夾：{period}")

    folder_id = period_folder["id"]
    results = {}

    for keyword, ext, is_zip in PAYMENT_FILE_CONFIGS:
        file_name = f"{period}{keyword}-{region_name}.{ext}"
        src = find_file_in_folder(drive, folder_id, file_name)

        if not src:
            log(f"⚠️ 找不到：{file_name}")
            results[keyword] = None
            continue

        if is_zip:
            # ZIP：解壓縮後轉 Google Sheet
            log(f"📦 解壓縮：{file_name}")
            ids = _unzip_and_convert(drive, folder_id, src["id"], period, keyword, region_name, log)
            results[keyword] = ids
        else:
            # 直接轉 Google Sheet
            sheet_name = file_name.rsplit(".", 1)[0]  # 去副檔名
            log(f"🔄 轉檔：{file_name}")
            new_id = convert_to_google_sheet(drive, folder_id, src["id"], sheet_name)
            results[keyword] = new_id
            log(f"✅ 完成：{sheet_name}")

    return results


def _unzip_and_convert(
    drive, folder_id: str, zip_file_id: str,
    period: str, keyword: str, region_name: str,
    log_fn
) -> list:
    """ZIP 解壓縮後轉 Google Sheet，回傳 ID 清單"""
    request = drive.files().get_media(fileId=zip_file_id)
    zip_bytes = io.BytesIO(request.execute())
    uploaded_ids = []

    with zipfile.ZipFile(zip_bytes) as zf:
        names = zf.namelist()
        for i, inner_name in enumerate(names):
            ext = "." + inner_name.rsplit(".", 1)[-1] if "." in inner_name else ""

            # 命名規則：單檔用原名，多檔加 -1, -2
            if len(names) == 1:
                out_base = f"{period}{keyword}-{region_name}"
            else:
                out_base = f"{period}{keyword}-{region_name}-{i + 1}"

            out_name_with_ext = out_base + ext

            # 上傳原始檔
            content = zf.read(inner_name)
            trash_files_by_name(drive, folder_id, out_name_with_ext)
            media = MediaIoBaseUpload(
                io.BytesIO(content), mimetype="application/octet-stream"
            )
            uploaded = drive.files().create(
                body={"name": out_name_with_ext, "parents": [folder_id]},
                media_body=media,
                fields="id"
            ).execute()

            # 轉成 Google Sheet
            new_id = convert_to_google_sheet(drive, folder_id, uploaded["id"], out_base)
            uploaded_ids.append(new_id)
            log_fn(f"✅ 解壓縮並轉檔：{out_base}")

    return uploaded_ids
