"""
Google Drive 操作共用模組
包含：
- 資料夾查找 / 建立
- 檔案查找（依命名規則）
- 複製檔案（蓋舊檔）
- ZIP 解壓縮 → 存回同資料夾
- Excel/CSV 轉 Google Sheet（蓋舊檔）
"""

import io
import zipfile
import streamlit as st
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseUpload
from modules.auth import get_credentials, get_drive_service
from modules.period_utils import get_file_name, PERIOD_FILE_LABELS

GOOGLE_SHEET_MIME = "application/vnd.google-apps.spreadsheet"
FOLDER_MIME = "application/vnd.google-apps.folder"


# ═══════════════════════════════════════
# 資料夾操作
# ═══════════════════════════════════════

def get_folder_by_name(drive, parent_id: str, name: str) -> dict | None:
    """在父資料夾下找指定名稱的資料夾，找不到回傳 None"""
    q = (
        f"name='{name}' and "
        f"'{parent_id}' in parents and "
        f"mimeType='{FOLDER_MIME}' and "
        f"trashed=false"
    )
    res = drive.files().list(q=q, fields="files(id, name)").execute()
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
# 檔案查找（依命名規則）
# ═══════════════════════════════════════

def find_file_in_folder(drive, folder_id: str, file_name: str) -> dict | None:
    """
    在指定資料夾中找檔案（不限類型）
    回傳 {id, name, mimeType} 或 None
    """
    q = (
        f"name='{file_name}' and "
        f"'{folder_id}' in parents and "
        f"trashed=false"
    )
    res = drive.files().list(q=q, fields="files(id, name, mimeType)").execute()
    files = res.get("files", [])
    return files[0] if files else None


def find_period_file(drive, root_folder_id: str, period: str, label: str, region_name: str) -> dict | None:
    """
    依命名規則找期別檔案
    例如：根目錄/202504-1/202504-1金流對帳-台北
    回傳 {id, name, mimeType} 或 None
    """
    # 找期別資料夾
    period_folder = get_folder_by_name(drive, root_folder_id, period)
    if not period_folder:
        return None

    file_name = get_file_name(period, label, region_name)
    return find_file_in_folder(drive, period_folder["id"], file_name)


def get_all_period_files(drive, root_folder_id: str, period: str, region_name: str) -> dict:
    """
    取得某期別所有類型的檔案 ID
    回傳：{"金流對帳": "fileId", "清潔承攬": "fileId", ...}
    找不到的項目值為 None
    """
    result = {}
    period_folder = get_folder_by_name(drive, root_folder_id, period)

    if not period_folder:
        for label in PERIOD_FILE_LABELS:
            result[label] = None
        return result

    folder_id = period_folder["id"]

    for label in PERIOD_FILE_LABELS:
        file_name = get_file_name(period, label, region_name)
        file = find_file_in_folder(drive, folder_id, file_name)
        result[label] = file["id"] if file else None

    return result


# ═══════════════════════════════════════
# 複製檔案（建立期別用）
# ═══════════════════════════════════════

def trash_files_by_name(drive, folder_id: str, name: str):
    """刪除資料夾中所有同名檔案"""
    q = f"name='{name}' and '{folder_id}' in parents and trashed=false"
    res = drive.files().list(q=q, fields="files(id)").execute()
    for f in res.get("files", []):
        drive.files().update(fileId=f["id"], body={"trashed": True}).execute()


def copy_file_to_folder(drive, source_file_id: str, dest_folder_id: str, new_name: str) -> str:
    """複製檔案到目標資料夾，蓋掉同名舊檔，回傳新檔 ID"""
    trash_files_by_name(drive, dest_folder_id, new_name)

    copied = drive.files().copy(
        fileId=source_file_id,
        body={"name": new_name, "parents": [dest_folder_id]}
    ).execute()
    return copied["id"]


def create_period_folder_and_files(
    root_folder_id: str,
    period: str,
    region_name: str
) -> dict:
    """
    建立期別資料夾並複製上一期的四類檔案
    回傳：{"period_folder_id": "...", "金流對帳": "...", ...}
    """
    from modules.period_utils import get_previous_period, get_file_name

    drive = get_drive_service()
    previous_period = get_previous_period(period)
    results = {}

    # 建立期別資料夾
    period_folder_id = get_or_create_folder(drive, root_folder_id, period)
    results["period_folder_id"] = period_folder_id
    st.write(f"📁 期別資料夾：{period}")

    # 找上一期資料夾
    prev_folder = get_folder_by_name(drive, root_folder_id, previous_period)
    if not prev_folder:
        raise Exception(f"找不到上一期資料夾：{previous_period}")

    prev_folder_id = prev_folder["id"]

    # 複製四類檔案
    for label in PERIOD_FILE_LABELS:
        old_name = get_file_name(previous_period, label, region_name)
        new_name = get_file_name(period, label, region_name)

        # 找上一期檔案
        src = find_file_in_folder(drive, prev_folder_id, old_name)
        if not src:
            st.warning(f"⚠️ 找不到上期 {label}：{old_name}")
            results[label] = None
            continue

        new_id = copy_file_to_folder(drive, src["id"], period_folder_id, new_name)
        results[label] = new_id
        st.write(f"✅ {label}：{new_name}")

    return results


# ═══════════════════════════════════════
# ZIP 解壓縮 → 存回同資料夾
# ═══════════════════════════════════════

def unzip_and_upload(drive, folder_id: str, zip_file_id: str, base_name: str) -> list[str]:
    """
    下載 ZIP 檔案，解壓後上傳到同一資料夾
    檔名規則：base_name（去掉.zip）+ 解壓縮後的副檔名
    蓋掉同名舊檔
    回傳：上傳成功的檔案 ID 清單
    """
    # 下載 ZIP
    request = drive.files().get_media(fileId=zip_file_id)
    zip_bytes = io.BytesIO(request.execute())

    uploaded_ids = []

    with zipfile.ZipFile(zip_bytes) as zf:
        names = zf.namelist()
        for i, inner_name in enumerate(names):
            # 決定輸出檔名
            ext = "." + inner_name.rsplit(".", 1)[-1] if "." in inner_name else ""
            if len(names) == 1:
                out_name = base_name + ext
            else:
                out_name = f"{base_name}-{i + 1}{ext}"

            # 刪除同名舊檔
            trash_files_by_name(drive, folder_id, out_name)

            # 上傳
            content = zf.read(inner_name)
            media = MediaIoBaseUpload(io.BytesIO(content), mimetype="application/octet-stream")
            uploaded = drive.files().create(
                body={"name": out_name, "parents": [folder_id]},
                media_body=media,
                fields="id"
            ).execute()
            uploaded_ids.append(uploaded["id"])
            st.write(f"📦 解壓縮：{out_name}")

    return uploaded_ids


# ═══════════════════════════════════════
# Excel / CSV 轉 Google Sheet（蓋舊檔）
# ═══════════════════════════════════════

CONVERT_MIME_MAP = {
    ".xls": "application/vnd.ms-excel",
    ".xlsx": "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
    ".csv": "text/csv",
}


def convert_to_google_sheet(drive, folder_id: str, source_file_id: str, new_name: str) -> str:
    """
    將 Excel/CSV 檔案轉換為 Google Sheet
    蓋掉同名舊 Google Sheet
    回傳新 Google Sheet 的 ID
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

    # 下載原始檔案內容
    content = drive.files().get_media(fileId=source_file_id).execute()

    # 取得原始 mimeType
    file_meta = drive.files().get(fileId=source_file_id, fields="name, mimeType").execute()
    src_mime = file_meta.get("mimeType", "application/octet-stream")

    # 上傳並轉換為 Google Sheet
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

    st.write(f"🔄 轉換完成：{new_name}")
    return converted["id"]


def convert_period_payment_files(
    folder_id: str,
    period: str,
    region_name: str,
) -> dict:
    """
    轉換期別資料夾內所有金流相關檔案
    包含：發票、已退款全部加收、已退款全部退款、預收、藍新收款、藍新退款
    ZIP 先解壓縮，再轉成 Google Sheet
    回傳：{檔名: 新 Google Sheet ID}
    """
    drive = get_drive_service()
    keywords = ["發票", "已退款全部加收", "已退款全部退款", "預收", "藍新收款", "藍新退款"]
    results = {}

    # 列出資料夾內所有檔案
    q = f"'{folder_id}' in parents and trashed=false"
    res = drive.files().list(q=q, fields="files(id, name, mimeType)").execute()
    all_files = res.get("files", [])

    for file in all_files:
        name = file["name"]
        file_id = file["id"]

        matched_keyword = next((kw for kw in keywords if kw in name), None)
        if not matched_keyword:
            continue

        # ZIP：先解壓縮
        if name.lower().endswith(".zip"):
            base_name = name[:-4]  # 去掉 .zip
            st.write(f"📦 解壓縮：{name}")
            uploaded_ids = unzip_and_upload(drive, folder_id, file_id, base_name)

            # 解壓縮後的檔案再轉 Google Sheet
            for uid in uploaded_ids:
                meta = drive.files().get(fileId=uid, fields="name").execute()
                inner_name = meta["name"]
                sheet_name = inner_name.rsplit(".", 1)[0]  # 去副檔名
                new_id = convert_to_google_sheet(drive, folder_id, uid, sheet_name)
                results[sheet_name] = new_id

        # Excel / CSV：直接轉
        elif any(name.lower().endswith(ext) for ext in [".xls", ".xlsx", ".csv"]):
            sheet_name = name.rsplit(".", 1)[0]
            new_id = convert_to_google_sheet(drive, folder_id, file_id, sheet_name)
            results[sheet_name] = new_id

    return results
