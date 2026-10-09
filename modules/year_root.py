"""跨年度期別資料夾定位。

地區根目錄（root_folder_id）位在「YYYY專員承攬服務費/0X.地區專員」。期別資料夾
（例：202612-2）放在該期別所屬年度的地區資料夾下，所以：
- 建立 202701-1 時，上一期 202612-2 要到「2026專員承攬服務費/0X.地區專員」找；
- 根目錄還沒切換到新年度時，202701-1 要建在「2027專員承攬服務費/0X.地區專員」。

root_for_period() 依期別年份，從目前的 root_folder_id 找到同名的年度資料夾，
與何時切換 root_folder_id 無關。
"""

from __future__ import annotations

import re
from typing import Dict, Tuple

FOLDER_MIME = "application/vnd.google-apps.folder"
_YEAR = re.compile(r"(?<!\d)(20\d{2})(?!\d)")
_CACHE: Dict[Tuple[str, str], str] = {}


def _get(drive, file_id: str) -> dict:
    return drive.files().get(
        fileId=file_id, fields="id,name,parents", supportsAllDrives=True
    ).execute()


def _child_folder(drive, parent_id: str, name: str) -> dict | None:
    escaped = name.replace("'", "\\'")
    res = drive.files().list(
        q=(f"name='{escaped}' and '{parent_id}' in parents and "
           f"mimeType='{FOLDER_MIME}' and trashed=false"),
        fields="files(id,name)", supportsAllDrives=True, includeItemsFromAllDrives=True,
    ).execute()
    files = res.get("files", [])
    return files[0] if files else None


def root_for_period(drive, root_folder_id: str, period: str) -> str:
    """回傳期別所屬年度的地區根目錄 ID；同年度或無法判斷時回傳原 root_folder_id。"""
    year = str(period or "")[:4]
    if not root_folder_id or not re.fullmatch(r"20\d{2}", year):
        return root_folder_id
    key = (root_folder_id, year)
    if key in _CACHE:
        return _CACHE[key]
    try:
        root = _get(drive, root_folder_id)
        parent_id = (root.get("parents") or [""])[0]
        if not parent_id:
            return root_folder_id
        parent = _get(drive, parent_id)
    except Exception:
        return root_folder_id  # 看不到上層（權限）時維持原本行為
    match = _YEAR.search(parent.get("name", ""))
    if not match or match.group(1) == year:
        _CACHE[key] = root_folder_id
        return root_folder_id
    grand_id = (parent.get("parents") or [""])[0]
    year_name = parent["name"].replace(match.group(1), year, 1)
    year_folder = _child_folder(drive, grand_id, year_name) if grand_id else None
    if not year_folder:
        raise FileNotFoundError(
            f"找不到 {year} 年度資料夾「{year_name}」；請先在 tool-system 執行「生成新年度／服務分潤表」"
        )
    area_folder = _child_folder(drive, year_folder["id"], root["name"])
    if not area_folder:
        raise FileNotFoundError(f"找不到「{year_name}/{root['name']}」")
    _CACHE[key] = area_folder["id"]
    return area_folder["id"]


# ── 檔案層級：地區設定裡的年度檔 ID 換成期別所屬年度的檔案 ─────────────
# 例：roster_id 指向「2026專員名冊與時數-台北」，執行 202701 的 00調薪／結算時
#     改用「2027專員名冊與時數-台北」（{YYYYMM}專員名冊 在新年度檔）。
YEAR_FILE_KEYS = ("allowance_id", "salary_id", "roster_id", "mail_id")
_FILE_CACHE: Dict[Tuple[str, str], str] = {}


def _norm(name: str) -> str:
    return re.sub(r"\s+", "", str(name or "")).replace("_", "-")


def _children(drive, parent_id: str) -> list:
    items, token = [], None
    while True:
        res = drive.files().list(
            q=f"'{parent_id}' in parents and trashed=false",
            fields="nextPageToken,files(id,name,mimeType)", pageSize=1000, pageToken=token,
            supportsAllDrives=True, includeItemsFromAllDrives=True,
        ).execute()
        items.extend(res.get("files", []))
        token = res.get("nextPageToken")
        if not token:
            return items


def _find(drive, parent_id: str, name: str, folder: bool):
    target = _norm(name)
    hits = [f for f in _children(drive, parent_id)
            if _norm(f.get("name")) == target
            and (f.get("mimeType") == FOLDER_MIME) == folder]
    return hits[0] if len(hits) == 1 else None


def file_for_period(drive, file_id: str, period: str) -> str:
    """回傳 file_id 在期別年度的同名檔（檔名年份替換）；同年度回傳原 ID，找不到 raise。"""
    year = str(period or "")[:4]
    if not file_id or not re.fullmatch(r"20\d{2}", year):
        return file_id
    key = (file_id, year)
    if key in _FILE_CACHE:
        return _FILE_CACHE[key]
    meta = drive.files().get(fileId=file_id, fields="id,name,parents",
                             supportsAllDrives=True).execute()
    m = _YEAR.search(meta.get("name", ""))
    if not m or m.group(1) == year:
        _FILE_CACHE[key] = file_id
        return file_id
    target = meta["name"].replace(m.group(1), year, 1)
    parent_id = (meta.get("parents") or [""])[0]
    hit = _find(drive, parent_id, target, False) if parent_id else None
    path, folder_id = [], parent_id
    for _ in range(4):  # 往上找含年份的資料夾，換年度後依相同子路徑往下找
        if hit or not folder_id:
            break
        folder = _get(drive, folder_id)
        fm = _YEAR.search(folder.get("name", ""))
        grand_id = (folder.get("parents") or [""])[0]
        if fm and grand_id:
            node = _find(drive, grand_id, folder["name"].replace(fm.group(1), year, 1), True)
            for sub in reversed(path):
                node = node and _find(drive, node["id"], sub, True)
            hit = node and _find(drive, node["id"], target, False)
            break
        path.append(folder.get("name", ""))
        folder_id = grand_id
    if not hit:
        raise FileNotFoundError(f"找不到 {year} 年度檔案「{target}」；請先執行「生成新年度」")
    _FILE_CACHE[key] = hit["id"]
    return hit["id"]


def cfg_for_period(cfg: dict | None, period: str, drive=None) -> dict:
    """地區設定的年度檔 ID 換成期別年度版本；讀不到檔案資訊時保留原值。"""
    cfg = dict(cfg or {})
    if not re.fullmatch(r"20\d{2}", str(period or "")[:4]):
        return cfg
    try:
        if drive is None:
            from modules.auth import get_jenny_drive_service
            drive = get_jenny_drive_service()
        for k in YEAR_FILE_KEYS:
            if str(cfg.get(k) or "").strip():
                try:
                    cfg[k] = file_for_period(drive, str(cfg[k]).strip(), period)
                except FileNotFoundError:
                    raise
                except Exception:
                    pass
    except FileNotFoundError:
        raise
    except Exception:
        pass
    return cfg
