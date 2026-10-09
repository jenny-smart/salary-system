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
