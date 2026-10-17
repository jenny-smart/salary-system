import re

import pytest

from modules import year_root, year_roots

FOLDER = "application/vnd.google-apps.folder"
SHEET = "application/vnd.google-apps.spreadsheet"
SHORTCUT = "application/vnd.google-apps.shortcut"


class _Req:
    def __init__(self, value):
        self.value = value

    def execute(self):
        return self.value


class FakeDrive:
    def __init__(self, items):
        self.items = {i["id"]: i for i in items}

    def files(self):
        return self

    def get(self, fileId, **_):
        i = self.items[fileId]
        return _Req({**i, "parents": [i["parent"]] if i.get("parent") else []})

    def list(self, q, **_):
        parent = re.search(r"'([^']+)' in parents", q).group(1)
        return _Req({"files": [i for i in self.items.values() if i.get("parent") == parent]})


HEADER = [["t"], ["d"], ["類別", "地區", "根目錄ID", "", "年度資料夾", "子資料夾", "檔名", "適用起年", "", "", "模式"]]


def _drive():
    return FakeDrive([
        {"id": "fin", "name": "02 營業額總表", "mimeType": FOLDER},
        {"id": "rev26", "name": "2026台北營業額總表", "mimeType": SHEET, "parent": "fin"},
        {"id": "top", "name": "專員承攬服務費", "mimeType": FOLDER},
        {"id": "y27", "name": "2027專員承攬服務費", "mimeType": FOLDER, "parent": "top"},
        {"id": "tp27", "name": "01.台北專員", "mimeType": FOLDER, "parent": "y27"},
        {"id": "rev27", "name": "2027營業額總表-台北", "mimeType": SHEET, "parent": "tp27"},
    ])


@pytest.fixture(autouse=True)
def _clear():
    year_root._FILE_CACHE.clear()


def _install(monkeypatch, drive, mode="比對"):
    values = HEADER + [["營業額總表", "台北", "top", "", "{YYYY}專員承攬服務費", "01.台北專員",
                        "{YYYY}營業額總表-台北", "2027", "", "", mode]]
    written = []
    roots = year_roots.YearRoots(values, lambda: drive, lambda r, t: written.append((r, t)))
    monkeypatch.setattr(year_roots, "get_year_roots", lambda: roots)
    return written


def test_renamed_revenue_summary_falls_back_to_root_rule(monkeypatch):
    drive = _drive()
    written = _install(monkeypatch, drive)
    cfg = year_root.cfg_for_period({"name": "台北", "revenue_summary_id": "rev26"}, "202701-1", drive=drive)
    assert cfg["revenue_summary_id"] == "rev27"
    assert any(row == 4 and "改用根目錄" in text for row, text in written)


def test_same_year_unchanged(monkeypatch):
    drive = _drive()
    _install(monkeypatch, drive)
    cfg = year_root.cfg_for_period({"name": "台北", "revenue_summary_id": "rev26"}, "202612-2", drive=drive)
    assert cfg["revenue_summary_id"] == "rev26"


def test_shortcut_with_swapped_name_resolves_to_target(monkeypatch):
    drive = _drive()
    drive.items["sc"] = {"id": "sc", "name": "2027台北營業額總表", "mimeType": SHORTCUT, "parent": "fin",
                         "shortcutDetails": {"targetId": "rev27", "targetMimeType": SHEET}}
    _install(monkeypatch, drive)
    cfg = year_root.cfg_for_period({"name": "台北", "revenue_summary_id": "rev26"}, "202701-1", drive=drive)
    assert cfg["revenue_summary_id"] == "rev27"


def test_not_found_anywhere_still_raises(monkeypatch):
    drive = _drive()
    del drive.items["rev27"]
    _install(monkeypatch, drive)
    with pytest.raises(FileNotFoundError):
        year_root.cfg_for_period({"name": "台北", "revenue_summary_id": "rev26"}, "202701-1", drive=drive)


def test_no_settings_sheet_keeps_old_behaviour(monkeypatch):
    drive = _drive()
    monkeypatch.setattr(year_roots, "get_year_roots", lambda: None)
    with pytest.raises(FileNotFoundError):
        year_root.cfg_for_period({"name": "台北", "revenue_summary_id": "rev26"}, "202701-1", drive=drive)
