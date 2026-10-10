import re

import pytest

from modules import year_root
from modules.year_root import root_for_period


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
        m = re.search(r"name='(.*?)' and", q)
        parent = re.search(r"'([^']+)' in parents", q).group(1)
        return _Req({"files": [i for i in self.items.values()
                               if i.get("parent") == parent and (not m or i["name"] == m.group(1))]})


@pytest.fixture
def drive():
    year_root._CACHE.clear()
    return FakeDrive([
        {"id": "top", "name": "專員承攬服務費"},
        {"id": "y26", "name": "2026專員承攬服務費", "parent": "top"},
        {"id": "y27", "name": "2027專員承攬服務費", "parent": "top"},
        {"id": "tp26", "name": "01.台北專員", "parent": "y26"},
        {"id": "tp27", "name": "01.台北專員", "parent": "y27"},
    ])


def test_same_year_keeps_root(drive):
    assert root_for_period(drive, "tp26", "202612-1") == "tp26"


def test_next_year_and_previous_year(drive):
    assert root_for_period(drive, "tp26", "202701-1") == "tp27"
    assert root_for_period(drive, "tp27", "202612-2") == "tp26"


def test_missing_year_folder(drive):
    with pytest.raises(FileNotFoundError):
        root_for_period(drive, "tp26", "202801-1")


def test_file_for_period_follows_year_folder(drive):
    from modules.year_root import _FILE_CACHE, file_for_period
    _FILE_CACHE.clear()
    drive.items.update({
        "r26": {"id": "r26", "name": "2026專員名冊與時數-台北", "parent": "tp26",
                "mimeType": "application/vnd.google-apps.spreadsheet"},
        "r27": {"id": "r27", "name": "2027專員名冊與時數-台北", "parent": "tp27",
                "mimeType": "application/vnd.google-apps.spreadsheet"},
    })
    for f in ("top", "y26", "y27", "tp26", "tp27"):
        drive.items[f]["mimeType"] = "application/vnd.google-apps.folder"
    assert file_for_period(drive, "r26", "202612-2") == "r26"
    assert file_for_period(drive, "r26", "202701-1") == "r27"


def test_service_fee_mail_resolves_passed_ids_by_period(drive, monkeypatch):
    """salaryapp 傳入 2026 mail_id／roster_id，期別 202701 時仍要開 2027 檔。"""
    from modules import auth, service_fee_mail
    from modules.year_root import _FILE_CACHE
    _FILE_CACHE.clear()
    for f in ("top", "y26", "y27", "tp26", "tp27"):
        drive.items[f]["mimeType"] = "application/vnd.google-apps.folder"
    sheet = "application/vnd.google-apps.spreadsheet"
    drive.items.update({
        "m26": {"id": "m26", "name": "2026承攬服務費mail-台北", "parent": "tp26", "mimeType": sheet},
        "m27": {"id": "m27", "name": "2027承攬服務費mail-台北", "parent": "tp27", "mimeType": sheet},
        "r26": {"id": "r26", "name": "2026專員名冊與時數-台北", "parent": "tp26", "mimeType": sheet},
        "r27": {"id": "r27", "name": "2027專員名冊與時數-台北", "parent": "tp27", "mimeType": sheet},
    })
    opened = []

    class Stop(Exception):
        pass

    class FakeGC:
        def open_by_key(self, key):
            opened.append(key)
            raise Stop

    monkeypatch.setattr(auth, "get_jenny_drive_service", lambda: drive)
    monkeypatch.setattr(service_fee_mail, "get_gspread_client", lambda: None)
    monkeypatch.setattr(service_fee_mail, "get_jenny_gspread_client", lambda: FakeGC())
    monkeypatch.setattr(service_fee_mail, "_region_ids", lambda gc, region: {})
    monkeypatch.setattr(service_fee_mail, "_setup_mail_sheet",
                        lambda ss, roster_id, period, log=None: opened.append(roster_id))
    with pytest.raises(Stop):
        service_fee_mail.sync_service_fee_mail(
            root_folder_id="tp26", period="202701-1", region="台北",
            mail_id="m26", roster_id="r26", cleaning_file_id="c", other_file_id="o",
            log=lambda *_: None)
    assert opened[0] == "m27"
