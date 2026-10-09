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
        return _Req({"id": i["id"], "name": i["name"], "parents": [i["parent"]] if i.get("parent") else []})

    def list(self, q, **_):
        name = re.search(r"name='(.*?)' and", q).group(1)
        parent = re.search(r"'([^']+)' in parents", q).group(1)
        return _Req({"files": [i for i in self.items.values() if i["name"] == name and i.get("parent") == parent]})


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
