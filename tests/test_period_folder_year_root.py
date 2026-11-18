"""2027 年跑 202701-1 時，期別資料夾要到「2027專員承攬服務費/同名地區資料夾」找（不是 2026 的地區資料夾）。"""
import importlib
import sys
from unittest.mock import MagicMock

import pytest

_REAL = ("modules.auth", "modules.year_root", "modules.cleaning_process", "modules.cleaning_process_1",
         "modules.cleaning_process_4")


@pytest.fixture(autouse=True)
def real_modules():
    """其他測試可能把 modules.auth 換成假模組；這裡改用真的模組，結束後還原。"""
    saved = {name: sys.modules.pop(name) for name in _REAL if name in sys.modules}
    yield
    for name in _REAL:
        sys.modules.pop(name, None)
    sys.modules.update(saved)


def _drive(period_folder_parent):
    """假 Drive：只有在 period_folder_parent 底下找得到期別資料夾與檔案。"""
    drive = MagicMock()

    def list_(q, **kwargs):
        resp = MagicMock()
        found = f"'{period_folder_parent}' in parents" in q or "'PERIOD' in parents" in q
        resp.execute.return_value = {"files": [{"id": "PERIOD" if "folder" in q else "FILE", "name": "x"}] if found else []}
        return resp

    drive.files.return_value.list.side_effect = list_
    return drive


@pytest.mark.parametrize("module_name, func", [
    ("modules.cleaning_process_1", "find_cleaning_file"),
    ("modules.cleaning_process_1", "find_payment_file"),
    ("modules.cleaning_process", "find_cleaning_file"),
])
def test_find_period_file_uses_year_root(monkeypatch, module_name, func):
    mod = importlib.import_module(module_name)
    drive = _drive("AREA-2027")
    monkeypatch.setattr(importlib.import_module("modules.auth"), "get_drive_service", lambda: drive)
    calls = []
    monkeypatch.setattr(importlib.import_module("modules.year_root"), "root_for_period",
                        lambda d, root, period: calls.append((root, period)) or "AREA-2027")
    assert getattr(mod, func)("AREA-2026", "202701-1", "台北") == "FILE"
    assert calls == [("AREA-2026", "202701-1")]


def test_yuanta_period_file_uses_year_root(monkeypatch):
    mod = importlib.import_module("modules.cleaning_process_4")
    drive = _drive("AREA-2027")
    monkeypatch.setattr(importlib.import_module("modules.auth"), "get_drive_service", lambda: drive)
    monkeypatch.setattr(importlib.import_module("modules.year_root"), "root_for_period", lambda d, root, period: "AREA-2027")
    folder_id, file_id = mod._yuanta_find_period_file("AREA-2026", "202701-1", "202701-1元大帳戶-台北")
    assert (folder_id, file_id) == ("PERIOD", "FILE")
