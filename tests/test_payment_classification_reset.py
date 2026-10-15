import sys
import types


def _stub_module(name, symbols):
    module = types.ModuleType(name)
    for symbol in symbols:
        setattr(module, symbol, lambda *args, **kwargs: None)
    sys.modules[name] = module


_stub_module(
    "modules.auth",
    ["get_drive_service", "get_credentials", "open_jenny_spreadsheet"],
)
_stub_module(
    "modules.drive_helper",
    [
        "get_folder_by_name", "find_file_in_folder", "find_file_by_keyword",
        "create_period_folder_and_files", "convert_period_order_file",
        "convert_payment_files",
    ],
)
_stub_module(
    "modules.sheet_helper",
    [
        "open_spreadsheet", "get_all_data", "get_paste_row", "paste_data",
        "find_last_non_empty_row",
    ],
)

from modules import payment_reconciliation as payment


class FakeSheet:
    def __init__(self, title):
        self.title = title


class FakeSpreadsheet:
    def __init__(self, titles):
        self.sheets = {title: FakeSheet(title) for title in titles}

    def worksheet(self, title):
        return self.sheets[title]


def _spreadsheets():
    clean = FakeSpreadsheet(["清潔營收明細"])
    other = FakeSpreadsheet(payment.OTHER_CONTRACT_MAP.values())
    return clean, other


def test_first_half_resets_every_revenue_detail_sheet_before_import(monkeypatch):
    clean, other = _spreadsheets()
    cleared = []
    monkeypatch.setattr(
        payment,
        "_clear_a2_bj_contents_and_formats",
        lambda sheet, log_fn=None: cleared.append(sheet.title),
    )

    targets = payment._prepare_classification_targets(clean, other, True)

    assert set(targets) == {*payment.OTHER_CONTRACT_MAP, "清潔"}
    assert cleared == [*payment.OTHER_CONTRACT_MAP.values(), "清潔營收明細"]


def test_second_half_keeps_existing_revenue_detail_rows(monkeypatch):
    clean, other = _spreadsheets()
    cleared = []
    monkeypatch.setattr(
        payment,
        "_clear_a2_bj_contents_and_formats",
        lambda sheet, log_fn=None: cleared.append(sheet.title),
    )

    payment._prepare_classification_targets(clean, other, False)

    assert cleared == []
