import importlib

import pytest

ms = importlib.import_module("modules.master_sheet")


class FakeSheet:
    def __init__(self, header, col_count):
        self.header, self.col_count, self.updates, self.added = header, col_count, [], 0

    def row_values(self, n):
        return list(self.header)

    def add_cols(self, n):
        self.added += n
        self.col_count += n

    def update(self, rng, values):
        self.updates.append((rng, values))


def _header(last_period_months):
    row = ["作業名稱"]
    year, month = 2028, 12 - last_period_months + 1
    for _ in range(last_period_months):
        for half in (1, 2):
            row += [f"{year}{month:02d}-{half}", ""]
        month += 1
    return row


def test_existing_period_found_without_writes():
    sheet = FakeSheet(_header(2), 10)          # 202811-1 … 202812-2
    assert ms._find_period_col(sheet, "202812-1", create=True) == 6
    assert sheet.updates == []


def test_new_year_period_appended_after_last():
    sheet = FakeSheet(_header(1), 5)           # 作業名稱, 202812-1, "", 202812-2, ""
    col = ms._find_period_col(sheet, "202901-2", create=True)
    assert sheet.updates == [("F1", [["202901-1", "", "202901-2", ""], ["ID/筆數", "完成時間", "ID/筆數", "完成時間"]])]
    assert col == 8 and sheet.added == 4       # 補到 I 欄（202901-2 的完成時間）


def test_read_only_or_older_period_is_not_created():
    sheet = FakeSheet(_header(1), 5)
    with pytest.raises(ValueError):
        ms._find_period_col(sheet, "202901-1")             # 讀取不建立
    with pytest.raises(ValueError):
        ms._find_period_col(sheet, "202701-1", create=True)  # 比最後期別早不補
    assert sheet.updates == []


def test_next_period():
    assert ms._next_period("202812-1") == "202812-2"
    assert ms._next_period("202812-2") == "202901-1"
