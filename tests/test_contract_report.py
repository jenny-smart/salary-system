from unittest import mock

from modules import contract_report as cr


def test_block_width_detects_taipei_and_taichung():
    assert cr.block_width(["='期別'!B2", "11501-1", "11501-2", "", "='期別'!B3", "11502-1"]) == 4
    assert cr.block_width(["id", "202301-1", "202301-2", "id2", "202302-1", "202302-2"]) == 3
    assert cr.block_width([]) == cr.DEFAULT_BLOCK_WIDTH


def test_month_labels_keep_roc_and_fix_stale():
    assert cr.month_labels("202601-2", ("11501-1", "11501-2")) == ["11501-1", "11501-2"]
    assert cr.month_labels("202601-2", ("202301-1", "202301-2")) == ["202601-1", "202601-2"]
    assert cr.month_labels("202603-2") == ["202603-1", "202603-2"]


def test_clean_person_rows_like_filter_formula():
    vals = [["場次和時數"], ["", 1, 2, 10, 20, 5], ["新人訓練費", 0, 0, 100, 200, 9],
            ["王小明", 3, 6, "1,200", 800, 50], ["0", 0, 0, 0, 0, 0], ["", 1, 1, 1, 1, 1],
            ["李", 1, 2, 300, 0, ""]]
    assert cr.clean_person_rows(vals, 4) == [["王小明", 1200, 800, 50], ["李", 300, 0, ""]]
    assert cr.clean_person_rows(vals, 3) == [["王小明", 1200, 800], ["李", 300, 0]]


def test_wash_person_rows_only_wash_service():
    vals = [["-1", "", "場次", "時數", "202611-1", "202611-2"], ["", "", "", "", 6860, 0],
            ["水洗", "洪暐智", "", "", 5740, 0], ["水洗", "劉士誠", "", "", 1120, 0],
            ["水洗", "", "", "", "", ""], ["水洗", 0, "", "", "", ""], ["地毯", "翠正", "", "", "", 0]]
    assert cr.wash_person_rows(vals, 4) == [["洪暐智", 5740, 0, ""], ["劉士誠", 1120, 0, ""]]
    assert cr.wash_person_rows(vals, 3) == [["洪暐智", 5740, 0], ["劉士誠", 1120, 0]]


def test_first_half_is_skipped_and_missing_report_id_fails():
    logs = []
    assert cr.run_contract_report_all("台北", "202611-1", "root", {"contract_report_id": "x"}, logs.append)
    assert cr.run_contract_report_all("台中", "202610-2", "root", {}, logs.append) is False
    assert any("contract_report_id" in m for m in logs)


def test_write_month_block_keeps_period_reference_and_clears_old_people():
    ws = mock.MagicMock(row_count=100, col_count=26)
    ws.get.return_value = [["='期別'!B3", "11502-1", "11502-2", ""]]
    cr.write_month_block(ws, 2, 4, "='期別'!B3", ["11502-1", "11502-2"], [10, 20],
                         [["王", 1, 2, 3]], "薪資表")
    ws.batch_clear.assert_called_once_with(["E6:H100"])
    data = ws.batch_update.call_args[0][0]
    assert data[0] == {"range": "E1:G1", "values": [["='期別'!B3", "11502-1", "11502-2"]]}
    assert data[1] == {"range": "F2:G2", "values": [[10, 20]]}
    assert {"range": "E6:H6", "values": [["王", 1, 2, 3]]} in data


def test_trim_rows_drops_trailing_blank_and_checkbox_rows():
    rows = [["姓名", "金額", "FALSE"], ["王", 800, "TRUE"], ["", "", "FALSE"], ["", "", ""]]
    assert cr.trim_rows(rows) == [["姓名", "金額", "FALSE"], ["王", 800, "TRUE"]]


def test_same_values_ignores_trailing_blanks():
    assert cr.same_values([["a", 1, ""], ["", ""]], [["a", "1"]])
    assert not cr.same_values([["a", 1]], [["a", 2]])

