from unittest.mock import MagicMock

from modules import unified_log


def test_row_layout_matches_other_systems():
    row = unified_log.build_row(function="複製清潔訂單列數", area="台北", period="202610", message="ID／筆數：153")
    assert len(row) == len(unified_log.HEADERS) == 11
    assert row[1:6] == ["薪資系統", "複製清潔訂單列數", "", "台北", "202610"] and row[8] == "成功"


def test_creates_year_sheet_when_missing_and_never_raises():
    unified_log._ensured.clear()
    spreadsheet = MagicMock()
    spreadsheet.worksheet.side_effect = Exception("not found")
    unified_log.write(spreadsheet=spreadsheet, function="測試")
    spreadsheet.add_worksheet.assert_called_once()
    spreadsheet.add_worksheet.return_value.append_row.assert_called_once()
    broken = MagicMock()
    broken.worksheet.side_effect = RuntimeError("x")
    broken.add_worksheet.side_effect = RuntimeError("no access")
    unified_log._ensured.clear()
    unified_log.write(spreadsheet=broken, function="測試")  # 不拋例外


def test_button_hook_logs_clicks_once_installed():
    import types
    from unittest.mock import patch

    st = types.SimpleNamespace(session_state={}, button=lambda label, **kw: label == "▶ 執行")
    with patch.object(unified_log, "write") as write:
        unified_log.install_button_hook(st, system="🧹 清潔承攬", function="執行功能", period="202610",
                                        message="① 建立期別")
        unified_log.install_button_hook(st, area="台北")
        assert st.button("🔎 檢查本次檔案與金額") is False
        assert st.button("▶ 執行") is True
    kwargs = write.call_args.kwargs
    assert write.call_count == 1 and kwargs["status"] == "執行" and kwargs["area"] == "台北"
    assert kwargs["function"] == "執行功能／▶ 執行" and kwargs["message"] == "① 建立期別"
