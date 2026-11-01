from unittest.mock import MagicMock

from modules import period_inventory as inv

FMAP = {
    "💰 金流對帳": ["① 建立期別資料夾與檔案（手動）", "② 期別訂單轉檔（xls/xlsx → Google Sheet）"],
    "🧹 清潔承攬": ["① 前置作業", "② 00調薪"],
}


def _row(ts, system, func, area, period, status="成功", run_type="手動（Python）", msg=""):
    return [ts, system, func, run_type, area, period, "", "", status, msg, ""]


def test_latest_result_wins_and_filters_period_area():
    rows = [
        _row("2026-11-01 10:00:00", "🧹 清潔承攬", "① 前置作業", "台北", "202610-2", "失敗", msg="boom"),
        _row("2026-11-01 11:00:00", "🧹 清潔承攬", "① 前置作業", "台北", "202610-2", "成功"),
        _row("2026-11-01 09:00:00", "🧹 清潔承攬", "② 00調薪", "台北", "202610-2", "失敗", msg="x"),
        _row("2026-11-01 09:00:00", "🧹 清潔承攬", "② 00調薪", "台中", "202610-1", "成功"),  # 別期
        _row("2026-11-01 09:00:00", "🧹 清潔承攬", "② 00調薪", "台中", "202610-2", "成功", run_type="手動（GAS）"),
    ]
    status = inv.build_inventory(rows, "202610-2", ["台北", "台中"], FMAP)
    assert status[("台北", "🧹 清潔承攬", "① 前置作業")].state == inv.DONE
    assert status[("台北", "🧹 清潔承攬", "② 00調薪")].state == inv.FAILED
    assert status[("台北", "🧹 清潔承攬", "② 00調薪")].message == "x"
    assert status[("台中", "🧹 清潔承攬", "② 00調薪")].state == inv.NONE


def test_scheduled_period_folder_counts_as_done():
    rows = [_row("2026-10-16 01:00:00", "薪資系統", "排程期別資料夾", "台中", "202610-2", run_type="")]
    status = inv.build_inventory(rows, "202610-2", ["台中"], FMAP)
    assert status[("台中", "💰 金流對帳", "① 建立期別資料夾與檔案（手動）")].state == inv.DONE


def test_queue_is_area_then_system_then_function_order_and_drop_area():
    picked = [("台中", "🧹 清潔承攬", "② 00調薪"), ("台北", "🧹 清潔承攬", "① 前置作業"),
              ("台中", "💰 金流對帳", "① 建立期別資料夾與檔案（手動）")]
    queue = inv.build_queue(picked, ["台北", "台中"], FMAP, period="202610-2")
    assert [(q["area"], q["function"]) for q in queue] == [
        ("台北", "① 前置作業"), ("台中", "① 建立期別資料夾與檔案（手動）"), ("台中", "② 00調薪")]
    assert all(q["period"] == "202610-2" and q["engine"] == "PYTHON" for q in queue)
    kept, dropped = inv.drop_area(queue, "台中")
    assert dropped == 2 and [q["area"] for q in kept] == ["台北"]


def test_load_log_rows_reads_period_year_and_current_year():
    ss = MagicMock()
    ws = MagicMock()
    ws.get_all_values.return_value = [["標題"], ["a"]]
    ss.worksheet.side_effect = lambda title: ws if title.endswith("2026") else (_ for _ in ()).throw(Exception())
    assert inv.load_log_rows("202612-2", 2027, spreadsheet=ss) == [["a"]]
    assert inv.log_years("202612-2", 2027) == [2026, 2027]
