import unittest
from unittest import mock

from modules import import_engine as engine


class ParseSettingsTest(unittest.TestCase):
    def test_parses_known_engines_and_ignores_others(self):
        rows = [
            ["作業", "引擎", "備註"],
            ["01專員請款", "python", ""],
            ["02儲值獎金", "COMPARE"],
            ["03新人實境", "亂填"],
            ["", "PYTHON"],
        ]
        self.assertEqual(
            engine.parse_engine_settings(rows),
            {"01專員請款": "PYTHON", "02儲值獎金": "COMPARE"},
        )

    def test_read_failure_falls_back_to_importrange(self):
        log = []
        with mock.patch.object(engine, "_load_settings", side_effect=RuntimeError("quota")):
            self.assertEqual(engine.get_engine("01專員請款", log), engine.ENGINE_IMPORTRANGE)
        self.assertIn("quota", log[0])

    def test_missing_task_uses_default(self):
        with mock.patch.object(engine, "_load_settings", return_value={}):
            self.assertEqual(engine.get_engine("01專員請款"), engine.ENGINE_IMPORTRANGE)


class FilterTest(unittest.TestCase):
    def test_filter_matches_like_sheets_and_pads(self):
        rows = [
            ["202611-1", "王小美", "", "", "", 200],
            ["202611-2", "李大華"],
            ["202611-1", "陳一", "", "", "", 100, "", "11/03"],
            [],
        ]
        out = engine.filter_rows(rows, 0, "202611-1", 8)
        self.assertEqual(len(out), 2)
        self.assertEqual(out[0], ["202611-1", "王小美", "", "", "", 200, "", ""])
        self.assertEqual(out[1][7], "11/03")

    def test_text_compare_is_case_insensitive_numbers_by_value(self):
        self.assertTrue(engine._sheets_equal("abc", "ABC"))
        self.assertTrue(engine._sheets_equal(200, 200.0))
        self.assertFalse(engine._sheets_equal("200", 200))


class CompareTest(unittest.TestCase):
    def test_identical_blocks(self):
        a = [["x", 1, ""], ["y", 2.0]]
        b = [["x", 1.0], ["y", 2, ""]]
        self.assertEqual(engine.compare_blocks(a, b, 3), [])

    def test_reports_cell_and_extra_row(self):
        a = [["x", 1]]
        b = [["x", 2], ["z", 3]]
        diffs = engine.compare_blocks(a, b, 2)
        self.assertEqual(diffs, [(1, 2, 1, 2), (2, 1, "", "z"), (2, 2, "", 3)])
        summary = engine.summarize_diffs(diffs, 10)
        self.assertIn("B10", summary)
        self.assertIn("A11", summary)


class FakeWs:
    """模擬工作表：記錄寫入，讀取時回傳寫入的值。"""

    def __init__(self, preset=None):
        self.cells = {}
        self.updates = []
        self.preset = preset

    def update(self, rng, rows, value_input_option=None):
        self.updates.append((rng, rows))
        self.cells[rng] = rows

    def get(self, rng, value_render_option=None):
        if self.preset is not None:
            return self.preset
        return self.cells.get(rng, [])


class WriteStaticBlockTest(unittest.TestCase):
    def test_writes_and_verifies(self):
        ws = FakeWs()
        log = []
        n = engine.write_static_block(ws, 5, [["a", 1], ["b", 2]], 2, log)
        self.assertEqual(n, 2)
        self.assertEqual(ws.updates[0][0], "A5:B6")

    def test_readback_mismatch_raises(self):
        ws = FakeWs(preset=[["a", 9]])
        with self.assertRaises(RuntimeError):
            engine.write_static_block(ws, 2, [["a", 1]], 2, [])


class AllowanceEngineTest(unittest.TestCase):
    """01專員請款三種引擎的分流。"""

    def setUp(self):
        # 其他測試會把 modules.auth 換成殘缺的 stub；這裡用自己的 stub 隔離匯入。
        import importlib
        import sys
        import types

        auth = types.ModuleType("modules.auth")
        auth.get_gspread_client = lambda: None
        auth.open_spreadsheet = lambda *_: None
        master = types.ModuleType("modules.master_sheet")
        master.record_execution = lambda *a, **k: None
        stubs = {"modules.auth": auth, "modules.master_sheet": master}
        patch_modules = mock.patch.dict(sys.modules, stubs)
        patch_modules.start()
        self.addCleanup(patch_modules.stop)
        sys.modules.pop("modules.cleaning_process_2", None)
        cp2 = importlib.import_module("modules.cleaning_process_2")
        self.addCleanup(sys.modules.pop, "modules.cleaning_process_2", None)
        self.cp2 = cp2
        self.src_rows = [["202611-1", "王小美", "", "", "", 200, "", "11/03"]]
        self.logged = []
        patcher = mock.patch.object(
            engine, "append_engine_log",
            side_effect=lambda *a, **k: self.logged.append((a, k)),
        )
        patcher.start()
        self.addCleanup(patcher.stop)

    def _run(self, engine_name, python_rows=None, python_error=None, importrange_rows=1, sheet_rows=None):
        ws = FakeWs(preset=sheet_rows)
        ir = mock.MagicMock(return_value=importrange_rows)
        py = mock.MagicMock(return_value=python_rows if python_rows is not None else self.src_rows)
        if python_error:
            py.side_effect = python_error
        with mock.patch.object(engine, "get_engine", return_value=engine_name), \
             mock.patch.object(self.cp2, "_allowance_importrange", ir), \
             mock.patch.object(self.cp2, "_allowance_python_rows", py):
            n = self.cp2._import_allowance_rows(None, ws, "SRC", "202611-1", 2, "台北", "202611-1", [])
        return n, ir, py, ws

    def test_importrange_mode_never_calls_python(self):
        n, ir, py, _ = self._run(engine.ENGINE_IMPORTRANGE)
        self.assertEqual(n, 1)
        ir.assert_called_once()
        py.assert_not_called()
        self.assertEqual(self.logged, [])

    def test_python_mode_writes_without_importrange(self):
        n, ir, py, ws = self._run(engine.ENGINE_PYTHON)
        self.assertEqual(n, 1)
        ir.assert_not_called()
        self.assertEqual(ws.updates[0][0], "A2:H2")
        self.assertEqual(self.logged[0][0][4], "完成")

    def test_python_mode_falls_back_when_source_unreadable(self):
        n, ir, py, _ = self._run(engine.ENGINE_PYTHON, python_error=PermissionError("403"))
        ir.assert_called_once()
        self.assertEqual(self.logged[0][0][4], "退回IMPORTRANGE")

    def test_compare_mode_logs_match(self):
        sheet = [["202611-1", "王小美", "", "", "", 200, "", "11/03"]]
        n, ir, py, _ = self._run(engine.ENGINE_COMPARE, sheet_rows=sheet)
        ir.assert_called_once()
        self.assertEqual(self.logged[0][0][4], "一致")

    def test_compare_mode_logs_mismatch_but_keeps_importrange_result(self):
        sheet = [["202611-1", "王小美", "", "", "", 300, "", "11/03"]]
        n, ir, py, _ = self._run(engine.ENGINE_COMPARE, sheet_rows=sheet)
        self.assertEqual(n, 1)
        self.assertEqual(self.logged[0][0][4], "不一致")
        self.assertEqual(self.logged[0][1]["diff_count"], 1)

    def test_compare_failure_does_not_break_import(self):
        n, ir, py, _ = self._run(engine.ENGINE_COMPARE, python_error=RuntimeError("boom"))
        self.assertEqual(n, 1)
        self.assertEqual(self.logged[0][0][4], "比對失敗")


if __name__ == "__main__":
    unittest.main()


def _isolated_import(name):
    """其他測試會把 modules.auth 換成殘缺的 stub；這裡用自己的 stub 隔離匯入。"""
    import importlib
    import sys
    import types

    auth = types.ModuleType("modules.auth")
    auth.get_gspread_client = lambda: None
    auth.open_spreadsheet = lambda *_: None
    master = types.ModuleType("modules.master_sheet")
    master.record_execution = lambda *a, **k: None
    master.get_recorded_value = lambda *a, **k: None
    project = types.ModuleType("modules.project_salary")
    project.project_people = lambda *a, **k: None
    stubs = {"modules.auth": auth, "modules.master_sheet": master, "modules.project_salary": project}
    patcher = mock.patch.dict(sys.modules, stubs)
    patcher.start()
    sys.modules.pop(name, None)
    module = importlib.import_module(name)
    return module, patcher


def _salary_row(name, kr, aa):
    """調薪資料 B3:AF 一列：B=name、K:R=kr(8)、AA:AF=aa(6)。"""
    row = [""] * 31
    row[0] = name
    row[9:17] = kr
    row[25:31] = aa
    return row


class AdjustBlockTest(unittest.TestCase):
    def setUp(self):
        self.cp1, patcher = _isolated_import("modules.cleaning_process_1")
        self.addCleanup(patcher.stop)

    def test_aligned_rows(self):
        roster = [["王小美", "A1", "台北", 1, 2], ["李大華", "A2", "台北", 3, 4]]
        salary = [
            _salary_row("王小美", list(range(8)), list(range(6))),
            _salary_row("李大華", list(range(10, 18)), list(range(20, 26))),
        ]
        block = self.cp1._adj_build_block(roster, salary)
        self.assertEqual(len(block), 2)
        self.assertEqual(len(block[0]), 20)
        self.assertEqual(block[0][:5], ["王小美", "A1", "台北", 1, 2])
        self.assertEqual(block[0][5], "")
        self.assertEqual(block[1][6:14], list(range(10, 18)))
        self.assertEqual(block[1][14:20], list(range(20, 26)))

    def test_only_contiguous_roster_rows_kept(self):
        roster = [["王小美"], [""], ["李大華"]]
        salary = [_salary_row("王小美", [1] * 8, [2] * 6)]
        self.assertEqual(len(self.cp1._adj_build_block(roster, salary)), 1)

    def test_matches_by_name_even_when_order_differs(self):
        # 調薪資料順序與名冊不同：原公式會放錯列，姓名對應則正確。
        roster = [["王小美"], ["李大華"]]
        salary = [
            _salary_row("李大華", [2] * 8, [20] * 6),
            _salary_row("王小美", [1] * 8, [10] * 6),
        ]
        block = self.cp1._adj_build_block(roster, salary)
        self.assertEqual(block[0][6:14], [1] * 8)
        self.assertEqual(block[0][14:20], [10] * 6)
        self.assertEqual(block[1][6:14], [2] * 8)

    def test_duplicate_name_uses_first_row_like_vlookup(self):
        roster = [["王小美"]]
        salary = [_salary_row("王小美", [1] * 8, [1] * 6), _salary_row("王小美", [9] * 8, [9] * 6)]
        self.assertEqual(self.cp1._adj_build_block(roster, salary)[0][6], 1)

    def test_missing_name_raises_with_names(self):
        roster = [["王小美"], ["新人甲"]]
        salary = [_salary_row("王小美", [1] * 8, [1] * 6)]
        with self.assertRaises(RuntimeError) as ctx:
            self.cp1._adj_build_block(roster, salary)
        self.assertIn("新人甲", str(ctx.exception))

    def test_empty_roster_raises(self):
        with self.assertRaises(ValueError):
            self.cp1._adj_build_block([], [])


class VoucherSelectTest(unittest.TestCase):
    def setUp(self):
        self.cp2, patcher = _isolated_import("modules.cleaning_process_2")
        self.addCleanup(patcher.stop)

    def test_selects_rows_and_masked_columns(self):
        row = [""] * 54
        row[0] = "儲值金"
        for c, v in zip((2, 3, 4, 5, 13, 54), ("B", "C", "D", "E", "M", "BB")):
            row[c - 1] = v
        other = list(row)
        other[0] = "清潔"
        out = self.cp2._voucher_select([row, other, ["儲值金"]])
        self.assertEqual(out[0], ["B", "C", "D", "E", "M", "BB"])
        self.assertEqual(out[1], [""] * 6)
        self.assertEqual(len(out), 2)


class RunImportTest(unittest.TestCase):
    """共用流程：start_col 與自訂 python_write。"""

    def test_compare_reads_offset_columns(self):
        ws = mock.MagicMock()
        ws.get.return_value = [["a", "b"]]
        logged = []
        with mock.patch.object(engine, "get_engine", return_value=engine.ENGINE_COMPARE), \
             mock.patch.object(engine, "append_engine_log", side_effect=lambda *a, **k: logged.append((a, k))):
            n = engine.run_import(
                "00調薪", "台北", "202611-1", [], ws=ws, start_row=3, width=2, start_col=19,
                importrange=lambda: 1, python_rows=lambda: [["a", "b"]],
            )
        self.assertEqual(n, 1)
        self.assertEqual(ws.get.call_args[0][0], "S3:T3")
        self.assertEqual(logged[0][0][4], "一致")

    def test_python_uses_custom_writer(self):
        writer = mock.MagicMock(return_value=2)
        with mock.patch.object(engine, "get_engine", return_value=engine.ENGINE_PYTHON), \
             mock.patch.object(engine, "append_engine_log"):
            n = engine.run_import(
                "00調薪", "台北", "202611-1", [], ws=None, start_row=3, width=2,
                importrange=lambda: 0, python_rows=lambda: [[1], [2]], python_write=writer,
            )
        self.assertEqual(n, 2)
        writer.assert_called_once_with([[1], [2]])
