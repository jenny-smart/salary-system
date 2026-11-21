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


class AdjustFormulaTest(unittest.TestCase):
    def setUp(self):
        self.cp1, patcher = _isolated_import("modules.cleaning_process_1")
        self.addCleanup(patcher.stop)

    def test_formulas_lookup_by_name(self):
        ws = mock.MagicMock()
        with mock.patch.object(self.cp1.time, "sleep"):
            self.cp1._adj_import_salary_k_r(ws, "SID", "202611", [])
            self.cp1._adj_import_salary_aa_af(ws, "SID", "202611", [])
        (r1, c1, f1), (r2, c2, f2) = [c.args for c in ws.update_cell.call_args_list]
        self.assertEqual((r1, c1, r2, c2), (3, 25, 3, 33))
        self.assertEqual(
            f1,
            '=ARRAYFORMULA(IF(S3:S300="",,VLOOKUP(S3:S300,'
            'IMPORTRANGE("SID","202611調薪資料!B3:R300"),{10,11,12,13,14,15,16,17},FALSE)))',
        )
        self.assertEqual(
            f2,
            '=ARRAYFORMULA(IF(S3:S300="",,VLOOKUP(S3:S300,'
            'IMPORTRANGE("SID","202611調薪資料!B3:AF300"),{26,27,28,29,30,31},FALSE)))',
        )


class FilterNoMatchTest(unittest.TestCase):
    """IMPORTRANGE 時「FILTER 篩不到資料」要能跟「還在載入」分開。"""

    def setUp(self):
        self.cp2, patcher = _isolated_import("modules.cleaning_process_2")
        self.addCleanup(patcher.stop)
        sleep = mock.patch.object(self.cp2.time, "sleep")
        sleep.start()
        self.addCleanup(sleep.stop)

    def _ws(self, err_type, message):
        ws = mock.MagicMock()
        ws.title = "02儲值獎金"
        ws.acell.return_value.value = "#N/A"
        ws.spreadsheet.fetch_sheet_metadata.return_value = {"sheets": [{"data": [{"rowData": [
            {"values": [{"effectiveValue": {"errorValue": {"type": err_type, "message": message}}}]}
        ]}]}]}
        return ws

    def test_detects_filter_message_en_and_zh(self):
        self.assertTrue(self.cp2._is_filter_no_match("N_A", "No matches are found in FILTER evaluation."))
        self.assertTrue(self.cp2._is_filter_no_match("N_A", "在 FILTER 運算中找不到相符項目。"))
        self.assertFalse(self.cp2._is_filter_no_match("N_A", "Did not find value '王小美' in VLOOKUP evaluation."))
        self.assertFalse(self.cp2._is_filter_no_match("REF", "FILTER"))

    def test_no_match_returns_zero_and_clears_formula(self):
        ws = self._ws("N_A", "No matches are found in FILTER evaluation.")
        log = []
        self.assertEqual(self.cp2._wait_and_convert(ws, "A2", log), 0)
        ws.batch_clear.assert_called_once_with(["A2"])
        self.assertEqual(ws.spreadsheet.fetch_sheet_metadata.call_count, 2)   # 連續確認兩次
        self.assertIn("視為無資料", log[-1])

    def test_no_match_raises_when_empty_not_ok(self):
        ws = self._ws("N_A", "No matches are found in FILTER evaluation.")
        with self.assertRaises(ValueError) as ctx:
            self.cp2._wait_and_convert(ws, "A2", [], empty_ok=False)
        self.assertIn("篩不到本期資料", str(ctx.exception))
        ws.batch_clear.assert_called_once_with(["A2"])

    def test_other_na_keeps_waiting_until_timeout(self):
        ws = self._ws("N_A", "Loading data...")
        clock = iter(range(0, 1000, 10))
        with mock.patch.object(self.cp2.time, "time", side_effect=lambda: next(clock)):
            with self.assertRaises(TimeoutError):
                self.cp2._wait_and_convert(ws, "A2", [], timeout=30)
        ws.batch_clear.assert_not_called()


# 202611-1 台北 薪資表 L 欄實際的舊版公式（含換行）
REAL_OLD = {
    "L1996": "=SUM(L2003:L2012)-L2015",
    "L2006": '=IFERROR(\n  SUM(FILTER(L$2:L$1995,\n    (($B$2:$B$1995<$B$2006)+($B$2:$B$1995>$B$2008))*\n    ($C$2:$C$1995<>"六")*\n    ($C$2:$C$1995<>"日")*\n    ($D$2:$D$1995<>"19:00-21:00")*\n    ($D$2:$D$1995<>"19:00-22:00")*\n    ($D$2:$D$1995<>"19:00-23:00")\n  )),\n0)',
    "L2008": '=IFERROR(\n  SUM(FILTER(L$2:L$1995,\n    (($B$2:$B$1995<$B$2006)+($B$2:$B$1995>$B$2008))*\n    (($C$2:$C$1995="六")+($C$2:$C$1995="日"))*\n    ($D$2:$D$1995<>"19:00-21:00")*\n    ($D$2:$D$1995<>"19:00-22:00")*\n    ($D$2:$D$1995<>"19:00-23:00")\n  )),\n0)\n',
    "L2009": '=IFERROR(\n  SUM(FILTER(L$2:L$1995,\n    ($B$2:$B$1995>=$B$2006)*($B$2:$B$1995<=$B$2007)*\n    ($C$2:$C$1995<>"六")*($C$2:$C$1995<>"日")\n  )),\n0)\n',
    "L2010": '=IFERROR(\n  SUM(FILTER(L$2:L$1995,\n    ($B$2:$B$1995>=$B$2006)*($B$2:$B$1995<=$B$2007)*\n    (\n      ($D$2:$D$1995="19:00-21:00")+($D$2:$D$1995="19:00-22:00")+($D$2:$D$1995="19:00-23:00")\n    )\n  )),\n0)\n',
    "L2011": '=IFERROR(\n  SUM(FILTER(L$2:L$1995,\n    ($B$2:$B$1995>=$B$2006)*($B$2:$B$1995<$B$2007)*\n    (\n      ($C$2:$C$1995="六")+($C$2:$C$1995="日")\n    )\n  )),\n0)\n',
    "L2012": '=IFERROR(\n  SUM(FILTER(L$2:L$1995,\n    ($B$2:$B$1995>=$B$2007)*($B$2:$B$1995<=$B$2008)\n  )),\n0)\n',
    "L2013": "", "L2014": "",
    "L2021": "=VLOOKUP(L$1,'00調薪'!$A:$M,10,false)",
    "L2022": "=VLOOKUP(L$1,'00調薪'!$A:$M,11,false)",
    "L2023": "=VLOOKUP(L$1,'00調薪'!$A:$M,12,false)",
    "L2024": "=VLOOKUP(L$1,'00調薪'!$A:$M,13,false)",
    "L2025": "=VLOOKUP(L$1,'00調薪'!$A:$M,14,false)",
    "L2026": "=VLOOKUP(L$1,'00調薪'!$A:$M,15,false)",
    "L2027": "=L2002*L2016+L2003*L2017+L2004*L2016+L2005*L2017+L2006*L2018+L2007*L2019+L2008*L2020+L2009*L2021+L2010*L2022+L2011*L2023+L2012*L2024",
}


class SalaryTemplateTest(unittest.TestCase):
    """前置作業：薪資表 L 欄範本（實習扣除、大掃除分類）。"""

    def setUp(self):
        self.cp1, patcher = _isolated_import("modules.cleaning_process_1")
        self.addCleanup(patcher.stop)

    def _ws(self, cells):
        ws = mock.MagicMock()
        ws.title = "薪資表"
        ws.batch_get.side_effect = lambda ranges, **k: [
            [[cells[r]]] if cells.get(r) else [] for r in ranges
        ]
        return ws

    def _written(self, ws):
        if not ws.batch_update.called:
            return {}
        return {d["range"]: d["values"][0][0] for d in ws.batch_update.call_args[0][0]}

    def _new_cells(self):
        cp = self.cp1
        cells = {f"L{r}": f + d for r, (f, d, _) in cp._CLEANUP_ROWS.items()}
        cells["L1996"] = cp._CHECK_1996[0]
        cells["L2006"] = REAL_OLD["L2006"] + cp._INTERN_SUFFIX[2006]
        cells["L2008"] = REAL_OLD["L2008"] + cp._INTERN_SUFFIX[2008]
        return cells

    def test_upgrades_real_old_template(self):
        ws = self._ws(REAL_OLD)
        log = []
        self.cp1._ensure_salary_template(ws, log)
        w = self._written(ws)
        self.assertEqual(set(w), {f"L{r}" for r in (1996, 2006, 2008, *range(2009, 2015), *range(2021, 2028))})
        self.assertNotIn("L2004", w)
        self.assertNotIn("L2005", w)
        b, e = "$B$2:$B$1995", "'04新人實習'!$E$2:$E$500"
        # 大掃除1 結束改為 < B2007（B2007 當天只屬於大掃除2）
        self.assertIn(f"({b}<$B$2007)", w["L2009"])
        self.assertIn(f"({b}<$B$2007)", w["L2010"])
        # 大掃除2 分平日／晚間／週末
        self.assertIn('($C$2:$C$1995<>"六")', w["L2012"])
        self.assertIn('($D$2:$D$1995="19:00-21:00")', w["L2013"])
        self.assertIn('(($C$2:$C$1995="六")+($C$2:$C$1995="日"))', w["L2014"])
        # 實習扣除
        for r in ("L2009", "L2011", "L2012", "L2014"):
            self.assertIn("'04新人實習'", w[r])
        self.assertNotIn("'04新人實習'", w["L2010"])
        self.assertNotIn("'04新人實習'", w["L2013"])
        self.assertIn(f"({e}<$B$2007)*(WEEKDAY({e},2)<=5)", w["L2009"])
        self.assertIn(f"({e}<=$B$2008)*(WEEKDAY({e},2)>5)", w["L2014"])
        # 費率：00調薪 J:O，大掃除1 晚間／週末對調修正、大掃除2 #REF! 修正
        self.assertEqual(w["L2022"], "=VLOOKUP(L$1,'00調薪'!$A:$O,12,false)")
        self.assertEqual(w["L2023"], "=VLOOKUP(L$1,'00調薪'!$A:$O,11,false)")
        self.assertEqual(w["L2025"], "=VLOOKUP(L$1,'00調薪'!$A:$O,15,false)")
        self.assertEqual(w["L2026"], "=VLOOKUP(L$1,'00調薪'!$A:$O,14,false)")
        self.assertTrue(w["L2027"].endswith("+L2012*L2024+L2013*L2025+L2014*L2026"))
        self.assertEqual(w["L1996"], "=SUM(L2003:L2014)-L2015")
        self.assertIn("已更新", log[-1])

    def test_already_new_writes_nothing(self):
        ws = self._ws(self._new_cells())
        self.cp1._ensure_salary_template(ws, [])
        ws.batch_update.assert_not_called()

    def test_unexpected_cleanup_row_skips_whole_cleanup_group(self):
        cells = dict(REAL_OLD, L2012="=自訂公式")
        ws = self._ws(cells)
        log = []
        self.cp1._ensure_salary_template(ws, log)
        w = self._written(ws)
        self.assertEqual(set(w), {"L2006", "L2008"})      # 只做一般平日／週末的實習扣除
        self.assertTrue(any("整組未更新" in line for line in log))

    def test_manual_minus_2003_is_replaced(self):
        suffix = self.cp1._INTERN_SUFFIX[2006]
        self.assertEqual(self.cp1._with_suffix("=SUM(X)-L2003", suffix), "=SUM(X)" + suffix)
        self.assertEqual(self.cp1._with_suffix("=SUM(X) - L2003 ", suffix), "=SUM(X)" + suffix)
