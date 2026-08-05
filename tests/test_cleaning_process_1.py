import unittest
from unittest.mock import patch

from modules import cleaning_process_1 as process


class FakeWorksheet:
    def __init__(self, column=None, row=None):
        self.column = column or []
        self.row = row or []
        self.cleared = []
        self.updated = []

    def col_values(self, _column):
        return self.column

    def row_values(self, _row):
        return self.row

    def batch_clear(self, ranges):
        self.cleared.extend(ranges)

    def update(self, *args, **kwargs):
        self.updated.append((args, kwargs))


class AdjustmentTests(unittest.TestCase):
    def test_to_num_handles_sheet_values(self):
        self.assertEqual(process._to_num("1,234.5"), 1234.5)
        self.assertEqual(process._to_num(""), 0.0)

    def test_step12_stops_before_clearing_on_large_roster_drop(self):
        adjust = FakeWorksheet(column=["", ""] + ["A"])
        salary = FakeWorksheet(row=[""] * 11 + [f"E{i}" for i in range(17)])

        with self.assertRaisesRegex(ValueError, "17 → 1"):
            process._adj_update_salary_l1(adjust, salary, True, [])

        self.assertEqual(salary.cleared, [])
        self.assertEqual(salary.updated, [])

    def test_step12_stops_on_empty_roster_without_clearing(self):
        adjust = FakeWorksheet(column=["", ""])
        salary = FakeWorksheet(row=[""] * 11 + ["A", "B"])

        with self.assertRaisesRegex(ValueError, "2 → 0"):
            process._adj_update_salary_l1(adjust, salary, True, [])

        self.assertEqual(salary.cleared, [])
        self.assertEqual(salary.updated, [])

    def test_run_adjustment_logs_full_traceback(self):
        log = []
        with patch.object(process, "get_gspread_client", side_effect=RuntimeError("boom")):
            result = process.run_adjustment("id", "region", "202608-1", True, log, {})

        self.assertFalse(result)
        self.assertTrue(any("Traceback (most recent call last)" in line for line in log))
        self.assertTrue(any("RuntimeError: boom" in line for line in log))


if __name__ == "__main__":
    unittest.main()
