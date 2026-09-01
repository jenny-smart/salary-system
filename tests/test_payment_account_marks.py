import ast
import re
import unittest
from pathlib import Path


source = Path("modules/payment_reconciliation.py").read_text(encoding="utf-8")
tree = ast.parse(source)
selected = [
    node for node in tree.body
    if isinstance(node, ast.FunctionDef)
    and node.name in {"_month_text", "_extract_year_month", "_account_mark"}
]
namespace = {"re": re}
exec(compile(ast.Module(body=selected, type_ignores=[]), "account_marks", "exec"), namespace)
_account_mark = namespace["_account_mark"]


class AccountMarkTest(unittest.TestCase):
    def test_current_and_prepaid_marks(self):
        period = "202609-1"
        cases = [
            ("2026/09/01", "1專業清潔", "清潔本月"),
            ("2026-10-01", "1專業清潔", "清潔預收"),
            ("2026/9/30", "套餐3水洗服務", "水洗本月"),
            ("2026/08/31", "套餐3水洗服務", "水洗預收"),
            ("2026/09/15", "4家電清潔", "家電本月"),
            ("2026/10/15", "4家電清潔", "家電預收"),
        ]
        for service_date, service_name, expected in cases:
            with self.subTest(expected=expected):
                self.assertEqual(
                    _account_mark(period, service_date, service_name), expected
                )

    def test_stored_value_is_same_for_every_month(self):
        self.assertEqual(
            _account_mark("202609-2", "2026/09/01", "VIP儲值金"), "儲值金"
        )
        self.assertEqual(
            _account_mark("202609-2", "2026/12/01", "VIP儲值金"), "儲值金"
        )

    def test_invalid_or_unmatched_rows_are_not_marked(self):
        self.assertIsNone(_account_mark("202609-1", "", "1專業清潔"))
        self.assertIsNone(_account_mark("202609-1", "2026/09/01", "其他服務"))
        self.assertIsNone(
            _account_mark("202609-1", "2026/09/01", "1專業清潔加購")
        )


if __name__ == "__main__":
    unittest.main()
