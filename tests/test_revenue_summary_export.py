import ast
import re
import unittest
from pathlib import Path


source = Path("modules/payment_reconciliation.py").read_text(encoding="utf-8")
tree = ast.parse(source)
selected = [
    node for node in tree.body
    if isinstance(node, ast.FunctionDef) and node.name == "_revenue_sheet_name"
]
namespace = {"re": re}
exec(compile(ast.Module(body=selected, type_ignores=[]), "revenue_sheet", "exec"), namespace)
_revenue_sheet_name = namespace["_revenue_sheet_name"]


class RevenueSheetNameTest(unittest.TestCase):
    def test_both_half_periods_use_same_month_sheet(self):
        self.assertEqual(_revenue_sheet_name("202609-1"), "202609")
        self.assertEqual(_revenue_sheet_name("202609-2"), "202609")

    def test_invalid_period_is_rejected(self):
        for period in ("", "20269-1", "202609-3", "2026/09"):
            with self.subTest(period=period):
                with self.assertRaises(ValueError):
                    _revenue_sheet_name(period)


if __name__ == "__main__":
    unittest.main()
