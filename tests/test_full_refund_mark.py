import ast
import json
import re
import unicodedata
import unittest
from pathlib import Path

import requests  # noqa: F401  (lemon_backend imports it)

from modules import lemon_backend


source = Path("modules/payment_reconciliation.py").read_text(encoding="utf-8")
tree = ast.parse(source)
selected = [
    node for node in tree.body
    if isinstance(node, ast.FunctionDef)
    and node.name in {"_extract_year_month", "_base_purchase_order_no", "_is_na", "_full_refund_candidates"}
]
namespace = {"re": re, "unicodedata": unicodedata}
exec(compile(ast.Module(body=selected, type_ignores=[]), "full_refund", "exec"), namespace)
_full_refund_candidates = namespace["_full_refund_candidates"]


class FullRefundCandidateTest(unittest.TestCase):
    def test_only_na_blank_bk_and_current_month(self):
        rows = [
            ["2026/10"],
            ["", "2026/10/20", "LC00217679", "信用卡", "FP56436998", "2400", "0", "#N/A"],
            ["", "2026/10/6", "LC002169601-1", "信用卡", "FP56436789", "4800", "0", "#N/A"],
            ["已全額退款", "2026/10/11", "LC00217208", "信用卡", "FP1", "4200", "0", "#N/A"],
            ["", "2026/09/30", "LC00200000", "信用卡", "FP2", "100", "0", "#N/A"],
            ["", "2026/10/28", "LC00217997", "ATM", "FP56437110", "600", "0", "FP56437110"],
        ]
        self.assertEqual(
            _full_refund_candidates(rows, "2026/10"),
            {"LC00217679": [2], "LC002169601": [3]},
        )


class RefundStatusParseTest(unittest.TestCase):
    def html(self, items):
        return "<script>new Vue({data:{purchaseList: " + json.dumps({"data": items}) + ", x: 1}})</script>"

    def test_full_refund_is_isrefund_3(self):
        items = lemon_backend.extract_purchase_list(self.html([
            {"order_no": "LC002169601", "isRefund": 3},
            {"order_no": "LC00217679", "isRefund": "2"},
        ]))
        self.assertTrue(lemon_backend.is_full_refund_item(items[0]))
        self.assertFalse(lemon_backend.is_full_refund_item(items[1]))

    def test_missing_purchase_list(self):
        self.assertEqual(lemon_backend.extract_purchase_list("<html></html>"), [])


if __name__ == "__main__":
    unittest.main()
