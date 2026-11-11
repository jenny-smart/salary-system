"""00調薪：薪資表 2016:2017（實境／實習承攬費）複製 L 欄樣板的回歸測試。"""
import re
import unittest

from modules import cleaning_process_1 as cp1


def _col_idx(letters):
    n = 0
    for ch in letters:
        n = n * 26 + ord(ch) - 64
    return n


def _parse(a1):
    m = re.fullmatch(r"([A-Z]+)(\d+)", a1)
    return _col_idx(m.group(1)), int(m.group(2))


class FakeSalary:
    """以 {(row, col): 公式或值} 模擬薪資表，只實作 00調薪 用到的方法。"""

    title = "薪資表"
    col_count = 60

    def __init__(self, names, cells):
        self.cells = dict(cells)
        for i, name in enumerate(names):
            self.cells[(1, 12 + i)] = name
        self.spreadsheet = self

    # ── 讀取 ──
    def row_values(self, row):
        last = max((c for (r, c) in self.cells if r == row), default=0)
        return [str(self.cells.get((row, c), "")) for c in range(1, last + 1)]

    def col_values(self, col):
        last = max((r for (r, c) in self.cells if c == col), default=0)
        return [str(self.cells.get((r, col), "")) for r in range(1, last + 1)]

    def get(self, rng, **kwargs):
        start, end = rng.split(":")
        c1, r1 = _parse(start)
        c2, r2 = _parse(end)
        out = []
        for r in range(r1, r2 + 1):
            row = [self.cells.get((r, c), "") for c in range(c1, c2 + 1)]
            while row and row[-1] == "":
                row.pop()
            out.append(row)
        while out and not out[-1]:
            out.pop()
        return out

    # ── 寫入 ──
    def _write(self, rng, values):
        rng = rng.split("!")[-1]
        start = rng.split(":")[0]
        c1, r1 = _parse(start)
        for dr, row in enumerate(values):
            for dc, v in enumerate(row):
                self.cells[(r1 + dr, c1 + dc)] = v

    def update(self, rng, values, **kwargs):
        self._write(rng, values)

    def values_batch_update(self, body):
        for item in body["data"]:
            self._write(item["range"], item["values"])

    def batch_clear(self, ranges):
        for rng in ranges:
            parts = rng.split(":")
            m1 = re.fullmatch(r"([A-Z]+)(\d*)", parts[0])
            m2 = re.fullmatch(r"([A-Z]+)(\d*)", parts[1])
            c1, c2 = _col_idx(m1.group(1)), _col_idx(m2.group(1))
            r1 = int(m1.group(2)) if m1.group(2) else 1
            r2 = int(m2.group(2)) if m2.group(2) else 10 ** 6
            for key in [k for k in self.cells if c1 <= k[1] <= c2 and r1 <= k[0] <= r2]:
                del self.cells[key]


class FakeAdjust:
    def __init__(self, names):
        self.names = names

    def col_values(self, col):
        return ["", ""] + list(self.names)


def _template(l2016):
    return {
        (2016, 12): l2016, (2017, 12): l2016,
        (2018, 12): "=L2016+80",
        (2041, 12): 500,                 # L 欄人員個別輸入值，不應帶到新人
    }


class TemplateRowsTest(unittest.TestCase):
    def _run(self, l2016, existing, new_names, existing_cells=None):
        cells = _template(l2016)
        cells.update(existing_cells or {})
        ws = FakeSalary(existing, cells)
        cp1._adj_update_salary_l1(FakeAdjust(new_names), ws, True, [])
        return ws

    def test_new_columns_get_numeric_template(self):
        ws = self._run(200, ["甲", "乙"], ["甲", "乙", "丙", "丁"])
        for col in (14, 15):               # N、O 為新增欄
            self.assertEqual(ws.cells[(2016, col)], 200)
            self.assertEqual(ws.cells[(2017, col)], 200)
            self.assertNotIn((2041, col), ws.cells)

    def test_new_columns_get_formula_template(self):
        ws = self._run("=200", ["甲"], ["甲", "乙", "丙"])
        for col in (13, 14):
            self.assertEqual(ws.cells[(2016, col)], "=200")
            self.assertEqual(ws.cells[(2018, col)],
                             f"={cp1._col_letter(col)}2016+80")

    def test_existing_blank_columns_are_filled(self):
        # 檔案由上一期複製而來：人員欄已存在，但 2016:2017 空白（202610-2 的狀況）
        ws = self._run("=200", ["甲", "乙", "丙"], ["甲", "乙", "丙"],
                       existing_cells={(2016, 13): 300})
        self.assertEqual(ws.cells[(2016, 13)], 300)      # 既有內容不動
        self.assertEqual(ws.cells[(2017, 13)], "=200")
        self.assertEqual(ws.cells[(2016, 14)], "=200")
        self.assertEqual(ws.cells[(2017, 14)], "=200")


if __name__ == "__main__":
    unittest.main()
