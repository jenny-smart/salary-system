import ast
import unittest
from pathlib import Path
from types import SimpleNamespace

source = Path('modules/cleaning_process_1.py').read_text(encoding='utf-8')
node = next(n for n in ast.parse(source).body
            if isinstance(n, ast.FunctionDef) and n.name == '_copy_formula_cells')
namespace = {
    'List': list, 'gspread': SimpleNamespace(Worksheet=object),
    '_log': lambda log, msg: log.append(msg),
    '_col_letter': lambda n: {13: 'M', 14: 'N'}[n],
}
exec(compile(ast.Module([node], []), 'x', 'exec'), namespace)
copy_formulas = namespace['_copy_formula_cells']


class Sheet:
    id, row_count, title = 7, 3000, '專案薪資表'

    def __init__(self, column_l):
        self.column_l, self.requests = column_l, []
        self.spreadsheet = SimpleNamespace(batch_update=self.requests.append)

    def get(self, address, **kwargs):
        assert kwargs['value_render_option'] == 'FORMULA'
        return [[v] if v != '' else [] for v in self.column_l]


class CopyFormulaCells(unittest.TestCase):
    def test_only_formula_runs_are_copied(self):
        # 列 2,3 公式；列 4 固定值；列 5 空白；列 6 公式
        ws = Sheet(['=A2', '=A3', 5, '', '=A6'])
        copy_formulas(ws, 14, [])
        reqs = ws.requests[0]['requests']
        spans = [(r['copyPaste']['source']['startRowIndex'],
                  r['copyPaste']['source']['endRowIndex']) for r in reqs]
        self.assertEqual(spans, [(1, 3), (5, 6)])
        for r in reqs:
            c = r['copyPaste']
            self.assertEqual(c['pasteType'], 'PASTE_FORMULA')
            self.assertEqual((c['destination']['startColumnIndex'],
                              c['destination']['endColumnIndex']), (12, 14))

    def test_no_formula_means_no_request(self):
        ws = Sheet([1, '', 'x'])
        copy_formulas(ws, 14, [])
        self.assertEqual(ws.requests, [])

    def test_single_person_skips_copy(self):
        ws = Sheet(['=A2'])
        copy_formulas(ws, 12, [])
        self.assertEqual(ws.requests, [])


if __name__ == '__main__':
    unittest.main()
