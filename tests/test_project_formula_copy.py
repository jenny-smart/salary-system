import ast
import unittest
from pathlib import Path
from types import SimpleNamespace

source = Path('modules/cleaning_process_1.py').read_text(encoding='utf-8')
node = next(n for n in ast.parse(source).body
            if isinstance(n, ast.FunctionDef) and n.name == '_copy_project_formulas')
logs = []
namespace = {
    'List': list, 'gspread': SimpleNamespace(Worksheet=object),
    '_log': lambda log, msg: log.append(msg),
    '_col_letter': lambda n: {13: 'M', 14: 'N'}[n],
}
exec(compile(ast.Module([node], []), 'x', 'exec'), namespace)
copy_formulas = namespace['_copy_project_formulas']


class Sheet:
    id, row_count = 7, 3000

    def __init__(self):
        self.requests = []
        self.spreadsheet = SimpleNamespace(batch_update=self.requests.append)


class CopyProjectFormulas(unittest.TestCase):
    def test_copies_l_column_formulas_to_other_people(self):
        ws, log = Sheet(), []
        copy_formulas(ws, 14, log)
        req = ws.requests[0]['requests'][0]['copyPaste']
        self.assertEqual(req['pasteType'], 'PASTE_FORMULA')
        self.assertEqual((req['source']['startColumnIndex'], req['source']['endColumnIndex']), (11, 12))
        self.assertEqual((req['destination']['startColumnIndex'], req['destination']['endColumnIndex']), (12, 14))
        self.assertEqual((req['source']['startRowIndex'], req['source']['endRowIndex']), (1, 2048))

    def test_single_person_skips_copy(self):
        ws = Sheet()
        copy_formulas(ws, 12, [])
        self.assertEqual(ws.requests, [])


if __name__ == '__main__':
    unittest.main()
