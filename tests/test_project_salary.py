import ast
import importlib.util
import unittest
from pathlib import Path
from types import SimpleNamespace
spec = importlib.util.spec_from_file_location('project_salary', 'modules/project_salary.py')
project_module = importlib.util.module_from_spec(spec)
spec.loader.exec_module(project_module)
project_people, _present = project_module.project_people, project_module._present


class Project:
    row_count = 2046

    def __init__(self, rows, b2=1):
        self.rows, self.b2 = rows, b2

    def row_values(self, number):
        return [''] * 11 + ['甲', '乙']

    def get(self, address, **kwargs):
        return [[self.b2]] if address == 'B2' else self.rows


def row(name, a=0, b=0):
    return [name] + [''] * 5 + [a, b]


class Summary:
    title = '總表'

    def __init__(self, values, formulas=None):
        self.values = values
        self.formulas = formulas or []
        self.clears = []
        self.spreadsheet = self

    def get(self, address, **kwargs):
        return self.formulas if kwargs['value_render_option'] == 'FORMULA' else self.values

    def batch_clear(self, ranges):
        self.clears.extend(ranges)

    def values_batch_update(self, data):
        self.data = data


source = ast.parse(Path('modules/cleaning_process_3.py').read_text())
source.body = [n for n in source.body if isinstance(n, ast.FunctionDef)
               and n.name in {'_append_project_people_to_summary', '_log'}]
namespace = dict(gspread=SimpleNamespace(Worksheet=object), List=list,
                 SUMMARY_START=4, SUMMARY_END=120, project_people=project_people,
                 _present=_present, time=SimpleNamespace(sleep=lambda _: None))
exec(compile(source, 'settlement', 'exec'), namespace)
append_people = namespace['_append_project_people_to_summary']


class ProjectSalaryTest(unittest.TestCase):
    def test_exactly_one_required(self):
        for value in (1, 1.0, '1'):
            self.assertEqual(project_people(Project([row('甲', value)]), []), ['甲'])
        for value in (0, 2, -1, '', '錯誤', float('nan')):
            with self.subTest(value=value), self.assertRaisesRegex(ValueError, 'L2 應為 1'):
                project_people(Project([row('甲', value)]), [])

    def test_inactive_and_missing_names(self):
        self.assertEqual(project_people(Project([row('甲', 2)], b2=0), []), [])
        self.assertEqual(project_people(Project([row('0', 1, 1)]), []), [])
        with self.assertRaisesRegex(ValueError, '找不到姓名'):
            project_people(Project([row('丙', 1)]), [])

    def test_multiple_people_and_row_limit(self):
        self.assertEqual(project_people(Project([row('甲、乙', 1, 1)]), []), ['甲', '乙'])
        rows = [[]] * 998 + [row('甲', 1), row('乙', 0, 1)]
        self.assertEqual(project_people(Project(rows), []), ['甲'])

    def test_append_after_last_nonzero_name(self):
        summary = Summary([['一般'], [0], ['另一人'], ['0']])
        self.assertEqual(append_people(summary, Project([row('甲', 1)]), []), [7])

    def test_capacity_failure_does_not_clear_old_rows(self):
        summary = Summary([['一般']] * 116 + [['甲']],
                          [[]] * 116 + [['甲', '', '', "='專案薪資表'!L2045"]])
        with self.assertRaisesRegex(ValueError, '空間不足'):
            append_people(summary, Project([row('甲、乙', 1, 1)]), [])
        self.assertEqual(summary.clears, [])

    def test_rerun_reuses_project_rows(self):
        summary = Summary([['一般'], ['甲']],
                          [['一般'], ['甲', '', '', "='專案薪資表'!L2045"]])
        self.assertEqual(append_people(summary, Project([row('甲', 1)]), []), [5])
        self.assertEqual(summary.clears, ['A5:G5'])

    def test_empty_project_removes_old_rows(self):
        summary = Summary([['甲']], [['甲', '', '', "='專案薪資表'!L2045"]])
        self.assertEqual(append_people(summary, Project([], b2=0), []), [])
        self.assertEqual(summary.clears, ['A4:G4'])
