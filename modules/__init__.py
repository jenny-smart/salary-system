"""Shared module compatibility hooks."""

from gspread.exceptions import WorksheetNotFound
from gspread.spreadsheet import Spreadsheet

_original_worksheet = Spreadsheet.worksheet


def _worksheet_with_atm_alias(self, title):
    """Allow ATM source worksheets to be named either ATM or 富邦ATM."""
    try:
        return _original_worksheet(self, title)
    except WorksheetNotFound:
        if title != "ATM":
            raise
        return _original_worksheet(self, "富邦ATM")


Spreadsheet.worksheet = _worksheet_with_atm_alias
