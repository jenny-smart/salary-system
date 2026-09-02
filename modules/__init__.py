"""Shared module compatibility hooks."""

from gspread.exceptions import WorksheetNotFound
from gspread.spreadsheet import Spreadsheet

_original_worksheet = Spreadsheet.worksheet


def _ensure_atm_columns(ws):
    """Ensure ATM source sheets have AA:AG available before formulas are written."""
    required_cols = 33  # AG
    if ws.col_count < required_cols:
        ws.add_cols(required_cols - ws.col_count)
    return ws


def _worksheet_with_atm_alias(self, title):
    """Allow ATM source worksheets to be named either ATM or 富邦ATM."""
    try:
        ws = _original_worksheet(self, title)
    except WorksheetNotFound:
        if title != "ATM":
            raise
        ws = _original_worksheet(self, "富邦ATM")

    if title in ("ATM", "富邦ATM") or ws.title in ("ATM", "富邦ATM"):
        ws = _ensure_atm_columns(ws)
    return ws


Spreadsheet.worksheet = _worksheet_with_atm_alias
