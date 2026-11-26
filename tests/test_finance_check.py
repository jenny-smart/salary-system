import pytest
pytest.importorskip("streamlit")
from modules.payment_reconciliation import _finance_check_reasons


def _row(**cols):
    """cols 以欄位字母指定，例如 U="信用卡", V="HR123"。"""
    row = [""] * 62
    for letter, value in cols.items():
        idx = 0
        for ch in letter:
            idx = idx * 26 + (ord(ch) - 64)
        row[idx - 1] = value
    return row


def test_normal_order_not_flagged():
    assert _finance_check_reasons(_row(B="LC1", U="信用卡", V="HR1")) == []


def test_stored_value_without_invoice_not_flagged():
    assert _finance_check_reasons(_row(U="儲值金")) == []


@pytest.mark.parametrize("pay", ["信用卡", "ATM", "藍新ATM"])
def test_card_atm_without_invoice(pay):
    assert _finance_check_reasons(_row(U=pay)) == [f"{pay}但V發票號碼空白"]


def test_pending_charge_and_refund():
    reasons = _finance_check_reasons(_row(U="儲值金", AJ="待加收", AQ="待退款"))
    assert reasons == ["待加收", "待退款"]


def test_charged_without_invoice_date():
    assert _finance_check_reasons(_row(U="儲值金", AJ="已加收")) == ["已加收但AM加收發票日期空白"]
    assert _finance_check_reasons(_row(U="儲值金", AJ="已加收", AM="2026/11/01")) == []


@pytest.mark.parametrize("status", ["已退款", "已部分退款"])
def test_refunded_without_allowance(status):
    assert _finance_check_reasons(_row(U="儲值金", AQ=status)) == [f"{status}但AU折讓單號空白"]
    assert _finance_check_reasons(_row(U="儲值金", AQ=status, AU="D123")) == []
