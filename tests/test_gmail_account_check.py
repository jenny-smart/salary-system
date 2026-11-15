import json
from types import SimpleNamespace

import pytest

from modules import auth


class _Svc:
    def __init__(self, email):
        self.email = email

    def users(self):
        return self

    def getProfile(self, userId):
        return SimpleNamespace(execute=lambda: {"emailAddress": self.email})


def _patch(monkeypatch, actual_email):
    monkeypatch.setattr(auth, "_load_region_gmail_tokens",
                        lambda: {"salary.lemontaichung@gmail.com": {"refresh_token": "x"}})
    monkeypatch.setattr(auth.UserCredentials, "from_authorized_user_info",
                        classmethod(lambda cls, info, scopes=None: SimpleNamespace(valid=True)))
    monkeypatch.setattr(auth.googleapiclient.discovery, "build",
                        lambda *a, **k: _Svc(actual_email))


def test_token_for_other_mailbox_is_rejected(monkeypatch):
    _patch(monkeypatch, "salary.lemontaipei@gmail.com")
    with pytest.raises(RuntimeError, match="授權帳號對不上"):
        auth.get_gmail_service("salary.lemontaichung@gmail.com")


def test_matching_mailbox_returns_service(monkeypatch):
    _patch(monkeypatch, "Salary.LemonTaichung@gmail.com")
    assert auth.get_gmail_service("salary.lemontaichung@gmail.com") is not None
