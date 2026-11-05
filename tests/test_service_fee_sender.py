import base64
import email
from email import policy

from modules import service_fee_sender as sender

MAILBOX = "salary.lemontaipei@gmail.com"
TEMPLATE = "<div>&lt;&lt; Name &gt;&gt;&nbsp;您好</div><div>連結：&lt;&lt; link &gt;&gt;</div>"


class FakeSheet:
    def __init__(self, rows):
        self.rows = rows  # A:I，從第 2 列開始
        self.updates = []

    def get(self, rng, value_render_option=None):
        return [list(r) for r in self.rows]

    def update(self, range_name=None, values=None, value_input_option=None):
        self.header = values

    def batch_update(self, data, value_input_option=None):
        for item in data:
            start = item["range"].split(":")[0]
            col, row = ord(start[0]) - ord("A"), int(start[1:])
            vals = item["values"][0]
            r = self.rows[row - 2]
            r += [""] * (12 - len(r))
            r[col:col + len(vals)] = vals
            if start[0] == "F":
                self.updates.append(row)


class _Exec:
    def __init__(self, value):
        self.value = value

    def execute(self):
        return self.value


class FakeGmail:
    def __init__(self, sent_before=None, threads=None, bounces=None):
        self.sent = []
        self.sent_before = sent_before or {}
        self.threads_map = threads or {}
        self.bounces = bounces or []

    def users(self):
        return self

    def messages(self):
        return self

    def threads(self):
        gm = self

        class T:
            def get(self, userId, id, **kw):
                return _Exec({"messages": gm.threads_map.get(id, [])})
        return T()

    def send(self, userId, body):
        msg = email.message_from_bytes(base64.urlsafe_b64decode(body["raw"]), policy=policy.default)
        self.sent.append(msg)
        return _Exec({"id": f"m{len(self.sent)}", "threadId": f"t{len(self.sent)}"})

    def list(self, userId, q, maxResults=None):
        if q.startswith("from:"):
            return _Exec({"messages": [{"id": f"b{i}"} for i in range(len(self.bounces))]})
        for key, msg in self.sent_before.items():
            if f"to:{key}" in q:
                return _Exec({"messages": [{"id": "old"}]})
        return _Exec({"messages": []})

    def get(self, userId, id, **kw):
        if id.startswith("b"):
            return _Exec(self.bounces[int(id[1:])])
        return _Exec(next(iter(self.sent_before.values())))


def _row(email_addr, name, subject=None, sent_at="", file=None):
    subject = subject or f"202610-2 檸檬家事｜清潔承攬服務費_{name}"
    file = file or f"202610-2 檸檬家事｜清潔承攬服務費_{name}"
    return [email_addr, name, f"https://drive.google.com/file/d/{name}/view", file, subject, sent_at, "", "", "",
            "", ""]


def test_render_body_fills_name_and_link():
    body = sender.render_body(TEMPLATE, "蔡立娟", "https://x.y/a?b=1&c=2")
    assert "蔡立娟&nbsp;您好" in body and '<a href="https://x.y/a?b=1&amp;c=2">' in body


def _drive_names(link):
    return f"202610-2 檸檬家事｜清潔承攬服務費_{link.split('/d/')[1].split('/')[0]}.pdf"


def test_sends_only_unsent_rows_and_records_status():
    ws = FakeSheet([_row("a@x.com", "甲"), _row("b@x.com", "乙", sent_at="2026/11/01 10:00:00")])
    gmail = FakeGmail()
    result = sender.send_service_fee_mails(gmail, ws, MAILBOX, "檸檬家事-台北", TEMPLATE, pause=0,
                                           period="202610-2", link_file_name=_drive_names)
    assert result["sent"] == 1 and result["skipped"] == 1 and not result["failed"]
    msg = gmail.sent[0]
    assert msg["To"] == "a@x.com" and msg["Subject"] == "202610-2 檸檬家事｜清潔承攬服務費_甲"
    assert "甲" in msg.get_body(("html",)).get_content()
    assert ws.rows[0][6] == sender.SENT and ws.rows[0][8] == "t1" and ws.rows[0][5]


def test_any_mismatch_stops_whole_batch():
    import pytest

    rows = [_row("a@x.com", "甲"),
            _row("b@x.com", "乙", subject="202610-2 檸檬家事｜清潔承攬服務費_丙"),   # 主旨是別人
            _row("c@x.com", "丁", file="202610-2 檸檬家事｜清潔承攬服務費_戊"),      # D 欄檔名是別人
            _row("bad", "己")]
    rows.append(_row("e@x.com", "庚"))
    rows[4][2] = "https://drive.google.com/file/d/辛/view"                           # 連結實際是別人
    ws = FakeSheet(rows)
    gmail = FakeGmail()
    with pytest.raises(RuntimeError, match="4 列核對不符"):
        sender.send_service_fee_mails(gmail, ws, MAILBOX, "檸檬家事-台北", TEMPLATE, pause=0,
                                      period="202610-2", link_file_name=_drive_names)
    assert gmail.sent == []
    assert ws.rows[0][6] == "" and ws.rows[0][5] == ""
    assert ws.rows[1][6].startswith("失敗：主旨不是乙")
    assert ws.rows[2][6].startswith("失敗：D 欄檔名不是丁")
    assert ws.rows[3][6] == "失敗：無 Email"
    assert ws.rows[4][6].startswith("失敗：連結檔案不是庚")


def test_wrong_period_subject_is_rejected():
    r = {"name": "甲", "subject": "202610-1 檸檬家事｜清潔承攬服務費_甲", "file": "", "link": "x"}
    assert "不是本期" in sender.check_same_person(r, sender.FEE, "202610-2")


def test_utf8_and_emoji_survive():
    from datetime import datetime
    from modules.service_fee_draft import build_mail_html

    body = sender.render_body(build_mail_html("202610-2", "台北", datetime(2026, 11, 2, 14)), "蔡立娟", "https://x")
    raw = base64.urlsafe_b64decode(sender.build_message(MAILBOX, "檸檬家事-台北", "a@x.com", "主旨😊", body)["raw"])
    msg = email.message_from_bytes(raw, policy=policy.default)
    html_part = msg.get_body(("html",))
    assert html_part.get_content_charset() == "utf-8" and '<meta charset="utf-8">' in html_part.get_content()
    for ch in ("😊", "💰", "🙏", "✨\ufe0f", "📩", "⏰\ufe0f", "🧰", "🍋"):
        assert ch in html_part.get_content() and ch in msg.get_body(("plain",)).get_content()
    assert msg["Subject"] == "主旨😊"


def _dep_row(email_addr, name, subject=None, sent_at=""):
    return [email_addr, name, "91", "2026/11/10", "1,500", subject or f"2026/11/10 檸檬家事｜工具包押金_{name}",
            sent_at, "", "", "", "", ""]


def test_deposit_mail_uses_g_columns_and_template():
    ws = FakeSheet([_dep_row("a@x.com", "林佳緯"), _dep_row("b@x.com", "鄭佩芬", subject="2026/11/10 押金_別人")])
    gmail = FakeGmail()
    import pytest
    with pytest.raises(RuntimeError):
        sender.send_mails(gmail, ws, MAILBOX, "檸檬家事財務", lambda r, t: sender.build_deposit_html(r),
                          sender.DEPOSIT, "202610-2", pause=0)
    assert gmail.sent == [] and ws.rows[1][7].startswith("失敗：主旨不是鄭佩芬")

    ws.rows[1][5] = "2026/11/10 檸檬家事｜工具包押金_鄭佩芬"
    ws.rows[1][7] = ""
    result = sender.send_mails(gmail, ws, MAILBOX, "檸檬家事財務", lambda r, t: sender.build_deposit_html(r),
                               sender.DEPOSIT, "202610-2", pause=0)
    assert result["sent"] == 2 and ws.rows[0][6] and ws.rows[0][7] == "已寄出" and ws.rows[0][9] == "t1"
    body = gmail.sent[0].get_body(("html",)).get_content()
    assert "已達91場" in body and "color:#ff0000\">2026/11/10" in body and "匯款1,500元" in body
    assert gmail.sent[0]["From"].startswith("檸檬家事財務")  # 測試直接傳入的名稱



def test_does_not_resend_when_found_in_sent_folder():
    old = {"internalDate": "1762300000000", "threadId": "told",
           "payload": {"headers": [{"name": "Subject", "value": "202610-2 檸檬家事｜清潔承攬服務費_甲"}]}}
    ws = FakeSheet([_row("a@x.com", "甲")])
    gmail = FakeGmail(sent_before={"a@x.com": old})
    result = sender.send_service_fee_mails(gmail, ws, MAILBOX, "檸檬家事-台北", TEMPLATE, pause=0)
    assert gmail.sent == [] and result["sent"] == 0 and ws.rows[0][8] == "told"


def test_test_mode_sends_one_and_writes_nothing():
    ws = FakeSheet([_row("a@x.com", "甲"), _row("b@x.com", "乙")])
    gmail = FakeGmail()
    sender.send_service_fee_mails(gmail, ws, MAILBOX, "檸檬家事-台北", TEMPLATE, test_to="me@x.com", pause=0)
    assert len(gmail.sent) == 1 and gmail.sent[0]["To"] == "me@x.com"
    assert ws.updates == []


def test_refresh_detects_reply_and_bounce():
    def m(sender_addr, ms):
        return {"internalDate": ms, "payload": {"headers": [{"name": "From", "value": sender_addr}]}}

    ws = FakeSheet([
        ["a@x.com", "甲", "l", "f", "s", "2026/11/05 10:00:00", "已寄出", "2026/11/05 10:00:00", "t1"],
        ["b@x.com", "乙", "l", "f", "s", "2026/11/05 10:00:00", "已寄出", "2026/11/05 10:00:00", "t2"],
        ["c@x.com", "丙", "l", "f", "s", "2026/11/05 10:00:00", "已寄出", "2026/11/05 10:00:00", "t3"],
        ["d@x.com", "丁", "l", "f", "s", "2026/11/05 10:00:00", "已寄出", "2026/11/05 10:00:00", "t4"],
    ])
    gmail = FakeGmail(
        threads={
            "t1": [m(MAILBOX, "1762308000000"), m("甲 <a@x.com>", "1762311600000")],
            "t2": [m(MAILBOX, "1762308000000")],
            "t3": [m(MAILBOX, "1762308000000"), m("Mail Delivery Subsystem <mailer-daemon@googlemail.com>",
                                                 "1762308060000")],
            "t4": [m(MAILBOX, "1762308000000")],
        },
        bounces=[{"internalDate": "1762308100000",
                  "payload": {"headers": [{"name": "X-Failed-Recipients", "value": "D@x.com"}]}}],
    )
    counts = sender.refresh_statuses(gmail, ws, MAILBOX)
    assert counts == {sender.SENT: 1, sender.CLICKED: 0, sender.REPLIED: 1, sender.BOUNCED: 2}
    assert ws.rows[0][6] == "已回覆" and ws.rows[1][6] == "已寄出"
    assert ws.rows[2][6] == "退信" and ws.rows[3][6] == "退信"
    assert ws.updates == [2, 4, 5]  # 沒變的列不重寫


def test_resync_keeps_sent_status_by_name_and_file(monkeypatch):
    import importlib
    import sys

    # 其他測試會在 sys.modules 放假模組，這裡載入真的
    for name in ("modules.auth", "modules.drive_helper", "modules.sheet_helper", "modules.service_fee_mail"):
        monkeypatch.delitem(sys.modules, name, raising=False)
    service_fee_mail = importlib.import_module("modules.service_fee_mail")

    class Ws:
        def __init__(self):
            self.rows = [["甲", "l", "202610-2 甲", "s", "2026/11/05 10:00:00", "已回覆", "t", "th1", "ab", "c"],
                         ["舊", "l", "202610-1 舊", "s", "2026/10/20 10:00:00", "已寄出", "t", "th0", "cd", ""]]
            self.writes = {}

        def update_cell(self, *a):
            pass

        def get(self, rng, value_render_option=None):
            return self.rows if rng == "B2:K" else [[r[0]] for r in self.writes.get("B", [])]

        def batch_clear(self, ranges):
            assert ranges == ["B2:K"]

        def update(self, rng, values, value_input_option=None):
            self.writes[rng[0]] = values

    ws = Ws()
    service_fee_mail._write_period_rows(ws, "202610-2", [("甲", "l", "202610-2 甲.pdf"), ("乙", "l", "202610-2 乙.pdf")],
                                        [], [])
    assert ws.writes["F"] == [["2026/11/05 10:00:00", "已回覆", "t", "th1", "ab", "c"], [""] * 6]


def test_tracking_link_and_token_written_before_send():
    ws = FakeSheet([_row("a@x.com", "甲")])
    gmail = FakeGmail()
    track = {"gas_url": "https://script.google.com/macros/s/X/exec", "mail_id": "MID", "period": "202610-2"}
    sender.send_service_fee_mails(gmail, ws, MAILBOX, "檸檬家事-台北", TEMPLATE, pause=0, track=track)
    token = ws.rows[0][9]
    assert len(token) == 16
    html_body = gmail.sent[0].get_body(("html",)).get_content()
    assert f"action=click&amp;m=MID&amp;s=202610-2&amp;k={token}" in html_body
    assert sender.LINK_TEXT in html_body and "drive.google.com" not in html_body
    assert "action=click" in gmail.sent[0].get_body(("plain",)).get_content()


def test_refresh_keeps_clicked_when_no_reply():
    ws = FakeSheet([["a@x.com", "甲", "l", "f", "s", "2026/11/05 10:00:00", "已點連結", "2026/11/05 11:00:00",
                     "t1", "abcd", "2026/11/05 11:00:00"]])
    gmail = FakeGmail(threads={"t1": [{"internalDate": "1762308000000",
                                       "payload": {"headers": [{"name": "From", "value": MAILBOX}]}}]})
    counts = sender.refresh_statuses(gmail, ws, MAILBOX)
    assert counts[sender.CLICKED] == 1 and ws.updates == []


def test_deposit_resync_keeps_status_only_for_existing_sheet(monkeypatch):
    import importlib
    import sys

    for name in ("modules.auth", "modules.drive_helper", "modules.sheet_helper", "modules.service_fee_mail"):
        monkeypatch.delitem(sys.modules, name, raising=False)
    sfm = importlib.import_module("modules.service_fee_mail")

    class Ws:
        title = "202610-2工具包押金"

        def __init__(self):
            self.writes = {}

        def get(self, rng, value_render_option=None):
            return [["林佳緯", "91", "2026/11/10", "1500", "memo", "2026/11/05 10:00:00", "已寄出", "t", "th", "", ""]]

        def batch_clear(self, ranges):
            assert ranges == ["B2:C", "G2:L"]

        def update(self, rng, values, value_input_option=None):
            self.writes[rng[0]] = values

    class Cleaning:
        pass

    monkeypatch.setattr(sfm, "_deposit_source_rows", lambda ss: [("林佳緯", 91), ("鄭佩芬", 97)])
    monkeypatch.setattr(sfm, "_deposit_amount", lambda region, log=None: 1500)
    for existed in (True, False):
        ws = Ws()
        monkeypatch.setattr(sfm, "_sheet_or_none", lambda ss, title, e=existed: ws if e else None)
        monkeypatch.setattr(sfm, "_ensure_deposit_sheet", lambda ss, period, log=None, w=ws: w)
        sfm._sync_tool_deposit(None, Cleaning(), "202610-2", "台中")
        if existed:
            assert ws.writes["G"] == [["2026/11/05 10:00:00", "已寄出", "t", "th", "", ""], [""] * 6]
        else:
            assert "G" not in ws.writes  # 剛從上一期複製的狀態不保留


def test_astral_emoji_are_real_4_byte_utf8_not_cesu8():
    """舊工具把 😊💰🙏 等拆成兩半各自編碼（CESU-8），收件端顯示成 ������。"""
    body = "<div>😊💰🙏📄📩💡🧰🍋 ✨\ufe0f⏰\ufe0f</div>"
    raw = base64.urlsafe_b64decode(sender.build_message(MAILBOX, "x", "a@x.com", "s", body)["raw"])
    msg = email.message_from_bytes(raw, policy=policy.default)
    for part in (msg.get_body(("html",)), msg.get_body(("plain",))):
        data = part.get_payload(decode=True)
        assert "😊".encode("utf-8") in data and "🧰".encode("utf-8") in data
        assert b"\xed\xa0" not in data and b"\xed\xb8" not in data  # 沒有代理字元（CESU-8）
        data.decode("utf-8")  # 嚴格解碼不出錯


def test_sender_display_name_and_signature_follow_region():
    from datetime import datetime
    from modules.service_fee_draft import build_mail_html

    assert sender.sender_display_name("台中區") == "檸檬家事服務財務-台中"
    assert "檸檬家事-台中" in build_mail_html("202610-2", "台中", datetime(2026, 11, 2, 14))


def test_sheet_progress_counts_sent_rows():
    ws = FakeSheet([_row("a@x.com", "甲", sent_at="2026/11/05 10:00:00"), _row("b@x.com", "乙")])
    assert sender.sheet_progress(ws, sender.FEE) == (1, 2)
    dep = FakeSheet([_dep_row("a@x.com", "林佳緯", sent_at="2026/11/05 10:00:00")])
    assert sender.sheet_progress(dep, sender.DEPOSIT) == (1, 1)
