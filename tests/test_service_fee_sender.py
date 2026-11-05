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
            r += [""] * (11 - len(r))
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


def _row(email_addr, name, subject="202610-2 檸檬家事｜清潔承攬服務費_X", sent_at=""):
    return [email_addr, name, "https://drive.google.com/file/d/abc/view", "202610-2 檔", subject, sent_at, "", "", "",
            "", ""]


def test_render_body_fills_name_and_link():
    body = sender.render_body(TEMPLATE, "蔡立娟", "https://x.y/a?b=1&c=2")
    assert "蔡立娟&nbsp;您好" in body and '<a href="https://x.y/a?b=1&amp;c=2">' in body


def test_sends_only_unsent_rows_and_records_status():
    ws = FakeSheet([_row("a@x.com", "甲"), _row("b@x.com", "乙", sent_at="2026/11/01 10:00:00"),
                    _row("bad", "丙")])
    gmail = FakeGmail()
    result = sender.send_service_fee_mails(gmail, ws, MAILBOX, "檸檬家事-台北", TEMPLATE, pause=0)
    assert result["sent"] == 1 and result["skipped"] == 1 and len(result["failed"]) == 1
    msg = gmail.sent[0]
    assert msg["To"] == "a@x.com" and msg["Subject"] == "202610-2 檸檬家事｜清潔承攬服務費_X"
    assert "甲" in msg.get_body(("html",)).get_content()
    assert ws.rows[0][6] == sender.SENT and ws.rows[0][8] == "t1" and ws.rows[0][5]
    assert ws.rows[2][6] == "失敗：無 Email" and ws.rows[2][5] == ""


def test_does_not_resend_when_found_in_sent_folder():
    old = {"internalDate": "1762300000000", "threadId": "told",
           "payload": {"headers": [{"name": "Subject", "value": "202610-2 檸檬家事｜清潔承攬服務費_X"}]}}
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


def test_resync_keeps_sent_status_by_name_and_file():
    from modules import service_fee_mail

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
