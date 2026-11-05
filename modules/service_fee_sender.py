"""承攬費通知信：用各區 Gmail 寄出，並回寫發送狀態。

各區「YYYY承攬服務費mail」的期別工作表（例如 202610-2）欄位：
  A Email Address｜B Name｜C link｜D 檔案名稱｜E 信件主旨
  F 發送時間｜G 狀態｜H 狀態時間｜I Gmail threadId｜J 追蹤碼｜K 點連結時間

狀態：已寄出 → 已點連結 → 已回覆／退信；寄送失敗會寫「失敗：原因」且 F 留空，下次會重寄。
F 有值的列不會重寄；寄之前也會查寄件備份，避免中斷後重跑造成重複寄出。
點連結：有設定中央 GAS 網址（GAS_SCHEDULER_WEB_APP_URL）時，信中連結改走
  {GAS}?action=click&m=mail試算表ID&s=期別&k=追蹤碼，GAS 記下 K 欄時間後轉到 C 欄連結。
開信：無法可靠得知（GAS 無法回傳追蹤圖片、各家信箱也會擋），不記錄。
"""
from __future__ import annotations

import html
import re
import secrets
import time
from urllib.parse import urlencode
from dataclasses import dataclass
from datetime import datetime
from email.message import EmailMessage
from email.utils import formataddr, parseaddr
from typing import Callable, Optional

import base64
import pytz

TAIPEI_TZ = pytz.timezone("Asia/Taipei")
STATUS_HEADERS = ["發送時間", "狀態", "狀態時間", "Gmail threadId", "追蹤碼", "點連結時間"]
SENT, CLICKED, REPLIED, BOUNCED = "已寄出", "已點連結", "已回覆", "退信"
LINK_TEXT = "點我查看承攬服務費"
EMAIL_RE = re.compile(r"^[^@\s]+@[^@\s]+\.[^@\s]+$")
BOUNCE_SENDERS = ("mailer-daemon", "postmaster")
FLUSH_EVERY = 10


def _emit(log, message: str) -> None:
    if callable(log):
        log(message)


def _now_text() -> str:
    return datetime.now(TAIPEI_TZ).strftime("%Y/%m/%d %H:%M:%S")


def _ms_text(ms) -> str:
    return datetime.fromtimestamp(int(ms) / 1000, TAIPEI_TZ).strftime("%Y/%m/%d %H:%M:%S")


def _cell(row: list, idx: int) -> str:
    return str(row[idx]).strip() if idx < len(row) and row[idx] is not None else ""


# ──────────────────────────────────────────────────────────────
# 信件內容
# ──────────────────────────────────────────────────────────────

def render_body(template_html: str, name: str, link: str, text: str = "") -> str:
    """把範本的 << Name >>、<< link >> 換成本人的姓名與連結（text 為連結顯示文字，預設顯示網址）。"""
    safe_link = html.escape(link, quote=True)
    body = re.sub(r"&lt;&lt;\s*Name\s*&gt;&gt;", html.escape(name), template_html)
    return re.sub(r"&lt;&lt;\s*link\s*&gt;&gt;",
                  f'<a href="{safe_link}">{html.escape(text or link)}</a>', body)


def tracking_url(gas_url: str, mail_id: str, period: str, token: str) -> str:
    return f"{gas_url}?{urlencode({'action': 'click', 'm': mail_id, 's': period, 'k': token})}"


def _row_body(template_html: str, r: dict, track: Optional[dict]) -> str:
    if not track:
        return render_body(template_html, r["name"], r["link"])
    url = tracking_url(track["gas_url"], track["mail_id"], track["period"], r["token"])
    return render_body(template_html, r["name"], url, LINK_TEXT)


def build_deposit_html(r: dict) -> str:
    """工具包押金通知信（同「<<MEMO>>」草稿）。"""
    e = html.escape
    lines = [
        f"{e(r['name'])}&nbsp;&nbsp;&nbsp;您好！", "<br>",
        f"目前場次已達{e(r['session'])}場，",
        f'我們將於 <b><span style="color:#ff0000">{e(r["date"])}</span></b> 匯款{e(r["money"])}元至您的帳戶',
        "謝謝您，辛苦了", "<br>", "<br>",
        "檸檬家事財務",
    ]
    return '<div dir="ltr">' + "".join(x if x == "<br>" else f"<div>{x}</div>" for x in lines) + "</div>"


def _html_to_text(body: str) -> str:
    text = re.sub(r"<br\s*/?>|</div>", "\n", body)
    text = re.sub(r"<a href=\"([^\"]*)\">([^<]*)</a>", lambda m: f"{m.group(2)}：{html.unescape(m.group(1))}"
                  if m.group(1) != m.group(2) else m.group(2), text)
    return html.unescape(re.sub(r"<[^>]+>", "", text)).strip()


def build_message(mailbox: str, sender_name: str, to: str, subject: str, body_html: str) -> dict:
    msg = EmailMessage()
    msg["From"] = formataddr((sender_name, mailbox))
    msg["To"] = to
    msg["Subject"] = subject
    msg.set_content(_html_to_text(body_html), charset="utf-8")
    msg.add_alternative(
        '<!DOCTYPE html><html><head><meta charset="utf-8"></head><body>' + body_html + "</body></html>",
        subtype="html", charset="utf-8")
    return {"raw": base64.urlsafe_b64encode(msg.as_bytes()).decode()}


# ──────────────────────────────────────────────────────────────
# 信件種類：承攬費通知信／工具包押金通知信
# ──────────────────────────────────────────────────────────────

def _col(letter: str) -> int:
    return ord(letter) - ord("A")


def _letter(idx: int) -> str:
    return chr(ord("A") + idx)


@dataclass(frozen=True)
class MailKind:
    label: str
    sheet_suffix: str      # 工作表名稱＝期別＋sheet_suffix
    subject_col: str       # 主旨欄
    status_col: str        # 狀態區塊起始欄（發送時間、狀態、狀態時間、threadId、追蹤碼、點連結時間）
    extra_cols: dict       # 其他欄位 → 欄名

    @property
    def last_col(self) -> str:
        return _letter(_col(self.status_col) + len(STATUS_HEADERS) - 1)


FEE = MailKind("承攬費通知信", "", "E", "F", {"link": "C", "file": "D"})
DEPOSIT = MailKind("工具包押金通知信", "工具包押金", "F", "G", {"session": "C", "date": "D", "money": "E"})


def read_rows(ws, kind: MailKind = FEE) -> list[dict]:
    values = ws.get(f"A2:{kind.last_col}", value_render_option="FORMATTED_VALUE") or []
    base = _col(kind.status_col)
    rows = []
    for i, r in enumerate(values, start=2):
        if not _cell(r, 1):  # B 欄姓名空白即略過
            continue
        row = {
            "row": i, "email": _cell(r, 0), "name": _cell(r, 1), "subject": _cell(r, _col(kind.subject_col)),
            "sent_at": _cell(r, base), "status": _cell(r, base + 1), "status_at": _cell(r, base + 2),
            "thread_id": _cell(r, base + 3), "token": _cell(r, base + 4), "clicked_at": _cell(r, base + 5),
        }
        row.update({k: _cell(r, _col(c)) for k, c in kind.extra_cols.items()})
        rows.append(row)
    return rows


class _StatusWriter:
    """累積狀態區塊（前 5 欄）的更新，每 FLUSH_EVERY 列寫一次，避免中斷後狀態遺失又不超過 Sheets 配額。"""

    def __init__(self, ws, kind: MailKind = FEE):
        self.ws, self.kind, self.pending = ws, kind, []

    def set(self, row: int, sent_at: str, status: str, status_at: str, thread_id: str, token: str = ""):
        end = _letter(_col(self.kind.status_col) + 4)
        self.pending.append({"range": f"{self.kind.status_col}{row}:{end}{row}",
                             "values": [[sent_at, status, status_at, thread_id, token]]})
        if len(self.pending) >= FLUSH_EVERY:
            self.flush()

    def flush(self):
        if self.pending:
            self.ws.batch_update(self.pending, value_input_option="RAW")
            self.pending = []


def ensure_headers(ws, kind: MailKind = FEE) -> None:
    ws.update(range_name=f"{kind.status_col}1:{kind.last_col}1", values=[STATUS_HEADERS],
              value_input_option="RAW")


# ──────────────────────────────────────────────────────────────
# 寄前核對：連結、主旨、姓名是同一人
# ──────────────────────────────────────────────────────────────

def _strip_pdf(name: str) -> str:
    return re.sub(r"(?i)\.pdf$", "", str(name or "").strip())


def check_same_person(r: dict, kind: MailKind, period: str = "",
                      link_file_name: Optional[Callable[[str], str]] = None) -> str:
    """回傳問題說明；空字串表示主旨、檔名、連結都是 B 欄這個人。"""
    name = r["name"]
    if kind is DEPOSIT:
        return "" if name in r["subject"] else f"主旨不是{name}（{r['subject']}）"
    if not r["subject"].endswith(f"_{name}"):
        return f"主旨不是{name}（{r['subject']}）"
    if period and not r["subject"].startswith(f"{period} "):
        return f"主旨不是本期（{r['subject']}）"
    if r.get("file") and not _strip_pdf(r["file"]).endswith(f"_{name}"):
        return f"D 欄檔名不是{name}（{r['file']}）"
    if link_file_name is not None:
        try:
            actual = _strip_pdf(link_file_name(r["link"]))
        except Exception as exc:
            return f"無法讀取連結檔名：{exc}"
        if not actual.endswith(f"_{name}"):
            return f"連結檔案不是{name}（{actual}）"
        if period and not actual.startswith(f"{period} "):
            return f"連結檔案不是本期（{actual}）"
    return ""


# ──────────────────────────────────────────────────────────────
# Gmail
# ──────────────────────────────────────────────────────────────

def _header(msg: dict, name: str) -> str:
    for h in msg.get("payload", {}).get("headers", []):
        if h.get("name", "").lower() == name.lower():
            return h.get("value", "")
    return ""


def find_already_sent(gmail, to: str, subject: str) -> Optional[dict]:
    """寄件備份裡已有同收件人＋同主旨的信，回傳該信（避免重複寄）。"""
    q = f'in:sent to:{to} subject:"{subject}"'
    found = gmail.users().messages().list(userId="me", q=q, maxResults=5).execute().get("messages", [])
    for item in found:
        msg = gmail.users().messages().get(userId="me", id=item["id"], format="metadata",
                                           metadataHeaders=["Subject"]).execute()
        if _header(msg, "Subject").strip() == subject:
            return msg
    return None


def send_mails(
    gmail,
    ws,
    mailbox: str,
    sender_name: str,
    body_for: Callable[[dict, bool], str],
    kind: MailKind = FEE,
    period: str = "",
    log: Optional[Callable[[str], None]] = None,
    test_to: str = "",
    pause: float = 0.3,
    track: bool = False,
    link_file_name: Optional[Callable[[str], str]] = None,
) -> dict:
    """寄出發送時間空白的列。body_for(row, tracked) 產生內文。

    寄任何一封之前，先核對每列主旨／檔名／連結都是同一人；有任何一列不符就整批不寄。
    test_to 有值時只寄第一封到該信箱、不回寫試算表。"""
    rows = read_rows(ws, kind)
    todo = [r for r in rows if not r["sent_at"]]
    result = {"sent": 0, "skipped": len(rows) - len(todo), "failed": [], "total": len(rows)}

    if test_to:
        if not rows:
            raise RuntimeError("工作表沒有可寄送的資料")
        r = (todo or rows)[0]
        problem = check_same_person(r, kind, period, link_file_name)
        if problem:
            raise RuntimeError(f"{r['name']}：{problem}")
        gmail.users().messages().send(userId="me", body=build_message(
            mailbox, sender_name, test_to, "【測試】" + r["subject"], body_for(r, False))).execute()
        _emit(log, f"✅ 測試信已寄到 {test_to}（內容為 {r['name']}）")
        return result

    if result["skipped"]:
        _emit(log, f"已寄出過 {result['skipped']} 封，不重寄")
    ensure_headers(ws, kind)
    writer = _StatusWriter(ws, kind)

    # ── 寄前核對：資料不完整或不是同一人，整批不寄 ──
    mismatched = []
    for r in todo:
        problem = ("無 Email" if not EMAIL_RE.match(r["email"]) else
                   "無主旨" if not r["subject"] else
                   "無連結" if kind is FEE and not r.get("link") else
                   check_same_person(r, kind, period, link_file_name))
        if problem:
            writer.set(r["row"], "", f"失敗：{problem}", _now_text(), "", r["token"])
            mismatched.append(f"{r['name']}（{problem}）")
    if mismatched:
        writer.flush()
        for item in mismatched:
            _emit(log, f"❌ 核對不符：{item}")
        raise RuntimeError(f"{len(mismatched)} 列核對不符，整批未寄出，請修正後重新執行")
    _emit(log, f"核對完成：{len(todo)} 封的主旨與{'連結檔案' if kind is FEE else '姓名'}都是同一人")

    # 先一次寫入所有追蹤碼再寄，收件人一收到就點也找得到這一列
    for r in todo:
        r["token"] = r["token"] or secrets.token_hex(8)
    token_col = _letter(_col(kind.status_col) + 4)
    if todo and track:
        ws.batch_update([{"range": f"{token_col}{r['row']}", "values": [[r["token"]]]} for r in todo],
                        value_input_option="RAW")
    try:
        for r in todo:
            try:
                existing = find_already_sent(gmail, r["email"], r["subject"])
                if existing:
                    t = _ms_text(existing["internalDate"])
                    writer.set(r["row"], t, SENT, t, existing.get("threadId", ""), r["token"])
                    _emit(log, f"⚠️ {r['name']} 先前已寄出（{t}），只補記錄不重寄")
                    result["skipped"] += 1
                    continue
                sent = gmail.users().messages().send(userId="me", body=build_message(
                    mailbox, sender_name, r["email"], r["subject"], body_for(r, track))).execute()
                t = _now_text()
                writer.set(r["row"], t, SENT, t, sent.get("threadId", ""), r["token"])
                result["sent"] += 1
                if pause:
                    time.sleep(pause)
            except Exception as exc:
                writer.set(r["row"], "", f"失敗：{str(exc)[:200]}", _now_text(), "", r["token"])
                result["failed"].append(f"{r['name']}（{exc}）")
    finally:
        writer.flush()

    _emit(log, f"{'✅' if not result['failed'] else '⚠️'} {kind.label}：寄出 {result['sent']} 封，"
               f"略過 {result['skipped']} 封，失敗 {len(result['failed'])} 封")
    for item in result["failed"]:
        _emit(log, f"❌ 寄送失敗：{item}")
    return result


def send_service_fee_mails(gmail, ws, mailbox: str, sender_name: str, template_html: str,
                           log=None, test_to: str = "", pause: float = 0.3,
                           track: Optional[dict] = None, period: str = "",
                           link_file_name: Optional[Callable[[str], str]] = None) -> dict:
    """承攬費通知信。track＝{"gas_url", "mail_id", "period"} 時，信中連結改走 GAS 追蹤點擊。"""
    def body_for(r, tracked):
        return _row_body(template_html, r, track if tracked else None)

    return send_mails(gmail, ws, mailbox, sender_name, body_for, FEE, period or (track or {}).get("period", ""),
                      log=log, test_to=test_to, pause=pause, track=bool(track), link_file_name=link_file_name)


def _bounced_addresses(gmail, after: str) -> dict:
    """退信通知（mailer-daemon）→ {收件人小寫: 時間}。after 為 YYYY/MM/DD。"""
    q = f"from:({' OR '.join(BOUNCE_SENDERS)}) after:{after}"
    out = {}
    for item in gmail.users().messages().list(userId="me", q=q, maxResults=200).execute().get("messages", []):
        msg = gmail.users().messages().get(userId="me", id=item["id"], format="metadata",
                                           metadataHeaders=["X-Failed-Recipients"]).execute()
        for addr in _header(msg, "X-Failed-Recipients").split(","):
            if addr.strip():
                out[addr.strip().lower()] = _ms_text(msg["internalDate"])
    return out


def refresh_statuses(gmail, ws, mailbox: str, log: Optional[Callable[[str], None]] = None,
                     kind: MailKind = FEE) -> dict:
    """依 threadId 檢查回覆與退信，更新狀態與狀態時間。"""
    rows = [r for r in read_rows(ws, kind) if r["thread_id"]]
    counts = {SENT: 0, CLICKED: 0, REPLIED: 0, BOUNCED: 0}
    if not rows:
        _emit(log, f"⚠️ {kind.label}沒有已寄出的信，無狀態可更新")
        return counts

    earliest = min(r["sent_at"] for r in rows)[:10] or datetime.now(TAIPEI_TZ).strftime("%Y/%m/%d")
    bounced = _bounced_addresses(gmail, earliest)
    me = mailbox.lower()
    writer = _StatusWriter(ws, kind)
    try:
        for r in rows:
            status, status_at = (CLICKED, r["clicked_at"]) if r["clicked_at"] else (SENT, r["sent_at"])
            thread = gmail.users().threads().get(userId="me", id=r["thread_id"], format="metadata",
                                                 metadataHeaders=["From"]).execute()
            for msg in thread.get("messages", []):
                sender = parseaddr(_header(msg, "From"))[1].lower()
                if any(s in sender for s in BOUNCE_SENDERS):
                    status, status_at = BOUNCED, _ms_text(msg["internalDate"])
                    break
                if sender and sender != me:
                    status, status_at = REPLIED, _ms_text(msg["internalDate"])  # 取最後一封回覆時間
            if status in (SENT, CLICKED) and r["email"].lower() in bounced:
                status, status_at = BOUNCED, bounced[r["email"].lower()]
            counts[status] += 1
            if (status, status_at) != (r["status"], r["status_at"]):
                writer.set(r["row"], r["sent_at"], status, status_at, r["thread_id"], r["token"])
    finally:
        writer.flush()

    _emit(log, f"✅ {kind.label}狀態更新：已寄出（未點未回）{counts[SENT]}、已點連結 {counts[CLICKED]}、"
               f"已回覆 {counts[REPLIED]}、退信 {counts[BOUNCED]}")
    return counts


# ──────────────────────────────────────────────────────────────
# 主流程（salaryapp 呼叫）
# ──────────────────────────────────────────────────────────────

def _open_mail_book(region: str, period: str, mail_id: str = ""):
    from modules.auth import get_gspread_client, get_jenny_gspread_client
    from modules.service_fee_mail import _region_ids
    from modules.year_root import cfg_for_period

    cfg = dict(_region_ids(get_gspread_client(), region))
    if str(mail_id or "").strip():
        cfg["mail_id"] = str(mail_id).strip()
    mail_id = str(cfg_for_period(cfg, period).get("mail_id", "") or "").strip()
    if not mail_id:
        raise ValueError(f"【{region}】地區設定 mail_id 為空")
    return get_jenny_gspread_client().open_by_key(mail_id), mail_id


def _mailbox(region: str) -> str:
    from modules.service_fee_draft import region_mailbox

    mailbox = region_mailbox(region)
    if not mailbox:
        raise ValueError(f"【{region}】沒有設定寄件 Gmail（service_fee_draft.REGION_MAILBOXES）")
    return mailbox


def sender_display_name(region: str) -> str:
    """寄件人顯示名稱，沿用各區信箱原本的「檸檬家事服務財務-地區」。"""
    return f"檸檬家事服務財務-{region.replace('區', '').strip()}"


def _drive_name_lookup():
    from modules.auth import get_jenny_drive_service
    from modules.service_fee_mail import _drive_file_name

    drive = get_jenny_drive_service()
    return lambda link: _drive_file_name(drive, link)


def run_send(region: str, period: str, deadline: datetime, mail_id: str = "",
             log=None, test_to: str = "", gas_url: str = "") -> dict:
    from modules.auth import get_gmail_service
    from modules.service_fee_draft import build_mail_html

    mailbox = _mailbox(region)
    book, mail_id = _open_mail_book(region, period, mail_id)
    sender_name = sender_display_name(region)
    track = {"gas_url": gas_url, "mail_id": mail_id, "period": period} if gas_url else None
    _emit(log, f"▶ 承攬費寄信 {region} {period}：寄件 {mailbox}"
               + ("（追蹤點連結）" if track else "（未設定 GAS 網址，不追蹤點連結）"))
    return send_service_fee_mails(get_gmail_service(mailbox), book.worksheet(period), mailbox, sender_name,
                                  build_mail_html(period, region, deadline), log=log, test_to=test_to,
                                  track=track, period=period, link_file_name=_drive_name_lookup())


def run_send_deposit(region: str, period: str, mail_id: str = "", log=None, test_to: str = "") -> dict:
    import gspread
    from modules.auth import get_gmail_service

    mailbox = _mailbox(region)
    book, _ = _open_mail_book(region, period, mail_id)
    try:
        ws = book.worksheet(period + DEPOSIT.sheet_suffix)
    except gspread.WorksheetNotFound:
        _emit(log, f"⚠️ 沒有「{period}{DEPOSIT.sheet_suffix}」工作表（工具包押金只在下半月），不寄信")
        return {"sent": 0, "skipped": 0, "failed": [], "total": 0}
    _emit(log, f"▶ 工具包押金寄信 {region} {period}：寄件 {mailbox}")
    return send_mails(get_gmail_service(mailbox), ws, mailbox, sender_display_name(region),
                      lambda r, _tracked: build_deposit_html(r), DEPOSIT, period, log=log, test_to=test_to)


def run_refresh(region: str, period: str, mail_id: str = "", log=None) -> dict:
    import gspread
    from modules.auth import get_gmail_service

    mailbox = _mailbox(region)
    gmail = get_gmail_service(mailbox)
    book, _ = _open_mail_book(region, period, mail_id)
    counts = refresh_statuses(gmail, book.worksheet(period), mailbox, log=log, kind=FEE)
    try:
        refresh_statuses(gmail, book.worksheet(period + DEPOSIT.sheet_suffix), mailbox, log=log, kind=DEPOSIT)
    except gspread.WorksheetNotFound:
        pass
    return counts
