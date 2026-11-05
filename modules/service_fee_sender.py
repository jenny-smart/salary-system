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
    msg.set_content(_html_to_text(body_html))
    msg.add_alternative(body_html, subtype="html")
    return {"raw": base64.urlsafe_b64encode(msg.as_bytes()).decode()}


# ──────────────────────────────────────────────────────────────
# 試算表讀寫
# ──────────────────────────────────────────────────────────────

def read_rows(ws) -> list[dict]:
    values = ws.get("A2:K", value_render_option="FORMATTED_VALUE") or []
    rows = []
    for i, r in enumerate(values, start=2):
        if not _cell(r, 1):  # B 欄姓名空白即略過
            continue
        rows.append({
            "row": i, "email": _cell(r, 0), "name": _cell(r, 1), "link": _cell(r, 2),
            "file": _cell(r, 3), "subject": _cell(r, 4), "sent_at": _cell(r, 5),
            "status": _cell(r, 6), "status_at": _cell(r, 7), "thread_id": _cell(r, 8),
            "token": _cell(r, 9), "clicked_at": _cell(r, 10),
        })
    return rows


class _StatusWriter:
    """累積 F:I 的更新，每 FLUSH_EVERY 列寫一次，避免中斷後狀態遺失又不超過 Sheets 配額。"""

    def __init__(self, ws):
        self.ws, self.pending = ws, []

    def set(self, row: int, sent_at: str, status: str, status_at: str, thread_id: str, token: str = ""):
        self.pending.append({"range": f"F{row}:J{row}", "values": [[sent_at, status, status_at, thread_id, token]]})
        if len(self.pending) >= FLUSH_EVERY:
            self.flush()

    def flush(self):
        if self.pending:
            self.ws.batch_update(self.pending, value_input_option="RAW")
            self.pending = []


def ensure_headers(ws) -> None:
    ws.update(range_name="F1:K1", values=[STATUS_HEADERS], value_input_option="RAW")


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


def send_service_fee_mails(
    gmail,
    ws,
    mailbox: str,
    sender_name: str,
    template_html: str,
    log: Optional[Callable[[str], None]] = None,
    test_to: str = "",
    pause: float = 0.3,
    track: Optional[dict] = None,
) -> dict:
    """寄出 F 欄（發送時間）空白的列。test_to 有值時只寄第一封到該信箱、不回寫試算表。
    track＝{"gas_url", "mail_id", "period"} 時，信中連結改走 GAS 追蹤點擊。"""
    rows = read_rows(ws)
    todo = [r for r in rows if not r["sent_at"]]
    result = {"sent": 0, "skipped": len(rows) - len(todo), "failed": [], "total": len(rows)}

    if test_to:
        if not todo and not rows:
            raise RuntimeError("工作表沒有可寄送的資料")
        r = (todo or rows)[0]  # 測試信直接放原連結，避免點擊被記到專員那一列
        gmail.users().messages().send(userId="me", body=build_message(
            mailbox, sender_name, test_to, "【測試】" + r["subject"],
            _row_body(template_html, r, None))).execute()
        _emit(log, f"✅ 測試信已寄到 {test_to}（內容為 {r['name']}）")
        return result

    if result["skipped"]:
        _emit(log, f"已寄出過 {result['skipped']} 封，不重寄")
    ensure_headers(ws)
    # 先一次寫入所有追蹤碼再寄，收件人一收到就點也找得到這一列
    for r in todo:
        r["token"] = r["token"] or secrets.token_hex(8)
    if todo and track:
        ws.batch_update([{"range": f"J{r['row']}", "values": [[r["token"]]]} for r in todo],
                        value_input_option="RAW")
    writer = _StatusWriter(ws)
    try:
        for r in todo:
            problem = ("無 Email" if not EMAIL_RE.match(r["email"]) else
                       "無主旨" if not r["subject"] else
                       "無連結" if not r["link"] else "")
            if problem:
                writer.set(r["row"], "", f"失敗：{problem}", _now_text(), "", r["token"])
                result["failed"].append(f"{r['name']}（{problem}）")
                continue
            try:
                existing = find_already_sent(gmail, r["email"], r["subject"])
                if existing:
                    t = _ms_text(existing["internalDate"])
                    writer.set(r["row"], t, SENT, t, existing.get("threadId", ""), r["token"])
                    _emit(log, f"⚠️ {r['name']} 先前已寄出（{t}），只補記錄不重寄")
                    result["skipped"] += 1
                    continue
                sent = gmail.users().messages().send(userId="me", body=build_message(
                    mailbox, sender_name, r["email"], r["subject"],
                    _row_body(template_html, r, track))).execute()
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

    _emit(log, f"{'✅' if not result['failed'] else '⚠️'} 寄出 {result['sent']} 封，"
               f"略過 {result['skipped']} 封，失敗 {len(result['failed'])} 封")
    for item in result["failed"]:
        _emit(log, f"❌ 寄送失敗：{item}")
    return result


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


def refresh_statuses(gmail, ws, mailbox: str, log: Optional[Callable[[str], None]] = None) -> dict:
    """依 threadId 檢查回覆與退信，更新 G、H 欄。"""
    rows = [r for r in read_rows(ws) if r["thread_id"]]
    counts = {SENT: 0, CLICKED: 0, REPLIED: 0, BOUNCED: 0}
    if not rows:
        _emit(log, "⚠️ 沒有已寄出的信（I 欄 threadId 空白），無狀態可更新")
        return counts

    earliest = min(r["sent_at"] for r in rows)[:10] or datetime.now(TAIPEI_TZ).strftime("%Y/%m/%d")
    bounced = _bounced_addresses(gmail, earliest)
    me = mailbox.lower()
    writer = _StatusWriter(ws)
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

    _emit(log, f"✅ 狀態更新：已寄出（未點未回）{counts[SENT]}、已點連結 {counts[CLICKED]}、"
               f"已回覆 {counts[REPLIED]}、退信 {counts[BOUNCED]}")
    return counts


# ──────────────────────────────────────────────────────────────
# 主流程（salaryapp 呼叫）
# ──────────────────────────────────────────────────────────────

def _open_period_sheet(region: str, period: str, mail_id: str = ""):
    from modules.auth import get_gspread_client, get_jenny_gspread_client
    from modules.service_fee_mail import _region_ids
    from modules.year_root import cfg_for_period

    cfg = dict(_region_ids(get_gspread_client(), region))
    if str(mail_id or "").strip():
        cfg["mail_id"] = str(mail_id).strip()
    mail_id = str(cfg_for_period(cfg, period).get("mail_id", "") or "").strip()
    if not mail_id:
        raise ValueError(f"【{region}】地區設定 mail_id 為空")
    return get_jenny_gspread_client().open_by_key(mail_id).worksheet(period), mail_id


def _mailbox(region: str) -> str:
    from modules.service_fee_draft import region_mailbox

    mailbox = region_mailbox(region)
    if not mailbox:
        raise ValueError(f"【{region}】沒有設定寄件 Gmail（service_fee_draft.REGION_MAILBOXES）")
    return mailbox


def run_send(region: str, period: str, deadline: datetime, mail_id: str = "",
             log=None, test_to: str = "", gas_url: str = "") -> dict:
    from modules.auth import get_gmail_service
    from modules.service_fee_draft import build_mail_html

    mailbox = _mailbox(region)
    ws, mail_id = _open_period_sheet(region, period, mail_id)
    sender_name = f"檸檬家事-{region.replace('區', '').strip()}"
    track = {"gas_url": gas_url, "mail_id": mail_id, "period": period} if gas_url else None
    _emit(log, f"▶ 承攬費寄信 {region} {period}：寄件 {mailbox}"
               + ("（追蹤點連結）" if track else "（未設定 GAS 網址，不追蹤點連結）"))
    return send_service_fee_mails(get_gmail_service(mailbox), ws, mailbox, sender_name,
                                  build_mail_html(period, region, deadline), log=log, test_to=test_to,
                                  track=track)


def run_refresh(region: str, period: str, mail_id: str = "", log=None) -> dict:
    from modules.auth import get_gmail_service

    mailbox = _mailbox(region)
    return refresh_statuses(get_gmail_service(mailbox), _open_period_sheet(region, period, mail_id)[0],
                            mailbox, log=log)
