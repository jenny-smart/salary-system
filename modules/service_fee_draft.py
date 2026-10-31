"""
承攬費通知信：統一更新各區 Gmail 範本草稿，並產生公告文字。

各區寄送承攬服務費時，是用各區 Gmail 帳號中主旨為「<<memo>>」的草稿當範本。
本模組依期別自動帶入服務期間、地區名稱，回覆期限由執行時手動輸入，
用 Gmail API 直接覆寫該草稿（草稿 ID 不變，寄信工具的設定不受影響）。

授權：每個地區帳號需各自授權一次 Gmail（scope：gmail.compose），
      token JSON 放在環境變數／Streamlit secret「GMAIL_TOKEN_<代碼>」，
      或本機 credentials/gmail_token_<代碼>.json。
      產生 token：python get_gmail_token.py <地區>
"""

from __future__ import annotations

import base64
import calendar
import datetime as dt
import html
import json
import os
import re
from email.mime.text import MIMEText
from pathlib import Path

from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials as UserCredentials


GMAIL_SCOPES = ["https://www.googleapis.com/auth/gmail.compose"]

PROJECT_ROOT = Path(__file__).resolve().parent.parent

# 地區 → (寄件 Gmail, secret 代碼)
REGION_MAILBOXES = {
    "台北": ("salary.lemontaipei@gmail.com", "TAIPEI"),
    "台中": ("salary.lemontaichung@gmail.com", "TAICHUNG"),
    "桃園": ("salary.lemontaoyuan@gmail.com", "TAOYUAN"),
    "新竹": ("salary.lemonhsinchu@gmail.com", "HSINCHU"),
    "高雄": ("salary.lemonkaohsuing@gmail.com", "KAOHSIUNG"),
}

TEMPLATE_SUBJECT = "<<memo>>"
WEEKDAYS = "一二三四五六日"
PERIOD_RE = re.compile(r"^(\d{4})(\d{2})-([12])$")


# ──────────────────────────────────────────────────────────────
# 文字內容
# ──────────────────────────────────────────────────────────────

def _region_key(region: str) -> str:
    return str(region or "").replace("區", "").strip()


def service_range(period: str) -> tuple[dt.date, dt.date]:
    """期別 → 服務期間（上半月 1~15 日；下半月 16 日~月底）。"""
    m = PERIOD_RE.fullmatch(str(period).strip())
    if not m:
        raise ValueError(f"期別格式錯誤：{period!r}，應為 YYYYMM-1 或 YYYYMM-2")
    year, month, half = int(m.group(1)), int(m.group(2)), m.group(3)
    if half == "1":
        return dt.date(year, month, 1), dt.date(year, month, 15)
    last = calendar.monthrange(year, month)[1]
    return dt.date(year, month, 16), dt.date(year, month, last)


def _range_text(period: str) -> str:
    start, end = service_range(period)
    return f"{start:%m/%d}-{end:%m/%d}"


def _deadline_text(deadline: dt.datetime) -> str:
    """2026/10/16(五) 14:00"""
    return f"{deadline:%Y/%m/%d}({WEEKDAYS[deadline.weekday()]}) {deadline:%H:%M}"


def build_mail_html(period: str, region: str, deadline: dt.datetime) -> str:
    """範本草稿內文（<< Name >>、<< link >> 由寄信工具代入）。"""
    start, _ = service_range(period)
    rng = _range_text(period)
    region_name = html.escape(_region_key(region))
    deadline_s = html.escape(_deadline_text(deadline))
    lines = [
        "&lt;&lt; Name &gt;&gt;&nbsp;&nbsp;&nbsp;您好！",
        "<br>",
        "這是您",
        f"{start.year}年{rng} 的承攬服務費，",
        "感謝您這個月的努力及付出，辛苦了",
        "<br>",
        "承攬服務費連結：&lt;&lt; link &gt;&gt;",
        "<br>",
        "有無問題，都麻煩回信告知喔，",
        f'<b><span style="color:#ff0000">回覆期限{deadline_s}前</span></b>',
        "如果未在時間內確認我們會依照薪資單上的金額發放薪資，謝謝～",
        "<br>",
        "《貼心小提醒》",
        "<b>場次到達門檻的工具包押金會另行通知發放</b>",
        "<b>因為它不是薪資，因此是另行發放。</b>",
        "<br>",
        f"檸檬家事-{region_name}",
    ]
    body = "".join(
        line if line == "<br>" else f"<div>{line}</div>" for line in lines
    )
    return f"<div dir=\"ltr\">{body}</div>"


def build_announcement(
    period: str,
    deadline: dt.datetime,
    announce_date: dt.date | None = None,
) -> str:
    """群組公告文字（寄出承攬服務費後張貼）。"""
    start, _ = service_range(period)
    rng = _range_text(period)
    announce_date = announce_date or dt.date.today()
    tomorrow = deadline.date() == announce_date + dt.timedelta(days=1)
    when = (
        f"{'明天 ' if tomorrow else ''}"
        f"{deadline.month}/{deadline.day}（{WEEKDAYS[deadline.weekday()]}）"
        f"{deadline:%H:%M}"
    )
    return "\n".join([
        f"{announce_date:%Y/%m/%d}  📢{start.year}/{rng}承攬服務費確認",
        "",
        f"{rng}承攬服務費已經寄出囉～",
        f"請大家在{when} 以前回覆。",
        "❌ 有問題請直接回信，勿再回報專區以免重工",
        "",
        "✅未在時間內回覆，會依承攬服務費上面金額發放",
        "✅沒有收到信件，先找找垃圾信箱",
        f"(主旨：{period} 檸檬家事｜清潔承攬服務費_姓名)，都沒有找到請至『專區』告知",
        "✅如計算有疑問，請一律『回覆信件說明問題』，我們將統一信件回覆處理🙏",
        "@All ",
        "再麻煩大家確認後回覆喔，謝謝大家",
    ])


# ──────────────────────────────────────────────────────────────
# Gmail
# ──────────────────────────────────────────────────────────────

def _streamlit_secret(name: str) -> str:
    try:
        import streamlit as st

        value = st.secrets.get(name, "")
        if isinstance(value, str):
            return value.strip()
        if value:
            return json.dumps(dict(value))
    except Exception:
        pass
    return ""


def _gmail_credentials(region: str) -> UserCredentials:
    key = _region_key(region)
    if key not in REGION_MAILBOXES:
        raise ValueError(f"【{region}】尚未設定寄件 Gmail")
    email, code = REGION_MAILBOXES[key]
    secret_name = f"GMAIL_TOKEN_{code}"
    local_path = PROJECT_ROOT / "credentials" / f"gmail_token_{code.lower()}.json"

    raw = os.environ.get(secret_name, "").strip() or _streamlit_secret(secret_name)
    if not raw and local_path.exists():
        raw = local_path.read_text(encoding="utf-8")
    if not raw:
        raise RuntimeError(
            f"【{key}】尚未授權 Gmail（{email}）：請以該帳號執行 "
            f"`python get_gmail_token.py {key}`，並把結果存成 secret「{secret_name}」"
        )
    try:
        info = json.loads(raw)
    except json.JSONDecodeError as exc:
        raise RuntimeError(f"{secret_name} 不是有效的 JSON：{exc}") from exc

    creds = UserCredentials.from_authorized_user_info(info, scopes=GMAIL_SCOPES)
    if not creds.valid:
        try:
            creds.refresh(Request())
        except Exception as exc:
            raise RuntimeError(
                f"【{key}】Gmail 授權已失效，請重新執行 get_gmail_token.py {key}：{exc}"
            ) from exc
    return creds


def _gmail_service(region: str):
    from googleapiclient.discovery import build

    return build("gmail", "v1", credentials=_gmail_credentials(region), cache_discovery=False)


def _header(message: dict, name: str) -> str:
    for h in (message.get("payload") or {}).get("headers", []):
        if str(h.get("name", "")).lower() == name.lower():
            return str(h.get("value", ""))
    return ""


def _find_template_draft(service) -> dict:
    """找主旨為 <<memo>> 的草稿；找不到或多於一份都中止。"""
    found = []
    page_token = None
    while True:
        resp = service.users().drafts().list(
            userId="me", q=f'subject:"{TEMPLATE_SUBJECT}"',
            pageToken=page_token, maxResults=100,
        ).execute(num_retries=3)
        for item in resp.get("drafts", []):
            draft = service.users().drafts().get(
                userId="me", id=item["id"], format="metadata",
                metadataHeaders=["Subject", "To"],
            ).execute(num_retries=3)
            if _header(draft.get("message", {}), "Subject").strip() == TEMPLATE_SUBJECT:
                found.append(draft)
        page_token = resp.get("nextPageToken")
        if not page_token:
            break
    if not found:
        raise RuntimeError(f"找不到主旨為「{TEMPLATE_SUBJECT}」的範本草稿")
    if len(found) > 1:
        raise RuntimeError(
            f"主旨為「{TEMPLATE_SUBJECT}」的草稿有 {len(found)} 份，請只保留一份範本"
        )
    return found[0]


def update_template_draft(
    period: str,
    region: str,
    deadline: dt.datetime,
    log=None,
) -> str:
    """覆寫該區 <<memo>> 範本草稿的內文，回傳草稿 ID。"""
    def _emit(msg):
        if log:
            log(msg)

    key = _region_key(region)
    email = REGION_MAILBOXES.get(key, ("", ""))[0]
    service = _gmail_service(region)
    draft = _find_template_draft(service)
    to = _header(draft.get("message", {}), "To")

    mime = MIMEText(build_mail_html(period, region, deadline), "html", "utf-8")
    mime["Subject"] = TEMPLATE_SUBJECT
    if to:
        mime["To"] = to
    raw = base64.urlsafe_b64encode(mime.as_bytes()).decode("ascii")

    service.users().drafts().update(
        userId="me", id=draft["id"],
        body={"id": draft["id"], "message": {"raw": raw}},
    ).execute(num_retries=3)
    _emit(
        f"✅ {key}範本草稿已更新（{email}）：服務期間 {_range_text(period)}，"
        f"回覆期限 {_deadline_text(deadline)}前"
    )
    return draft["id"]
