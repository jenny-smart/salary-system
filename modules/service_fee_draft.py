"""
承攬費通知信：產生各區範本草稿內文與群組公告文字（由使用者自行貼上）。

各區寄送承攬服務費時，是用各區 Gmail 帳號中主旨為「<<memo>>」的草稿當範本。
本模組依期別自動帶入服務期間、地區名稱，回覆期限由執行時手動輸入。
"""

from __future__ import annotations

import calendar
import datetime as dt
import html
import re


# 各區寄件 Gmail（範本草稿所在帳號）
REGION_MAILBOXES = {
    "台北": "salary.lemontaipei@gmail.com",
    "台中": "salary.lemontaichung@gmail.com",
    "桃園": "salary.lemontaoyuan@gmail.com",
    "新竹": "salary.lemonhsinchu@gmail.com",
    "高雄": "salary.lemonkaohsuing@gmail.com",
}
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
        "&lt;&lt; Name &gt;&gt;&nbsp;&nbsp;&nbsp;您好！😊",
        "<br>",
        f"這是您 {start.year}年{rng} 的承攬服務費 💰",
        "感謝您這個月的努力及付出，辛苦了 🙏✨",
        "<br>",
        "📄 承攬服務費連結：&lt;&lt; link &gt;&gt;",
        "<br>",
        "有無問題，都麻煩回信告知喔 📩",
        f'<b><span style="color:#ff0000">⏰ 回覆期限 {deadline_s}前</span></b>',
        "如果未在時間內確認，我們會依照薪資單上的金額發放薪資，謝謝～",
        "<br>",
        "💡《貼心小提醒》",
        "<b>🧰 場次到達門檻的工具包押金會另行通知發放</b>",
        "<b>因為它不是承攬費，因此是另行發放</b>",
        "<br>",
        f"🍋 檸檬家事-{region_name}",
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
        f"{announce_date:%Y/%m/%d}  📢 {start.year}/{rng} 承攬服務費確認",
        "",
        f"💌 {rng} 承攬服務費已經寄出囉～",
        f"⏰ 請大家在{when} 以前回覆。",
        "❌ 有問題請直接回信，勿再回報專區以免重工",
        "",
        "✅ 未在時間內回覆，會依承攬服務費上面金額發放",
        "✅ 沒有收到信件，先找找垃圾信箱 🔍",
        f"   (主旨：{period} 檸檬家事｜清潔承攬服務費_姓名)，都沒有找到請至『專區』告知",
        "✅ 如計算有疑問，請一律『回覆信件說明問題』，我們將統一信件回覆處理 🙏",
        "@All",
        "再麻煩大家確認後回覆喔，謝謝大家 💛",
    ])


def region_mailbox(region: str) -> str:
    return REGION_MAILBOXES.get(_region_key(region), "")
