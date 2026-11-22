"""
即時時數及承攬費試算（不讀寫期別檔）。

規則照薪資表 L 欄範本（cleaning_process_1._ensure_salary_template）：
  - 每張訂單，服務專員欄有出現的人，各記該單「服務時數」。
  - 晚間：服務時間為 19:00-21:00／19:00-22:00／19:00-23:00；單價 = 平日 + 20。
  - 週末：星期六、日且非晚間；單價 = 00調薪 D 欄（調整後週末）。
  - 平日：週一～五且非晚間；單價 = 00調薪 C 欄（調整後平日）。
  - 大掃除1：大掃除1開始 ≤ 日期 < 大掃除2開始；大掃除2：大掃除2開始 ≤ 日期 ≤ 大掃除最後一天；
    各分平日／晚間／週末，單價用 00調薪 J:O。
  - 實習：實習場在訂單裡也有，但另以每小時 200 給付；依實習日期從同條件的平日／週末時數扣除
    （實習不排晚間）。
  - 實境：不在訂單裡，每小時 200 另計。
只算服務承攬費；扣款、獎金、津貼、車馬費等仍以結算為準。
"""
from __future__ import annotations

from dataclasses import dataclass, field
from datetime import date, datetime
from typing import Dict, Iterable, List, Optional, Tuple

EVENING_SLOTS = {"19:00-21:00", "19:00-22:00", "19:00-23:00"}
TRAINEE_RATE = 200          # 實境、實習每小時
EVENING_EXTRA = 20          # 晚間 = 平日 + 20

CATEGORIES = (
    "平日", "晚間", "週末",
    "大掃除1平日", "大掃除1晚間", "大掃除1週末",
    "大掃除2平日", "大掃除2晚間", "大掃除2週末",
)


def parse_date(value) -> Optional[date]:
    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, date):
        return value
    text = str(value or "").strip().replace("-", "/")
    if not text:
        return None
    try:
        y, m, d = (int(x) for x in text.split("/")[:3])
        return date(y, m, d)
    except (ValueError, TypeError):
        return None


def parse_number(value) -> float:
    try:
        return float(str(value).replace(",", "").strip() or 0)
    except ValueError:
        return 0.0


def split_staff(text: str) -> List[str]:
    """「A X B X C」→ [A, B, C]"""
    import re
    return [p.strip() for p in re.split(r"\s*[Xx×＊*、,，/／]\s*", str(text or "")) if p.strip()]


@dataclass
class Rates:
    weekday: float = 0
    weekend: float = 0
    cleanup: Tuple[float, float, float, float, float, float] = (0, 0, 0, 0, 0, 0)
    # cleanup = 大掃除I 平日/週末/晚間、大掃除II 平日/週末/晚間（00調薪 J:O 順序）

    def for_category(self, cat: str) -> float:
        if cat == "平日":
            return self.weekday
        if cat == "晚間":
            return self.weekday + EVENING_EXTRA
        if cat == "週末":
            return self.weekend
        j, k, l, m, n, o = self.cleanup
        return {
            "大掃除1平日": j, "大掃除1週末": k, "大掃除1晚間": l,
            "大掃除2平日": m, "大掃除2週末": n, "大掃除2晚間": o,
        }[cat]


@dataclass
class CleanupDates:
    start1: Optional[date] = None   # B2006
    start2: Optional[date] = None   # B2007
    end: Optional[date] = None      # B2008

    def phase(self, d: date) -> int:
        """0＝一般、1＝大掃除1、2＝大掃除2"""
        if self.start1 and self.start2 and self.start1 <= d < self.start2:
            return 1
        if self.start2 and self.end and self.start2 <= d <= self.end:
            return 2
        return 0


def classify(d: date, slot: str, cleanup: CleanupDates) -> str:
    evening = str(slot or "").strip() in EVENING_SLOTS
    weekend = d.weekday() >= 5
    kind = "晚間" if evening else ("週末" if weekend else "平日")
    phase = cleanup.phase(d)
    return kind if phase == 0 else f"大掃除{phase}{kind}"


@dataclass
class Session:
    date: date
    slot: str
    customer: str
    hours: float
    category: str
    staff: str
    order_no: str = ""
    note: str = ""


@dataclass
class Estimate:
    name: str
    hours: Dict[str, float] = field(default_factory=lambda: {c: 0.0 for c in CATEGORIES})
    sessions: List[Session] = field(default_factory=list)
    trainee_hours: float = 0.0     # 實境
    intern_hours: float = 0.0      # 實習
    rates: Optional[Rates] = None
    warnings: List[str] = field(default_factory=list)

    @property
    def session_count(self) -> int:
        return len(self.sessions)

    def amount(self) -> float:
        total = (self.trainee_hours + self.intern_hours) * TRAINEE_RATE
        if self.rates:
            total += sum(h * self.rates.for_category(c) for c, h in self.hours.items())
        return total


def build_estimates(
    orders: Iterable[dict],
    rates: Dict[str, Rates],
    interns: Iterable[dict] = (),
    trainees: Iterable[dict] = (),
    cleanup: CleanupDates = CleanupDates(),
    start: Optional[date] = None,
    end: Optional[date] = None,
    adjustments: Optional[Dict[Tuple[str, str], float]] = None,
) -> Dict[str, Estimate]:
    """
    orders：{date, slot, customer, staff, hours, order_no?, cancelled?}
    interns：{name, date, hours}（新人實習：C 新人姓名、E 實習日期、K 時數）
    trainees：{name, date, hours}（新人實境：C、E、K）
    adjustments：{(訂單編號, 專員): 實際時數}，有的話取代該專員在該單的時數。
    start/end：訂單只算這段日期（含頭尾）；None 表示不限。實習／實境不再篩日期。
    """
    adjustments = adjustments or {}
    result: Dict[str, Estimate] = {}

    def est(name: str) -> Estimate:
        if name not in result:
            result[name] = Estimate(name=name, rates=rates.get(name))
        return result[name]

    def in_range(d: Optional[date]) -> bool:
        return d is not None and (start is None or d >= start) and (end is None or d <= end)

    for o in orders:
        if o.get("cancelled"):
            continue
        d = parse_date(o.get("date"))
        if not in_range(d):
            continue
        hours = parse_number(o.get("hours"))
        cat = classify(d, o.get("slot", ""), cleanup)
        order_no = str(o.get("order_no") or "")
        for name in split_staff(o.get("staff", "")):
            h, note = hours, ""
            key = (order_no, name)
            if order_no and key in adjustments:
                h = adjustments[key]
                if h != hours:
                    note = f"原{hours:g}→實際{h:g}小時"
            e = est(name)
            if name not in rates and "調薪資料找不到單價" not in e.warnings:
                e.warnings.append("調薪資料找不到單價")
            e.hours[cat] += h
            e.sessions.append(Session(d, o.get("slot", ""), o.get("customer", ""), h, cat,
                                      o.get("staff", ""), order_no, note))

    # 實習／實境由呼叫端先篩好本期的列（期別檔是依「期別」欄歸期，不一定照日期）
    for row in interns:
        d = parse_date(row.get("date"))
        if d is None:
            continue
        h = parse_number(row.get("hours"))
        e = est(row["name"])
        e.intern_hours += h
        if not in_range(d):
            continue   # 實習日不在本期訂單範圍（例如上期未付補發）：只計 200，不扣訂單時數
        # 實習不排晚間：從同日條件的平日／週末扣除
        cat = classify(d, "", cleanup)
        e.hours[cat] -= h
        if e.hours[cat] < 0:
            e.warnings.append(f"{d:%m/%d} 實習 {h:g} 小時在訂單找不到相同條件的時數")
            e.hours[cat] = 0.0

    for row in trainees:
        est(row["name"]).trainee_hours += parse_number(row.get("hours"))

    return result
