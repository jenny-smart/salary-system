"""
即時試算執行：讀各區來源 → live_estimate 計算 → 寫回 LemonSalarySystem
「即時試算-{地區}」「即時試算明細-{地區}」（每次整份覆寫，不動期別檔）。

來源（ID 取自 LemonSalarySystem「地區設定」）：
  訂單：salary_id「訂單資料」（tool-system 每日覆寫，本月＋次月；只取服務日期 ≤ 今天）
        之後可改讀 allowance_id「專員訂單」。
  單價：salary_id「{YYYYMM}調薪資料」B 姓名、L 調整後平日、M 調整後週末、AA:AF 大掃除。
  實習／實境：salary_id「新人實習」「新人實境」：A 期別＝本期，或 A 空白（尚未歸期）。
  加減時：allowance_id「專員收現」有訂單編號＋實際時數的列（v2.8.0）。
  大掃除日期：LemonSalarySystem「大掃除日期」。
執行：python -m modules.live_estimate_runner --region 台北 [--date 2026-11-22] [--dry-run]
"""
from __future__ import annotations

import argparse
import re
from datetime import date, datetime, timedelta, timezone
from typing import Dict, List, Optional, Tuple

from modules.live_estimate import (
    CATEGORIES, CleanupDates, Rates, build_estimates, parse_date, parse_number, split_staff,
)

MASTER_SHEET_ID = "1GdW3FSZ0s3TGeYiNx3JtYvED_RRfJjiFYwLFeYHZ1hA"
TZ = timezone(timedelta(hours=8))
OTHER_LABELS = ("水洗", "收納", "家電", "座椅", "地毯")
PROJECT_PRICE = "1299"   # Y 欄 = 1299 → 專案訂單（另計，不在清潔薪資表）


def period_of(d: date) -> Tuple[str, date, date]:
    """日期所屬期別與起訖日。"""
    if d.day <= 15:
        return f"{d:%Y%m}-1", d.replace(day=1), d.replace(day=15)
    nxt = (d.replace(day=28) + timedelta(days=4)).replace(day=1)
    return f"{d:%Y%m}-2", d.replace(day=16), nxt - timedelta(days=1)


def infer_date(text, ref: date) -> Optional[date]:
    """「11/14」這種沒有年份的日期：取最接近 ref 的年份。"""
    d = parse_date(text)
    if d:
        return d
    m = re.match(r"^\s*(\d{1,2})/(\d{1,2})\s*$", str(text or ""))
    if not m:
        return None
    mo, da = int(m.group(1)), int(m.group(2))
    best = None
    for y in (ref.year - 1, ref.year, ref.year + 1):
        try:
            cand = date(y, mo, da)
        except ValueError:
            continue
        if best is None or abs((cand - ref).days) < abs((best - ref).days):
            best = cand
    return best


def _col(rows: List[List[str]], i: int) -> List[str]:
    return [r[i] if i < len(r) else "" for r in rows]


def read_region_ids(gc, region: str) -> Dict[str, str]:
    rows = gc.open_by_key(MASTER_SHEET_ID).worksheet("地區設定").get_all_values()
    header = rows[0]
    for r in rows[1:]:
        if r and r[0].strip() == region:
            return {h: (r[i] if i < len(r) else "").strip() for i, h in enumerate(header)}
    raise KeyError(f"地區設定找不到 {region}")


def read_orders(ss, today: date, start: date) -> List[dict]:
    ws = ss.worksheet("訂單資料")
    # 只讀需要的欄，避免讀到電話地址等個資
    b, e, hij, m, y, ah, bd = ws.batch_get(
        ["B2:B", "E2:E", "H2:J", "M2:M", "Y2:Y", "AH2:AH", "BD2:BD"])
    n = max(len(b), len(hij))
    pad = lambda rows: rows + [[]] * (n - len(rows))
    b, e, hij, m, y, ah, bd = map(pad, (b, e, hij, m, y, ah, bd))
    out = []
    for i in range(n):
        item = (e[i] or [""])[0]
        if any(lbl in item for lbl in OTHER_LABELS) or "清潔" not in item:
            continue
        if str((y[i] or [""])[0]).replace(",", "").strip() == PROJECT_PRICE:
            continue
        d = parse_date((hij[i] or [""])[0])
        if d is None or d < start or d > today:
            continue
        hij_row = (hij[i] or []) + ["", "", ""]
        out.append(dict(
            order_no=(b[i] or [""])[0], date=d, slot=hij_row[1], hours=hij_row[2],
            customer=(m[i] or [""])[0], staff=(ah[i] or [""])[0],
            cancelled=bool(str((bd[i] or [""])[0]).strip()),
        ))
    return out


def read_rates(ss, yyyymm: str) -> Dict[str, Rates]:
    ws = ss.worksheet(f"{yyyymm}調薪資料")
    names, lm, af = ws.batch_get(["B3:B", "L3:M", "AA3:AF"])
    rates = {}
    for i, nm in enumerate(names):
        name = (nm or [""])[0].strip()
        if not name:
            continue
        wk = (lm[i] if i < len(lm) else []) + ["", ""]
        cl = ((af[i] if i < len(af) else []) + [""] * 6)[:6]
        rates[name] = Rates(parse_number(wk[0]), parse_number(wk[1]),
                            tuple(parse_number(x) for x in cl))
    return rates


def read_trainee_rows(ss, sheet: str, period: str, ref: date) -> Tuple[List[dict], int]:
    rows = ss.worksheet(sheet).get("A2:K")
    out, skipped = [], 0
    for r in rows:
        r = r + [""] * 11
        mark, name, hours = r[0].strip(), r[2].strip(), r[10]
        if not name:
            continue
        if mark and mark != period:
            continue
        d = infer_date(r[4], ref)
        if d is None:
            skipped += 1
            continue
        if not mark and (d > ref or (ref - d).days > 60):
            continue   # 尚未歸期的列只算今天以前、60 天內的（實境常晚一期才登記）
        out.append(dict(name=name, date=d, hours=hours))
    return out, skipped


def read_adjustments(ss, orders: List[dict]) -> Dict[Tuple[str, str], float]:
    """專員收現：有訂單編號＋實際時數的列。全部→該單所有專員；自己→提交者。後送出的覆蓋先送出的。"""
    try:
        rows = ss.worksheet("專員收現").get_all_values()
    except Exception:
        return {}
    if not rows:
        return {}
    h = rows[0]
    idx = {k: (h.index(k) if k in h else -1) for k in
           ("訂單編號", "實際時數", "調整範圍", "調整人員", "專員姓名")}
    if idx["訂單編號"] < 0 or idx["實際時數"] < 0:
        return {}
    staff_of = {o["order_no"]: split_staff(o["staff"]) for o in orders if o.get("order_no")}
    get = lambda r, k: (r[idx[k]] if 0 <= idx[k] < len(r) else "").strip()
    adj = {}
    for r in rows[1:]:
        no, actual = get(r, "訂單編號"), get(r, "實際時數")
        if not no or actual == "":
            continue
        if get(r, "調整範圍") == "全部":
            who = staff_of.get(no) or split_staff(get(r, "調整人員"))
        else:
            who = [get(r, "專員姓名")]
        for name in who:
            if name:
                adj[(no, name)] = parse_number(actual)
    return adj


def read_cleanup(gc) -> CleanupDates:
    try:
        row = (gc.open_by_key(MASTER_SHEET_ID).worksheet("大掃除日期").get("A2:C2") or [[]])[0]
    except Exception:
        return CleanupDates()
    row = row + ["", "", ""]
    return CleanupDates(parse_date(row[0]), parse_date(row[1]), parse_date(row[2]))


def run(region: str, today: Optional[date] = None, dry_run: bool = False, log=print) -> dict:
    from modules.auth import get_jenny_gspread_client
    gc = get_jenny_gspread_client()
    today = today or datetime.now(TZ).date()
    period, start, end = period_of(today)
    ids = read_region_ids(gc, region)
    salary = gc.open_by_key(ids["salary_id"])
    allowance = gc.open_by_key(ids["allowance_id"])

    orders = read_orders(salary, today, start)
    rates = read_rates(salary, f"{today:%Y%m}")
    interns, bad_i = read_trainee_rows(salary, "新人實習", period, today)
    trainees, bad_t = read_trainee_rows(salary, "新人實境", period, today)
    adjustments = read_adjustments(allowance, orders)
    cleanup = read_cleanup(gc)
    log(f"{region} {period}（{start:%m/%d}～{today:%m/%d}）：訂單 {len(orders)}、"
        f"單價 {len(rates)} 人、實習 {len(interns)}、實境 {len(trainees)}、加減時 {len(adjustments)}")
    if bad_i or bad_t:
        log(f"⚠️ 實習／實境日期無法判讀：{bad_i}／{bad_t} 列")

    est = build_estimates(orders, rates, interns, trainees, cleanup, start, today, adjustments)
    stamp = datetime.now(TZ).strftime("%Y/%m/%d %H:%M")

    summary = [["期別", "專員", "場次", "實境時數", "實習時數", *[f"{c}時數" for c in CATEGORIES],
                "估算承攬費", "資料截至", "更新時間", "提醒"]]
    detail = [["期別", "專員", "服務日期", "星期", "服務時間", "客戶", "時數", "類別",
               "訂單編號", "服務專員", "備註"]]
    week = "一二三四五六日"
    intern_days, intern_count = {}, {}
    for r in interns:
        intern_days.setdefault(r["name"], set()).add(r["date"])
        intern_count[r["name"]] = intern_count.get(r["name"], 0) + 1
    for name in sorted(est):
        e = est[name]
        sessions = sorted(e.sessions, key=lambda s: (s.date, s.slot))
        # 場次同薪資表 2001：訂單場次扣掉實習筆數
        summary.append([period, name, max(len(sessions) - intern_count.get(name, 0), 0), e.trainee_hours, e.intern_hours,
                        *[e.hours[c] for c in CATEGORIES], round(e.amount()),
                        f"{today:%Y/%m/%d}", stamp, "；".join(e.warnings)])
        for s in sessions:
            note = s.note
            if s.date in intern_days.get(name, ()):
                note = (note + "；" if note else "") + "實習日（實習時數另以200計）"
            detail.append([period, name, f"{s.date:%Y/%m/%d}", week[s.date.weekday()], s.slot,
                           s.customer, s.hours, s.category, s.order_no, s.staff, note])

    log(f"專員 {len(summary) - 1} 人、明細 {len(detail) - 1} 列")
    if not dry_run:
        master = gc.open_by_key(MASTER_SHEET_ID)
        for title, values in ((f"即時試算-{region}", summary), (f"即時試算明細-{region}", detail)):
            try:
                ws = master.worksheet(title)
                ws.clear()
            except Exception:
                ws = master.add_worksheet(title, rows=max(len(values) + 50, 200), cols=len(values[0]))
            if ws.row_count < len(values):
                ws.add_rows(len(values) - ws.row_count + 50)
            ws.update("A1", values, value_input_option="USER_ENTERED")
            ws.freeze(rows=1)
            log(f"已寫入「{title}」")
    return {"period": period, "staff": len(summary) - 1, "sessions": len(detail) - 1}


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--region", default="台北")
    p.add_argument("--date", default="")
    p.add_argument("--dry-run", action="store_true")
    a = p.parse_args()
    d = datetime.strptime(a.date, "%Y-%m-%d").date() if a.date else None
    for region in [x.strip() for x in a.region.split(",") if x.strip()]:
        run(region, d, a.dry_run)


if __name__ == "__main__":
    main()
