from datetime import date

from modules.live_estimate import CleanupDates, Rates, build_estimates, classify


def test_classify_weekday_weekend_evening():
    cl = CleanupDates()
    assert classify(date(2026, 11, 12), "09:00-12:00", cl) == "平日"
    assert classify(date(2026, 11, 14), "09:00-12:00", cl) == "週末"
    assert classify(date(2026, 11, 14), "19:00-22:00", cl) == "晚間"


def test_classify_cleanup_phases():
    cl = CleanupDates(date(2026, 12, 15), date(2027, 1, 21), date(2027, 2, 4))
    assert classify(date(2026, 12, 20), "09:00-12:00", cl) == "大掃除1週末"
    assert classify(date(2027, 1, 21), "09:00-12:00", cl) == "大掃除2平日"
    assert classify(date(2027, 2, 5), "09:00-12:00", cl) == "平日"


def test_intern_hours_paid_200_and_deducted_from_order_hours():
    orders = [
        dict(date="2026/11/12", slot="09:00-12:00", customer="葉", staff="李心媛 X 李佳芸", hours="3"),
        dict(date="2026/11/14", slot="09:00-12:00", customer="林", staff="李佳芸", hours="3"),
    ]
    rates = {"李佳芸": Rates(280, 320), "李心媛": Rates(310, 350)}
    interns = [dict(name="李佳芸", date="2026/11/12", hours=3)]
    trainees = [dict(name="李佳芸", date="2026/10/21", hours=2)]
    est = build_estimates(orders, rates, interns, trainees)
    e = est["李佳芸"]
    assert e.hours["平日"] == 0 and e.hours["週末"] == 3
    assert e.amount() == 3 * 200 + 2 * 200 + 3 * 320
    assert est["李心媛"].amount() == 3 * 310


def test_adjustment_replaces_one_staff_hours():
    orders = [dict(date="2026/11/12", slot="09:00-12:00", staff="A X B", hours="3", order_no="LC1")]
    est = build_estimates(orders, {"A": Rates(300, 340), "B": Rates(300, 340)},
                          adjustments={("LC1", "A"): 4})
    assert est["A"].hours["平日"] == 4 and est["B"].hours["平日"] == 3
    assert est["A"].sessions[0].note == "原3→實際4小時"


def test_cancelled_and_out_of_range_skipped():
    orders = [
        dict(date="2026/11/12", staff="A", hours="3", cancelled=True),
        dict(date="2026/11/20", staff="A", hours="3"),
        dict(date="2026/11/10", staff="A", hours="2"),
    ]
    est = build_estimates(orders, {"A": Rates(300, 340)}, start=date(2026, 11, 1), end=date(2026, 11, 15))
    assert est["A"].hours["平日"] == 2


from modules.live_estimate_runner import infer_date, period_of, read_trainee_rows


def test_period_of():
    assert period_of(date(2026, 11, 22)) == ("202611-2", date(2026, 11, 16), date(2026, 11, 30))
    assert period_of(date(2026, 2, 3)) == ("202602-1", date(2026, 2, 1), date(2026, 2, 15))


def test_infer_date_without_year():
    assert infer_date("11/14", date(2026, 11, 22)) == date(2026, 11, 14)
    assert infer_date("12/30", date(2027, 1, 5)) == date(2026, 12, 30)
    assert infer_date("2026/11/2", date(2026, 11, 22)) == date(2026, 11, 2)


class _WS:
    def __init__(self, rows):
        self.rows = rows

    def get(self, _):
        return self.rows


class _SS:
    def __init__(self, rows):
        self.rows = rows

    def worksheet(self, _):
        return _WS(self.rows)


def test_trainee_rows_current_period_or_unmarked():
    rows = [
        ["202611-1", "", "舊人", "", "11/12", "", "", "", "", "", "3"],
        ["202611-2", "", "甲", "", "11/18", "", "", "", "", "", "2"],
        ["", "", "乙", "", "11/20", "", "", "", "", "", "3"],
        ["", "", "丙", "", "11/28", "", "", "", "", "", "3"],
    ]
    got, bad = read_trainee_rows(_SS(rows), "新人實習", "202611-2", date(2026, 11, 22))
    assert [r["name"] for r in got] == ["甲", "乙"] and bad == 0
