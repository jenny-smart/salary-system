import datetime
import holidays


def previous_taiwan_business_day(
    target_date: datetime.date,
) -> datetime.date:
    """
    若指定日期為六、日或台灣國定假日，
    往前找到最近一個工作日。
    """
    tw_holidays = holidays.country_holidays(
        "TW",
        years=[target_date.year],
    )

    d = target_date

    while d.weekday() >= 5 or d in tw_holidays:
        d -= datetime.timedelta(days=1)

    return d
