from datetime import datetime, timedelta, date
from enum import Enum


class Month(Enum):
    JANUARY = 1
    FEBRUARY = 2
    MARCH = 3
    APRIL = 4
    MAY = 5
    JUNE = 6
    JULY = 7
    AUGUST = 8
    SEPTEMBER = 9
    OCTOBER = 10
    NOVEMBER = 11
    DECEMBER = 12

    @classmethod
    def get_month_name(cls, month_number: int):
        for month in cls:
            if month.value == month_number:
                return month.name.capitalize()
        return None


def get_all_days_of_month(year: int, month: int) -> set[date]:
    first_day = datetime(year, month, 1).date()
    next_month = first_day.replace(month=month % 12 + 1, day=1)
    last_day = next_month - timedelta(days=1)
    return {
        first_day + timedelta(days=i) for i in range((last_day - first_day).days + 1)
    }
