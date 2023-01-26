"""UTILITIES TO HANDLE DATE VALUES"""

# pylint: disable=too-few-public-methods
from abc import ABC, abstractmethod
from datetime import datetime, timedelta
import re
from typing import Tuple, Generator, Union
from dateutil.relativedelta import relativedelta

# import pandas as pd
from api_utilities.exceptions import (
    TimeIntervalError,
    TimeBucketError,
    CadenceTimeBucketError,
    DateValueError,
)


class DatePhraseHandler(ABC):
    """Handles Phrases In the Input Date"""

    date_value: Union[str, None] = None
    time_bucket: Union[str, None] = None
    cadence: Union[int, None] = None

    def __init__(self, date_value: str):
        self.date_value = date_value

    @abstractmethod
    def phrase_to_date(self) -> str:
        """Processes the date_value and translates it to a datetime value
        Raises:
            NotImplementedError: _description_
        Returns:
            str: _description_
        """
        raise NotImplementedError


class BaseDateInterval(ABC):
    """Get Start of the Date Range"""

    def __init__(self, time_bucket: str, cadence: int):
        self.time_bucket = time_bucket
        self.cadence = cadence

    @abstractmethod
    def get_start_date(self, start_date: datetime) -> datetime:
        """Gets the start date of the range provided the input startdate
        Args:
            start_date (datetime): should be a datetime value
        Raises:
            NotImplementedError: _description_
        Returns:
            datetime: the processed start date
        """
        raise NotImplementedError

    @abstractmethod
    def add_interval(self, date_value: datetime, cadence: int) -> datetime:
        """Add the time interval to the current start date to get the end date
        Args:
            date_value (datetime): datetime value that is the start of the period
            cadence (int): time interval to add to the start date tp the end date
        Raises:
            NotImplementedError: raise error of NotImplementedError
        Returns:
            datetime: the end date value not inclusive
        """
        raise NotImplementedError

    @abstractmethod
    def subtract_interval(self, date_value: datetime, cadence: int) -> datetime:
        """Subtract the time interval to the current start date to get the end date
        Args:
            date_value (datetime): datetime value that is the start of the period
            cadence (int): time interval to add to the start date tp the end date
        Raises:
            NotImplementedError: raise error of NotImplementedError
        Returns:
            datetime: the end date value not inclusive
        """
        raise NotImplementedError


class YearInterval(BaseDateInterval):
    """Class ti Get Yearly Cadence Start Date"""

    def __init__(self, time_bucket: str, cadence: int):
        super().__init__(time_bucket=time_bucket, cadence=cadence)

    def get_start_date(self, start_date: datetime) -> datetime:
        """Handles Yearly Buckets"""
        if self.time_bucket in ["yearly", "last_year", "this_year"]:
            start_date = start_date.replace(month=1, day=1)
        return start_date

    def add_interval(self, date_value: datetime, cadence: int):
        """Return a date that's `years` years after the date (or datetime)
        object `date_value`. Return the same calendar date (month and day) in the
        destination year, if it exists, otherwise use the following day
        (thus changing February 29 to March 1).
        """
        next_date = date_value + relativedelta(years=cadence)
        return next_date

    def subtract_interval(self, date_value: datetime, cadence: int):
        next_date = date_value - relativedelta(years=cadence)
        return next_date


class MonthInterval(BaseDateInterval):
    """Gets the Monthly Cadence Start Date"""

    def __init__(self, time_bucket: str, cadence: int):
        super().__init__(time_bucket=time_bucket, cadence=cadence)

    def get_start_date(self, start_date: datetime) -> datetime:
        """Handles Monthly Buckets"""
        if self.time_bucket in ["monthly", "month", "last_month", "this_month"]:
            start_date = start_date.replace(day=1)
        return start_date

    def add_interval(self, date_value: datetime, cadence: int):
        next_date = date_value + relativedelta(months=cadence)
        return next_date

    def subtract_interval(self, date_value: datetime, cadence: int):
        next_date = date_value - relativedelta(months=cadence)
        return next_date


class WeekInterval(BaseDateInterval):
    """Get the Weekly Cadence Start Date"""

    def __init__(self, time_bucket: str, cadence: int):
        super().__init__(time_bucket=time_bucket, cadence=cadence)

    def get_start_date(self, start_date: datetime) -> datetime:
        """Handles Weekly Buckets"""
        if self.time_bucket in ["weekly", "last_week", "this_week"]:
            if datetime.strftime(start_date, "%A") == "Sunday":
                return start_date
            start_date = start_date - timedelta(days=start_date.weekday() + 1)
        return start_date

    def add_interval(self, date_value: datetime, cadence: int):
        next_date = date_value + relativedelta(weeks=cadence)
        return next_date

    def subtract_interval(self, date_value: datetime, cadence: int):
        next_date = date_value - relativedelta(weeks=cadence)
        return next_date


class DayInterval(BaseDateInterval):
    """Get the Daily Cadence Start Date"""

    def __init__(self, time_bucket: str, cadence: int):
        super().__init__(time_bucket=time_bucket, cadence=cadence)

    def get_start_date(self, start_date: datetime) -> datetime:
        """Handles Daily Buckets"""
        return start_date

    def add_interval(self, date_value: datetime, cadence: int):
        next_date = date_value + relativedelta(days=cadence)
        return next_date

    def subtract_interval(self, date_value: datetime, cadence: int):
        next_date = date_value - relativedelta(days=cadence)
        return next_date


class PastDatePhraseHandler(DatePhraseHandler):
    """Handles Past Date Phrases"""

    def phrase_to_date(self) -> Tuple[int, str]:
        """Processes the date value to return self.cadence and bucket
        Args:
            date_value (str): string representing date
        Raises:
            ValueError: if passed a wrong and unexpected date value
        Returns:
            Tuple[int, str]: self.cadence and bucket(day, week, month, year)
        """
        pattern = re.compile(r"[a-z]+")
        if not isinstance(self.date_value, str) or not pattern.search(
            self.date_value.strip().lower()
        ):
            raise TypeError(
                f"wrong date_string provided, expecting a string but got: {self.date_value}"
            )
        date_value = self.date_value.lower().strip()
        if date_value in ["yesterday", "last_week", "last_month", "last_year"]:
            self.cadence, self.time_bucket = 1, date_value
        if date_value in ["today", "this_week", "this_year", "this_month"]:
            self.cadence, self.time_bucket = 0, date_value
        if re.search(r"\_", date_value) and self.time_bucket is None:
            if len(date_value.split("_")) != 3 or date_value.split("_")[2] != "ago":
                raise DateValueError(f"wrong date string provided: {date_value}")
            if len(date_value.split("_")) == 3:
                self.cadence, self.time_bucket = date_value.split("_")[:-1]
                if self.time_bucket.endswith("s"):
                    self.time_bucket = self.time_bucket.replace("s", "")
        if self.cadence is None or self.time_bucket is None:
            raise CadenceTimeBucketError(
                f"cadence or time_bucket is None, wrong date_string provided: '{date_value}'"
            )
        # print(self.cadence, self.time_bucket)
        self.cadence, self.time_bucket = int(self.cadence), self.time_bucket.strip()
        return self.cadence, self.time_bucket


class IntervalDatePhraseHandler(DatePhraseHandler):
    """Class for handling interval date string"""

    def phrase_to_date(self) -> Tuple[int, str]:
        """Processes the interval variable to get the cadence and the time_bucket
        Raises:
            TypeError: is the interval value p[rovided is not a string]
            ValueError: if the interval can be split more than 2 times
            ValueError: if time_bucket and cadence end up to be None
        Returns:
            Tuple[int, str]: cadence that is an int and time_bucket
        """
        if not isinstance(self.date_value, str) or not re.search(
            r"[a-z]+", str(self.date_value).lower()
        ):
            raise TypeError(
                f"wrong interval provided, expecting a string but got: {self.date_value}"
            )
        date_value = self.date_value.lower().strip()
        if date_value in ["day", "yearly", "weekly", "monthly"]:
            self.cadence, self.time_bucket = 1, date_value
        if len(date_value.split("_")) > 2 and self.time_bucket is None:
            raise TimeIntervalError(
                f"invalid value for interval provided: {date_value}"
            )
        if len(self.date_value.replace(" ", "_").split("_")) == 2:
            self.cadence, self.time_bucket = date_value.split("_")
        if self.cadence is None or self.time_bucket is None:
            raise CadenceTimeBucketError(
                f"cadence or time_bucket is None, wrong interval provided: '{date_value}'"
            )
        # print(self.cadence, self.time_bucket)
        self.cadence, self.time_bucket = int(self.cadence), self.time_bucket.strip()
        return self.cadence, self.time_bucket


class DateHandlerFactory:
    """A class that handles provided date to translate it to valie datetime object"""

    def get_date_handler(self, time_bucket: str, cadence: int) -> BaseDateInterval:
        """Factory method to determine what time bucket we are going to process
        Args:
            time_bucket (str): either day, week, month, year
            cadence (int): cadence to be used to subtract or add date
        Raises:
            TimeBucketError: If the time bucket provided is not catered for
        Returns:
            BaseDateInterval: Returns and instance of BaseDateInterval
        """
        root_bucket = None
        time_bucket_mapping = {
            "day": ["day", "yesterday", "today"],
            "week": ["week", "weekly", "last_week", "this_week"],
            "month": ["month", "monthly", "last_month", "this_month"],
            "year": ["year", "yearly", "last_year", "this_year"],
        }
        for key, val in time_bucket_mapping.items():
            if time_bucket in val:
                root_bucket = key
        if root_bucket is None:
            raise TimeBucketError(f"wrong time bucket in the provided: '{time_bucket}'")
        date_handlers = {
            "day": DayInterval,
            "week": WeekInterval,
            "month": MonthInterval,
            "year": YearInterval,
        }
        return date_handlers[root_bucket](time_bucket=time_bucket, cadence=cadence)


def date_string_handler(date_string: str) -> datetime:
    """Processes the Date and Returns the date value to work with
    Args:
        date_string (str): used to determine the date '2022-11-14', 2_days_ago, yeaterday, today
    Returns:
        datetime: datetime value
    """
    if re.search(r"\d{4}\-\d{2}\-\d{2}", str(date_string).strip()):
        return datetime.strptime(str(date_string).strip(), "%Y-%m-%d")
    phrase_handler = PastDatePhraseHandler(date_value=date_string)
    cadence, time_bucket = phrase_handler.phrase_to_date()
    handler = DateHandlerFactory().get_date_handler(
        time_bucket=time_bucket, cadence=cadence
    )
    start_date = datetime.now()
    start_date = handler.get_start_date(start_date=start_date)
    date_value = handler.subtract_interval(date_value=start_date, cadence=cadence)
    return date_value


def date_range_iterator(
    start_date: str,
    end_date: str,
    interval: str,
    end_inclusive: bool = False,
    time_format: str = "%Y-%m-%d",
) -> Generator[Tuple[str, str], None, None]:
    """Processes the Date and Returns the date value to work with
    Args:
        start_date (str): date string (2022-11-14 or the allowed date string values)
        end_date (str): date string (2022-11-14 or the allowed date string values)
        interval (str): string 1_month, 1_day, yearly, monthly, weekly
        end_inclusive (bool): whether the yielded end for period/interval is inclusive or not.
        time_format (str, optional): The format of the returned date value. Defaults to "%Y-%m-%d".
    Yields:
        Generator[Tuple[str, str], None, None]: start and end (exclusive) for each time interval.
    """
    phrase_handler = IntervalDatePhraseHandler(date_value=interval)
    cadence, time_bucket = phrase_handler.phrase_to_date()
    handler = DateHandlerFactory().get_date_handler(
        time_bucket=time_bucket, cadence=cadence
    )
    startdate = date_string_handler(start_date)
    enddate = date_string_handler(end_date)
    startdate = handler.get_start_date(start_date=startdate)
    next_end = handler.add_interval(date_value=startdate, cadence=cadence)
    while startdate <= enddate:
        end = next_end
        if end_inclusive:
            end = next_end - timedelta(days=1)
        yield datetime.strftime(startdate, time_format), datetime.strftime(
            end, time_format
        )
        startdate = next_end
        next_end = handler.add_interval(date_value=startdate, cadence=cadence)


def date_handler(date_string, date_format="%Y-%m-%d"):
    """
    Takes a date string or phrase and returns the valid date in
    the specified format.
    :date_string: A phrase like "3_days_ago", "yesterday" or "today"
                     or a date string such as "2021-01-01".
    :date_format: The returned formatting of the date.
    """
    try:
        if date_string == "today":
            return datetime.now().strftime(date_format)
        if date_string == "yesterday":
            return (datetime.now() - timedelta(days=1)).strftime(date_format)
        if "_days_ago" in date_string:
            return phrase_to_date(phrase=date_string, date_format=date_format)
        return date_string
    except ValueError as err:
        raise ValueError(
            "StartDate requires a valid input "
            'such as "today", "yesterday" or "<int>_days_ago".'
        ) from err


def phrase_to_date(phrase, date_format="%Y-%m-%d"):
    """
    Takes a typical phrase such as "3_days_ago" and returns a
    date based on that phrase.
    :phrase: str. Something like 3_days_ago or 25_days_ago etc.
    :return: '%Y-%m-%d'
    """
    try:
        date_delta = int(phrase.split("_")[0])
        return (datetime.now() - timedelta(days=date_delta)).strftime(date_format)
    except ValueError as err:
        raise ValueError(
            f"Phrasing for date: {phrase} is not valid, "
            f"try something like: 3_days_ago"
        ) from err


def date_range(start_date, end_date, delta=timedelta(days=1)):
    """
    The range is inclusive, so both start_date and end_date will be returned.
    :start_date: The datetime object representing the first day in the range.
    :end_date: The datetime object representing the second day in the range.
    :delta: A datetime.timedelta instance, specifying the step interval. Defaults to one day.
    Yields:
        Each datetime object in the range.
    """
    start_date = date_handler(date_string=start_date)
    end_date = date_handler(date_string=end_date)
    start_date = datetime.strptime(start_date, "%Y-%m-%d")
    end_date = datetime.strptime(end_date, "%Y-%m-%d")
    current_date = start_date
    while current_date <= end_date:
        yield current_date.strftime("%Y-%m-%d")
        current_date += delta


# # pylint: disable=invalid-name
# def filter_data_by_dates(
#     start_date, end_date, date_field, data_frame: pd.DataFrame, date_fmt="%Y-%m-%d"
# ):
#     """
#     Takes a data frame and filters the data by given date field and
#     date scopes that can be phrases.
#     If no scope is added, imply current time is expected.
#     """
#     _now = datetime.now().strftime(date_fmt)
#     if start_date is None:
#         sd = _now
#     else:
#         sd = date_handler(start_date, date_fmt)
#     if end_date is None:
#         ed = _now
#     else:
#         ed = date_handler(end_date, date_fmt)
#     print(f"Gathering data between {sd} and {ed}.")
#     # filter data by given date field
#     if sd is not None and ed is not None:
#         data_frame["tmp_date"] = pd.to_datetime(data_frame[date_field])
#         data_frame["tmp_date"] = data_frame["tmp_date"].dt.strftime(date_fmt)
#         data_frame = data_frame[
#             (
#                 data_frame["tmp_date"]
#                 >= datetime.strptime(sd, date_fmt).strftime(date_fmt)
#             )
#             & (
#                 data_frame["tmp_date"]
#                 <= datetime.strptime(ed, date_fmt).strftime(date_fmt)
#             )
#         ]
#         data_frame = data_frame.drop("tmp_date", axis="columns")
#     if len(data_frame.index) > 0:
#         return data_frame
#     print(f"No data for given date filter: {sd} to {ed}.")
#     return None
