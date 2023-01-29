"""
date_managers.exceptions
~~~~~~~~~~~~~~~~~~~~
Exceptions used in date_managers.
"""


class DateManagersException(Exception):
    """A base class for sdc_dp_helpers' exceptions."""


class TimeIntervalError(DateManagersException):
    """Exception Raised When you provide a wrong time interval value
    Args:
        DateManagersException (_type_): _description_
    """


class DateValueError(DateManagersException):
    """Exception Raised When you provide a wrong time interval value
    Args:
        DateManagersException (_type_): _description_
    """


class CadenceError(DateManagersException):
    """Exception Raised When you provide a wrong time cadence value
    Args:
        DateManagersException (_type_): _description_
    """


class TimeBucketError(DateManagersException):
    """Exception Raised When you provide a wrong time bucket value
    Args:
        DateManagersException (_type_): _description_
    """


class CadenceTimeBucketError(DateManagersException):
    """Exception Raised When you provide a wrong cadence or time bucket value
    Args:
        DateManagersException (_type_): _description_
    """
