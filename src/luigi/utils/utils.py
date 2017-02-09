from datetime import datetime
from pytz import timezone
import re
import time


def convert_timezone(
        datetime_string,
        datetime_format='%Y/%m/%d %H:%M:%S',
        from_timezone='Asia/Taipei',
        to_timezone='UTC'):
    """Convert timezone.
    Reference:
        http://tech.glowing.com/cn/dealing-with-timezone-in-python/

    Args:
        datetime_string: datetime string object.
        datetime_format: datetime format.
        from_timezone: timezone of datetime string.
        to_timezone: target timezone.

    Returns:
        datetime object converted to target timezone.
    """

    tz = timezone(from_timezone)
    target = datetime.strptime(datetime_string, datetime_format)
    return tz.localize(target).astimezone(timezone(to_timezone))


def to_dict(keys, values):
    convert = {
        datetime: lambda x: x.strftime('%Y-%m-%d %H:%M:%S')
    }
    def f(v):
        return convert.get(type(x), lambda x: x)(v)

    return dict(
        (k, convert(v)) for (k, v) in zip(keys, values)
    )
