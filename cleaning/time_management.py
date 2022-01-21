from datetime import datetime, timedelta
from pytz import timezone
from pandas.core.tools.datetimes import _guess_datetime_format_for_array
import pandas as pd
import numpy as np

def eastern_time(fmt='%Y-%m-%d-%H:%M', delta=0):
    # date format examples: '%Y-%m-%d-%H:%M:%S %Z%z' = 2020-12-27-14:02:59 EST-0500
    # define eastern timezone
    eastern = timezone('US/Eastern')
    # naive datetime: naive_dt = datetime.now()
    # localized datetime
    loc_dt = datetime.now(eastern)
    loc_dt += timedelta(days=delta)
    time_string = loc_dt.strftime(fmt)
    return time_string

def string_to_datetime(time_string, fmt=None):
    # example format '%Y-%m-%d-%H:%M' '2020-12-27-14:10'
    if fmt == None:
        datetime_object = pd.to_datetime(time_string)
    else:
        datetime_object = datetime.strptime(time_string, fmt)
    return datetime_object


def format_time_string(time_string, current_fmt = None, new_fmt = '%Y-%m-%d-%H:%M'):
    if current_fmt == None:
        datetime_object = pd.to_datetime(time_string)
    else:
        datetime_object = datetime.strptime(time_string, current_fmt)

    new_time_string = datetime_object.strftime(new_fmt)  #'2020-12-27-14:10'
    return new_time_string

# format_time_string('2020-12-27-14:11:10')