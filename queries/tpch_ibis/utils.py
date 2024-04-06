import datetime
import functools
from dateutil.relativedelta import relativedelta

@functools.singledispatch
def add_date(datestr, dy=0, dm=0, dd=0):
    dt = datetime.date.fromisoformat(datestr)
    dt += relativedelta(years=dy, months=dm, days=dd)
    return dt.isoformat()
@functools.singledispatch
def date_obj(datestr):
    dt = datetime.date.fromisoformat(datestr)
    print("DATE:")
    print(type(dt))
    return dt


@add_date.register(datetime.date)
def _(dt, dy=0, dm=0, dd=0):
    dt += relativedelta(years=dy, months=dm, days=dd)
    return dt