from constants import *
from libc.time cimport tm, mktime
from cpython.int cimport PyInt_AS_LONG
from cpython.tuple cimport PyTuple_GET_ITEM
import datetime
import os
from pytz import utc



ctypedef long long long64

cdef is_windows = os.name == 'nt'
cdef has_timestamp_fn = hasattr(datetime.datetime, 'timestamp')

cdef long64 MCS_PER_SECOND = MCS_PER_SECOND
cdef long64 MCS_PER_MINUTE = MCS_PER_MINUTE
cdef long64 MCS_PER_HOUR = MCS_PER_HOUR

cdef long64 MLS_PER_SECOND = MLS_PER_SECOND
cdef long64 MLS_PER_MINUTE = MLS_PER_MINUTE
cdef long64 MLS_PER_HOUR = MLS_PER_HOUR


epoch = datetime.datetime(1970, 1, 1, tzinfo=utc)
epoch_naive = datetime.datetime(1970, 1, 1)



cpdef prepare_timestamp_millis(object data):
    cdef object tt
    cdef tm time_tuple
    if isinstance(data, datetime.datetime):
        if not has_timestamp_fn:
            if data.tzinfo is not None:
                return <long64>(<double>(
                    <object>(data - epoch).total_seconds()) * MLS_PER_SECOND
                )
            tt = data.timetuple()
            time_tuple.tm_sec = PyInt_AS_LONG(<object>(PyTuple_GET_ITEM(tt, 5)))
            time_tuple.tm_min = PyInt_AS_LONG(<object>(PyTuple_GET_ITEM(tt, 4)))
            time_tuple.tm_hour = PyInt_AS_LONG(<object>(PyTuple_GET_ITEM(tt, 3)))
            time_tuple.tm_mday = PyInt_AS_LONG(<object>(PyTuple_GET_ITEM(tt, 2)))
            time_tuple.tm_mon = PyInt_AS_LONG(<object>(PyTuple_GET_ITEM(tt, 1))) - 1
            time_tuple.tm_year = PyInt_AS_LONG(<object>(PyTuple_GET_ITEM(tt, 0))) - 1900
            time_tuple.tm_isdst = PyInt_AS_LONG(<object>(PyTuple_GET_ITEM(tt, 8)))

            return mktime(& time_tuple) * MLS_PER_SECOND + <long64>(
                int(data.microsecond) / 1000)
        else:
            # On Windows, timestamps before the epoch will raise an error.
            # See https://bugs.python.org/issue36439
            if is_windows:
                if data.tzinfo is not None:
                    return <long64>(<double>(
                        <object>(data - epoch).total_seconds()) * MLS_PER_SECOND
                    )
                else:
                    return <long64>(<double>(
                        <object>(data - epoch_naive).total_seconds()) * MLS_PER_SECOND
                    )
            else:
                return <long64>(<double>(data.timestamp()) * MLS_PER_SECOND)
    else:
        return data