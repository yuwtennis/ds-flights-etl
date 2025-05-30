"""Module that takes care of time zone converting"""

import logging
from datetime import datetime, timedelta
from typing import Any, Generator, Tuple, Optional
import pytz
import apache_beam as beam

from dsflightsetl import LOGGER
from dsflightsetl.airport import AirportCsvPolicies, AirportLocation
from dsflightsetl.flight import Flight, FlightPolicy, VALID_DATETIME_FMT, VALID_DATE_FMT

CANCELLED_FLIGHT_TIME_STR = ""
CANCELLED_FLIGHT_TZ_OFFSET = 0.0


def tz_correct(
    flight: Flight, airport_timezones: dict[int, AirportLocation]
) -> Generator[Flight, None, None]:
    """

    :param airport_timezones:
    :param flight:
    :return:
    """
    flight_tmp: dict[str, Any] = flight.model_dump()

    try:
        for field in ["crs_dep_time", "dep_time", "wheels_off"]:
            timezone = airport_timezones[
                getattr(flight, "origin_airport_seq_id")
            ].timezone
            flight_tmp[field], dep_tz = as_utc_with_standard_time_offset(
                flight.fl_date, getattr(flight, field), timezone
            )

        flight_tmp["dep_airport_tzoffset"] = dep_tz

        for field in ["crs_arr_time", "arr_time", "wheels_on"]:
            timezone = airport_timezones[
                getattr(flight, "dest_airport_seq_id")
            ].timezone
            flight_tmp[field], arr_tz = as_utc_with_standard_time_offset(
                flight.fl_date, getattr(flight, field), timezone
            )

        flight_tmp["arr_airport_tzoffset"] = arr_tz

        for field in ["wheels_off", "crs_arr_time", "arr_time", "wheels_on"]:
            flight_tmp[field] = add_24h_if_before(
                flight_tmp[field], flight_tmp["dep_time"]
            )

        yield Flight(**flight_tmp)
    except KeyError:
        logging.exception("Unknown airport %s", flight.model_dump())


def as_utc_with_standard_time_offset(
    flight_date: str, hhmm: str, tzone: Optional[str]
) -> Tuple[str, float]:
    """
    Convert date time string into utc with standard time offsets.
    This is a workaround with is_dst.

    https://pypi.org/project/pytz/
    >The is_dst parameter is ignored for most timestamps.
    >It is only used during DST transition ambiguous periods to resolve that ambiguity.

    :param flight_date:
    :param hhmm:
    :param tzone:
    :return:
    """

    try:
        if AirportCsvPolicies.is_valid_time(hhmm) and tzone is not None:
            loc_tz = pytz.timezone(str(tzone))

            # Tip: localize will convert into midnight
            # then add hours from that offset will be a safe way
            loc_dt = loc_tz.localize(datetime.strptime(flight_date, VALID_DATE_FMT))
            loc_dt += timedelta(hours=int(hhmm[:2]), minutes=int(hhmm[2:]))

            dst_offset: Optional[timedelta] = loc_dt.dst()
            utc_offset: Optional[timedelta] = loc_dt.utcoffset()

            if not isinstance(utc_offset, timedelta) or not isinstance(
                dst_offset, timedelta
            ):
                raise ValueError()

            utc_dt = loc_dt.astimezone(pytz.utc) + dst_offset

            # https://docs.python.org/3/library/datetime.html#datetime.tzinfo.utcoffset
            # Returning standard offset required sum of utc offset and dst offset
            return (
                utc_dt.strftime(VALID_DATETIME_FMT),
                utc_offset.total_seconds() - dst_offset.total_seconds(),
            )
        return CANCELLED_FLIGHT_TIME_STR, CANCELLED_FLIGHT_TZ_OFFSET
    except (ValueError, pytz.UnknownTimeZoneError) as err:
        raise err


def add_24h_if_before(arr_time: str, dep_time: str) -> str:
    """
    Date hack. Compensate the arrival time by adding 24 hrs
    since fl date is based on departure airport.

    :param arr_time:
    :param dep_time:
    :return:
    """
    if (
        FlightPolicy.is_valid_datetime(arr_time, VALID_DATETIME_FMT)
        and FlightPolicy.is_valid_datetime(dep_time, VALID_DATETIME_FMT)
        and arr_time < dep_time
    ):
        adt = datetime.strptime(arr_time, VALID_DATETIME_FMT)
        adt += timedelta(hours=24)
        return adt.strftime(VALID_DATETIME_FMT)

    LOGGER.debug(
        "Arrival time is not before departure time.  arr_time: %s, dep_time: %s",
        arr_time,
        dep_time,
    )
    return arr_time


class UTCConvert(beam.PTransform):
    """Convert time into UTC"""

    def __init__(self, airports: beam.pvalue.AsDict):
        """

        :param airports: AsDict of airport seq id and AirportLocation
        """
        super().__init__()
        self._airports = airports

    def expand(self, pcoll: Any) -> Any:  # pylint: disable=arguments-renamed
        """
        Takes pcollection of json string flights and convert time into UTC

        :param p_col:
        :return:
        """
        return pcoll | "Convert origin and dest timezone into utc" >> beam.FlatMap(
            tz_correct, self._airports
        )
