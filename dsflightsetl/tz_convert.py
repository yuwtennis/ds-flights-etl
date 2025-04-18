"""Module that takes care of time zone converting"""

import logging
from datetime import datetime, timedelta
from typing import Any, Optional, Generator
import pytz
import apache_beam as beam
from dsflightsetl.airport import AirportCsvPolicies, AirportLocation
from dsflightsetl.flight import Flight


VALID_DATE_FMT = "%Y-%m-%d"
VALID_DATETIME_FMT = "%Y-%m-%d %H:%M:%S"
CANCELLED_FLIGHT_TIME_STR = ""


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
            flight_tmp[field] = as_utc(
                flight.fl_date,
                getattr(flight, field),
                timezone,
            )

        for field in ["crs_arr_time", "arr_time", "wheels_on"]:
            timezone = airport_timezones[
                getattr(flight, "dest_airport_seq_id")
            ].timezone
            flight_tmp[field] = as_utc(
                flight.fl_date,
                getattr(flight, field),
                timezone,
            )

        yield Flight(**flight_tmp)
    except KeyError:
        logging.exception("Unknown airport %s", flight.model_dump())


def as_utc(date: str, hhmm: str, tzone: Optional[str]) -> str:
    """
    Convert date time string into utc by putting origin airport timezone into account

    :param date:
    :param hhmm:
    :param tzone:
    :return:
    """

    try:
        if AirportCsvPolicies.is_valid_time(hhmm) and tzone is not None:
            loc_tz = pytz.timezone(str(tzone))

            # Tip: localize will convert into midnight
            # then add hours from that offset will be a safe way
            loc_dt = loc_tz.localize(
                datetime.strptime(VALID_DATE_FMT, date), is_dst=False
            )
            loc_dt += timedelta(hours=int(hhmm[:2]), minutes=int(hhmm[2:]))
            utc_dt = loc_dt.astimezone(pytz.utc)
            return utc_dt.strftime(VALID_DATETIME_FMT)
        return CANCELLED_FLIGHT_TIME_STR
    except (ValueError, pytz.UnknownTimeZoneError) as err:
        raise err


class UTCConversion(beam.PTransform):
    """Convert time into UTC"""

    def __init__(self, airports: beam.pvalue.AsDict):
        """

        :param airports: AsDict of airport seq id and AirportLocation
        """
        super().__init__()
        self._airports = airports

    def expand(self, pcoll: Any):  # pylint: disable=arguments-renamed
        """
        Takes pcollection of json string flights and convert time into UTC

        :param p_col:
        :return:
        """
        return (
            pcoll
            | "As Flight entity" >> beam.Map(Flight.of)
            | "Convert origin and dest timezone into utc"
            >> beam.FlatMap(tz_correct, self._airports)
        )
