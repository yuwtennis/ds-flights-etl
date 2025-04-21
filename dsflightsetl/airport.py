"""Class objects representing Airport CSV by BTS"""

import csv
from enum import Enum, auto
from typing import Optional, Any
import apache_beam as beam

import timezonefinder
from pydantic import BaseModel

from dsflightsetl import LOGGER


class Airport(Enum):
    """Airport Attributes"""

    AIRPORT_SEQ_ID = 0
    AIRPORT_ID = auto()
    AIRPORT = auto()
    DISPLAY_AIRPORT_NAME = auto()
    DISPLAY_AIRPORT_CITY_NAME_FULL = auto()
    AIRPORT_WAC_SEQ_ID2 = auto()
    AIRPORT_WAC = auto()
    AIRPORT_COUNTRY_NAME = auto()
    AIRPORT_COUNTRY_CODE_ISO = auto()
    AIRPORT_STATE_NAME = auto()
    AIRPORT_STATE_CODE = auto()
    AIRPORT_STATE_FIPS = auto()
    CITY_MARKET_SEQ_ID = auto()
    CITY_MARKET_ID = auto()
    DISPLAY_CITY_MARKET_NAME_FULL = auto()
    CITY_MARKET_WAC_SEQ_ID2 = auto()
    CITY_MARKET_WAC = auto()
    LAT_DEGREES = auto()
    LAT_HEMISPHERE = auto()
    LAT_MINUTES = auto()
    LAT_SECONDS = auto()
    LATITUDE = auto()
    LON_DEGREES = auto()
    LON_HEMISPHERE = auto()
    LON_MINUTES = auto()
    LON_SECONDS = auto()
    LONGITUDE = auto()
    UTC_LOCAL_TIME_VARIATION = auto()
    AIRPORT_START_DATE = auto()
    AIRPORT_THRU_DATE = auto()
    AIRPORT_IS_CLOSED = auto()
    AIRPORT_IS_LATEST = auto()


class AirportLocation(BaseModel):  # pylint: disable=too-few-public-methods
    """Airport location entity"""

    airport_seq_id: int
    latitude: float
    longitude: float
    timezone: Optional[str]

    @classmethod
    def from_airport_csv(
        cls, csv_line: str
    ) -> "AirportLocation":  # pylint: disable=invalid-name
        """

        :param csv_line:
        :return: AirportLocation
        """
        csv_obj: list[str] = next(csv.reader([csv_line]))
        lat = float(csv_obj[Airport.LATITUDE.value])
        lon = float(csv_obj[Airport.LONGITUDE.value])

        attrs: dict[str, Any] = {}

        attrs["airport_seq_id"] = int(csv_obj[Airport.AIRPORT_SEQ_ID.value])
        attrs["latitude"] = lat
        attrs["longitude"] = lon

        tz_finder = timezonefinder.TimezoneFinder()
        attrs["timezone"] = tz_finder.timezone_at(lng=lon, lat=lat)

        LOGGER.debug("attrs: %s, csvline: %s", attrs, csv_line)

        return cls(**attrs)

    @classmethod
    def from_airport_location_csv(cls, csv_line: str):
        """

        :param csv_line:
        :return:
        """
        csv_obj: list[str] = next(csv.reader([csv_line]))

        return cls(
            airport_seq_id=int(csv_obj[0]),
            latitude=float(csv_obj[1]),
            longitude=float(csv_obj[2]),
            timezone=csv_obj[3],
        )

    def to_csv(self):
        """To csv"""
        return f"{self.airport_seq_id},{self.latitude},{self.longitude},{self.timezone}"


class AirportCsvPolicies:
    """Policies for evaluating the csv"""

    @staticmethod
    def is_header(line: str) -> bool:
        """
        Asserts whether the string is the csv header

        :param line:
        :return:
        """
        return line.startswith("AIRPORT_SEQ_ID")

    @staticmethod
    def is_us_airport(line: str) -> bool:
        """
        Asserts whether the line includes timezones of US

        :param line:
        :return:
        """
        return "United States" in line

    @staticmethod
    def has_valid_coordinates(lat: str, lon: str) -> bool:
        """
        Asserts whether the given coordinates are valid

        :param lat:
        :param lon:
        :return:
        """
        try:
            float(lat)
            float(lon)
        except ValueError:
            return False

        return True

    @staticmethod
    def is_valid_time(hhmm: str) -> bool:
        """
        Asserts whether given hour minute is a valid value

        :param hhmm:
        :return:
        """
        return len(hhmm) > 0 and int(hhmm) < 2400


class UsAirports(beam.PTransform):
    """Return valid results from given csv line"""

    def expand(self, pcoll: Any) -> Any:  # pylint: disable=arguments-renamed
        """
        Filter out unwanted csv line and return Airport location

        :param pcoll:
        :return:
        """
        return pcoll | beam.Filter(
            lambda line: not AirportCsvPolicies.is_header(line)
            and AirportCsvPolicies.is_us_airport(line)
            and AirportCsvPolicies.has_valid_coordinates(
                line.split(",")[Airport.LATITUDE.value],
                line.split(",")[Airport.LONGITUDE.value],
            )
        )
