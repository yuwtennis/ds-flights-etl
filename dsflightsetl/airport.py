""" Class objects representing Airport CSV by BTS """
import csv
from enum import Enum, auto


class Airport(Enum):
    """ Airport Attributes """
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


class AirportLocation:  # pylint: disable=too-few-public-methods
    """ Object representing the location of airport """
    airport_seq_id: str
    latitude: str
    longitude: str

    @classmethod
    def of(cls, csv_line: str) -> 'AirportLocation':  # pylint: disable=invalid-name
        """

        :param csv_line:
        :return: AirportLocation
        """
        csv_obj: list[str] = next(csv.reader([csv_line]))

        inst = cls()
        inst.airport_seq_id = csv_obj[Airport.AIRPORT_SEQ_ID.value]
        inst.latitude = csv_obj[Airport.LATITUDE.value]
        inst.longitude = csv_obj[Airport.LONGITUDE.value]

        return inst

    def to_csv(self):
        """ To csv """
        return f"{self.airport_seq_id},{self.latitude},{self.longitude}"
