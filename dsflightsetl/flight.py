"""Flight object"""

import json
from typing import Any

from pydantic import BaseModel
import apache_beam as beam
from dsflightsetl import LOGGER

NUM_OF_FIELDS = 19


class Flight(BaseModel):
    """Flight entity"""

    fl_date: str
    unique_carrier: str
    origin_airport_seq_id: int
    origin: str
    dest_airport_seq_id: int
    dest: str
    crs_dep_time: str
    dep_time: str
    dep_delay: int
    taxi_out: int
    wheels_off: str
    wheels_on: str
    taxi_in: int
    crs_arr_time: str
    arr_time: str
    arr_delay: int
    cancelled: bool
    diverted: bool
    distance: float

    @classmethod
    def of(cls, json_str: str) -> "Flight":  # pylint: disable=invalid-name
        """
        Convert Json string to Flight entity

        :param json_str:
        :return:
        """
        LOGGER.debug("flight: %s", json_str)
        return Flight(**normalize_dict_keys(json.loads(json_str)))


def normalize_dict_keys(src_dict: dict[str, Any]):
    """

    :param src_dict:
    :return:
    """
    return {key.lower(): value for key, value in src_dict.items()}


class FlightPolicy:
    """Policies for Flight entity"""

    @staticmethod
    def will_flight_depart(json_str: str):
        """

        :param json_str:
        :return:
        """
        return json.loads(json_str)["CANCELLED"] is False

    @staticmethod
    def has_flight_arrived(json_str: str):
        """

        :param json_str:
        :return:
        """
        return json.loads(json_str)["DIVERTED"] is False


class ValidFlights(beam.PTransform):
    """Valid flights"""

    def expand(self, pcoll: Any) -> Any:  # pylint: disable=arguments-renamed
        """

        :param pcoll:
        :return:
        """
        return pcoll | beam.Filter(
            lambda line: FlightPolicy.will_flight_depart(line)
            and FlightPolicy.has_flight_arrived(line)
        )
