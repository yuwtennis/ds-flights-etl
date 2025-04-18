"""Flight object"""

import json
from typing import Any

from pydantic import BaseModel

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
    def has_valid_num_of_fields(json_str: str):
        """

        :param json_str:
        :return:
        """
        return len(json.loads(json_str).keys()) == NUM_OF_FIELDS
