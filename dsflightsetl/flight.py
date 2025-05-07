"""Flight object"""

import abc
import json
from abc import abstractmethod
from datetime import datetime
from enum import Enum
from typing import Any, Optional

from pydantic import BaseModel
from dsflightsetl import LOGGER

VALID_DATE_FMT = "%Y-%m-%d"
VALID_DATETIME_FMT = "%Y-%m-%d %H:%M:%S"


class Flight(BaseModel):
    """Flight entity"""

    fl_date: str
    unique_carrier: str
    origin_airport_seq_id: int
    origin: str
    dep_airport_tzoffset: Optional[float] = None
    dest_airport_seq_id: int
    dest: str
    arr_airport_tzoffset: Optional[float] = None
    crs_dep_time: str
    dep_time: str
    dep_delay: Optional[int] = None
    taxi_out: Optional[int] = None
    wheels_off: str
    wheels_on: str
    taxi_in: Optional[int] = None
    crs_arr_time: str
    arr_time: str
    arr_delay: Optional[int] = None
    cancelled: bool
    diverted: bool
    distance: float

    @classmethod
    def from_csv(cls, json_str: str) -> "Flight":  # pylint: disable=invalid-name
        """
        Convert Json string to Flight entity

        :param json_str:
        :return:
        """
        LOGGER.debug("flight: %s", json_str)
        return Flight(**normalize_dict_keys(json.loads(json_str)))

    @classmethod
    def from_row_dict(cls, data: dict[str, Any]) -> "Flight":
        """
        Convert bigquery row dictionary to Flight entity

        :param data:
        :return:
        """
        return Flight(**normalize_dict_keys(data))


class EventType(Enum):
    """Enum for event types"""

    DEPARTED = "departed"
    ARRIVED = "arrived"
    WHEELSOFF = "wheelsoff"


class Event(BaseModel, metaclass=abc.ABCMeta):
    """Base class for event entityt"""

    event_type: EventType
    event_time: str
    flight: Flight

    @abstractmethod
    def serialize(self) -> dict[str, Any]:
        """This method will be overrided"""

    def _to_bq_schema(self, fields: list[str]):
        """Serialize in to bq schema format"""

        data = self.flight.model_dump(include=set(fields))
        event_data = json.dumps(data)

        data["event_type"] = self.event_type.value
        data["event_time"] = self.event_time
        data["event_data"] = event_data

        return data


class Departed(Event):
    """Special entity including departed only attributes"""

    DEPARTED_EVENT_ATTRS: list[str] = [
        "fl_date",
        "unique_carrier",
        "origin_airport_seq_id",
        "origin",
        "dep_airport_tzoffset",
        "dest_airport_seq_id",
        "dest",
        "arr_airport_tzoffset",
        "crs_dep_time",
        "dep_time",
        "dep_delay",
        "crs_arr_time",
        "cancelled",
        "diverted",
    ]

    def serialize(self) -> dict[str, Any]:
        """Override"""
        return self._to_bq_schema(self.DEPARTED_EVENT_ATTRS)


class Arrived(Departed):
    """Special entity including departed and arrived attributes"""

    ARRIVED_EVENT_ATTRS: list[str] = [
        "taxi_out",
        "wheels_off",
        "wheels_on",
        "taxi_in",
        "arr_time",
        "arr_delay",
        "distance",
    ]

    def serialize(self) -> dict[str, Any]:
        """Override"""
        return self._to_bq_schema(self.DEPARTED_EVENT_ATTRS + self.ARRIVED_EVENT_ATTRS)


class Wheelsoff(Departed):
    """Special entity including departed and wheelsoff attributes"""

    WHEELSOFF_EVENT_ATTRS: list[str] = ["taxi_out", "wheels_off"]

    def serialize(self) -> dict[str, Any]:
        """Override"""
        return self._to_bq_schema(
            self.DEPARTED_EVENT_ATTRS + self.WHEELSOFF_EVENT_ATTRS
        )


def normalize_dict_keys(src_dict: dict[str, Any]):
    """

    :param src_dict:
    :return:
    """
    return {key.lower(): value for key, value in src_dict.items()}


def get_next_event(flight: Flight) -> Any:
    """
    Dispatches events accordingly

    :param flight:
    :return:
    """
    if FlightPolicy.is_valid_datetime(flight.dep_time, VALID_DATETIME_FMT):
        yield Departed(
            event_type=EventType.DEPARTED, event_time=flight.dep_time, flight=flight
        )

    if FlightPolicy.is_valid_datetime(flight.arr_time, VALID_DATETIME_FMT):
        yield Arrived(
            event_type=EventType.ARRIVED, event_time=flight.arr_time, flight=flight
        )

    if FlightPolicy.is_valid_datetime(flight.wheels_off, VALID_DATETIME_FMT):
        yield Wheelsoff(
            event_type=EventType.WHEELSOFF, event_time=flight.wheels_off, flight=flight
        )


class FlightPolicy:
    """Policies"""

    @staticmethod
    def is_valid_datetime(str_datetime: str, fmt: str):
        """Test if input is a valid datetime string"""
        try:
            datetime.strptime(str_datetime, fmt)
        except ValueError:
            return False

        return True
