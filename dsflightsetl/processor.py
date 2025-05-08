"""Processors"""

import abc
import json
from typing import Any

import apache_beam as beam
from apache_beam.io.gcp.internal.clients.bigquery import TableReference
from dsflightsetl.flight import Flight, get_next_event
from dsflightsetl.message import Subscription
from dsflightsetl.repository import ReadFlights, WriteFlights
from dsflightsetl.tz_convert import UTCConvert


class Processor(metaclass=abc.ABCMeta):
    """Interface"""

    @abc.abstractmethod
    def read(self, pipeline: Any):
        """

        :param pipeline:
        :param airports:
        :return:
        """

    @abc.abstractmethod
    def write(self, flights: Any) -> None:
        """

        :param flights:
        :return:
        """


class Batch(Processor):
    """Batch"""

    def __init__(
        self, tbrs: dict[str, TableReference], airports: Any, all_flights_path: str
    ):
        self._tbrs = tbrs
        self._airports = airports
        self._all_flights_path = all_flights_path

    def read(self, pipeline: Any) -> Any:
        """For batch"""
        return (
            pipeline
            | "Load flight samples as Json String" >> ReadFlights(self._tbrs["flights"])
            | "UTC conversion" >> UTCConvert(beam.pvalue.AsDict(self._airports))
        )

    def write(self, flights: Any):
        """

        :param flights:
        :param all_flights_path:
        :param tbrs:
        :return:
        """
        # To gcs
        _ = (
            flights
            | "Serialize Flight into json string"
            >> beam.Map(lambda flight: flight.stringify())
            | "Write out to gcs"
            >> beam.io.WriteToText(file_path_prefix=self._all_flights_path)
        )

        # To BQ as tz corrected events
        _ = (
            flights
            | "Serialize into dict of Flight model"
            >> beam.Map(lambda flight: flight.serialize())
            | "Write out to tzcorr table"
            >> WriteFlights(
                self._tbrs["flight_tz_corr"],
                beam.io.BigQueryDisposition.WRITE_TRUNCATE,
                beam.io.BigQueryDisposition.CREATE_NEVER,
            )
        )

        # To BQ as simeevents with event types and time
        _ = (
            flights
            | "As event" >> beam.FlatMap(get_next_event)
            | "Serialize" >> beam.Map(lambda event: event.serialize())
            | "Write out to simevents table"
            >> WriteFlights(
                self._tbrs["simevents"],
                beam.io.BigQueryDisposition.WRITE_TRUNCATE,
                beam.io.BigQueryDisposition.CREATE_NEVER,
            )
        )


class Streaming(Processor):
    """Streaming"""

    def __init__(
        self, topic_resources: list[Subscription], tbrs: dict[str, TableReference]
    ):
        self._subscription_resource = topic_resources
        self._tbrs = tbrs

    def read(self, pipeline: Any) -> Any:
        """

        :param pipeline:
        :return:
        """
        events = {}

        for sub in self._subscription_resource:
            events[str(sub)] = (
                pipeline
                | f"read: {sub.event_type}"
                >> beam.io.ReadFromPubSub(subscription=str(sub))
                | f"parse: {sub.event_type}"
                >> beam.Map(lambda s: Flight.from_row_dict(json.loads(s)))
            )

        return (resource for path, resource in events.items()) | beam.Flatten()

    def write(self, flights: Any):
        """

        :param flights:
        :return:
        """
        # To BQ as simeevents with event types and time
        _ = (
            flights
            | "Serialize into dict of Flight model"
            >> beam.Map(lambda flight: flight.serialize())
            | "Write out to streaming events table"
            >> WriteFlights(
                self._tbrs["streaming_events"],
                beam.io.BigQueryDisposition.WRITE_APPEND,
                beam.io.BigQueryDisposition.CREATE_NEVER,
            )
        )
