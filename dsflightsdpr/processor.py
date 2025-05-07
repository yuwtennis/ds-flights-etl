"""Processors"""

import abc
from typing import Any

import apache_beam as beam
from apache_beam.io.gcp.internal.clients.bigquery import TableReference
from dsflightsdpr.flight import Flight, get_next_event
from dsflightsdpr.message import TopicResource
from dsflightsdpr.repository import ReadFlights, WriteFlights
from dsflightsdpr.tz_convert import UTCConvert


class Processor(metaclass=abc.ABCMeta):
    """Interface"""

    @abc.abstractmethod
    def read(self, pipeline: Any, airports: Any):
        """

        :param pipeline:
        :param airports:
        :return:
        """

    def write(
        self, flights: Any, all_flights_path: str, tbrs: dict[str, TableReference]
    ) -> None:
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
            >> beam.Map(lambda flight: flight.model_dump_json())
            | "Write out to gcs"
            >> beam.io.WriteToText(file_path_prefix=all_flights_path)
        )

        # To BQ as tz corrected events
        _ = (
            flights
            | "Serialize into dict of Flight model"
            >> beam.Map(lambda flight: flight.model_dump())
            | "Write out to tzcorr table" >> WriteFlights(tbrs["flight_tz_corr"])
        )

        # To BQ as simeevents with event types and time
        _ = (
            flights
            | "As event" >> beam.FlatMap(get_next_event)
            | "Serialize" >> beam.Map(lambda event: event.serialize())
            | "Write out to simevents table" >> WriteFlights(tbrs["simevents"])
        )


class Batch(Processor):
    """Batch"""

    def __init__(self, tbrs: dict[str, TableReference]):
        self._tbrs = tbrs

    def read(self, pipeline: Any, airports: Any) -> Any:
        """For batch"""
        return (
            pipeline
            | "Load flight samples as Json String" >> ReadFlights(self._tbrs["flights"])
            | "UTC conversion" >> UTCConvert(beam.pvalue.AsDict(airports))
        )

    def write(
        self, flights: Any, all_flights_path: str, tbrs: dict[str, TableReference]
    ):
        """

        :param flights:
        :param all_flights_path:
        :param tbrs:
        :return:
        """
        super().write(flights, all_flights_path, self._tbrs)


class Streaming(Processor):
    """Streaming"""

    def __init__(
        self, topic_resources: list[TopicResource], tbrs: dict[str, TableReference]
    ):
        self._topic_resource = topic_resources
        self._tbrs = tbrs

    def read(self, pipeline: Any, airports: Any) -> Any:
        """

        :param pipeline:
        :param airports:
        :return:
        """
        events = {}

        for topic in self._topic_resource:
            events[str(topic)] = (
                pipeline
                | f"read: {str(topic)}" >> beam.io.ReadFromPubSub(topic=str(topic))
                | f"parse: {str(topic)}" >> beam.Map(lambda s: Flight.from_row_dict(s))
            )

        return (resource for path, resource in events.items()) | beam.Flatten()

    def write(
        self, flights: Any, all_flights_path: str, tbrs: dict[str, TableReference]
    ):
        """

        :param flights:
        :param all_flights_path:
        :param tbrs:
        :return:
        """
        super().write(flights, all_flights_path, tbrs)

        # To BQ as simeevents with event types and time
        _ = flights | "Write out to streaming events table" >> WriteFlights(
            tbrs["streaming_events"]
        )
