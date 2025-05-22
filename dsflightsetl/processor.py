"""Processors"""

import abc
import json
from typing import Any, Optional

import apache_beam as beam
import numpy as np
from apache_beam.io.gcp.internal.clients.bigquery import TableReference
from dsflightsetl.flight import get_next_event, EventType, StreamingDelay
from dsflightsetl.message import Subscription, SimEvent
from dsflightsetl.repository import ReadFlights, WriteFlights
from dsflightsetl.tz_convert import UTCConvert


class Processor(metaclass=abc.ABCMeta):
    """Interface"""

    @abc.abstractmethod
    def read(self, pipeline: beam.pipeline.Pipeline):
        """

        :param pipeline:
        :return:
        """

    @abc.abstractmethod
    def write(self, events: Any) -> None:
        """

        :param flights:
        :return:
        """


class TzCorr(Processor):
    """Batch"""

    def __init__(
        self,
        tbrs: dict[str, TableReference],
        airports: Any,
        all_flights_path: str,
        sample_rate: Optional[float] = None,
    ):
        self._tbrs = tbrs
        self._airports = airports
        self._all_flights_path = all_flights_path
        self._sample_rate = sample_rate

    def read(self, pipeline: beam.pipeline.Pipeline) -> Any:
        """For batch"""
        return (
            pipeline
            | "Load flight samples as Json String"
            >> ReadFlights(self._tbrs["flights"], self._sample_rate)
            | "UTC conversion" >> UTCConvert(beam.pvalue.AsDict(self._airports))
        )

    def write(self, events: Any):
        """

        :param events:
        :return:
        """
        # To gcs
        _ = (
            events
            | "Serialize Flight into json string"
            >> beam.Map(lambda flight: flight.stringify())
            | "Write out to gcs"
            >> beam.io.WriteToText(file_path_prefix=self._all_flights_path)
        )

        # To BQ as tz corrected events
        _ = (
            events
            | "Prepare for insert into flight_tz_corr"
            >> beam.Map(lambda flight: flight.serialize())
            | "Write out to tzcorr table"
            >> WriteFlights(
                self._tbrs["tz_corr"],
                beam.io.BigQueryDisposition.WRITE_TRUNCATE,
                beam.io.BigQueryDisposition.CREATE_NEVER,
            )
        )

        # To BQ as simeevents with event types and time
        _ = (
            events
            | "As event" >> beam.FlatMap(get_next_event)
            | "Prepare for insert into simevents"
            >> beam.Map(lambda event: event.serialize())
            | "Write out to simevents table"
            >> WriteFlights(
                self._tbrs["simevents"],
                beam.io.BigQueryDisposition.WRITE_TRUNCATE,
                beam.io.BigQueryDisposition.CREATE_NEVER,
            )
        )


class StreamAgg(Processor):
    """Streaming"""

    def __init__(
        self, topic_resources: list[Subscription], tbrs: dict[str, TableReference]
    ):
        self._subscription_resource = topic_resources
        self._tbrs = tbrs

    def read(self, pipeline: beam.pipeline.Pipeline) -> Any:
        """

        :param pipeline:
        :return:
        """
        events = {}

        # FIXME PubSub message should also treated as entity
        for sub in self._subscription_resource:
            events[str(sub)] = (
                pipeline
                | f"read: {sub.event_type}"
                >> beam.io.ReadFromPubSub(
                    subscription=str(sub), timestamp_attribute="EventTimeStamp"
                )
                | f"parse: {sub.event_type}"
                >> beam.Map(lambda s: SimEvent(event_data=json.loads(s)))
            )

        return (resource for path, resource in events.items()) | beam.Flatten()

    def write(self, events: Any):
        """

        :param events:
        :return:
        """
        # To BQ as simeevents with event types and time
        _ = (
            events
            | "Prepare for insert into streaming events"
            >> beam.Map(lambda s: s.event_data)
            | "Write out to streaming events table"
            >> WriteFlights(
                self._tbrs["streaming_events"],
                beam.io.BigQueryDisposition.WRITE_APPEND,
                beam.io.BigQueryDisposition.CREATE_NEVER,
            )
        )

    def write_streamimg_delays(self, streaming_delays: Any):
        """

        :param streaming_delays:
        :return:
        """
        _ = (
            streaming_delays
            | "Prepare for insert into streaming delays"
            >> beam.Map(lambda streaming_delay: streaming_delay.serialize())
            | "Write out to streaming delay table"
            >> WriteFlights(
                self._tbrs["streaming_delays"],
                beam.io.BigQueryDisposition.WRITE_APPEND,
                beam.io.BigQueryDisposition.CREATE_NEVER,
            )
        )

    def count_by_airport(self, all_events: Any) -> Any:
        """

        :param all_events:
        :return:
        """
        # Window length is 1 hour
        duration = 60 * 60

        # Emit every 5 min
        emit_frequency = 5 * 60

        return (
            all_events
            | "byairport" >> beam.Map(self._by_airport)
            | "window"
            >> beam.WindowInto(beam.window.SlidingWindows(duration, emit_frequency))
            | "group" >> beam.GroupByKey()
            | "compute" >> beam.Map(lambda x: self._mean_by_airport(x[0], x[1]))
        )

    def _by_airport(self, event: SimEvent) -> Any:
        """

        :param event:
        :return:
        """
        if event.event_data["event_type"] == EventType.DEPARTED.value:
            return event.event_data["origin"], event

        return event.event_data["dest"], event

    def _mean_by_airport(self, airport: str, events: Any) -> StreamingDelay:
        """

        :param airport:
        :param events:
        :return:
        """
        arrived = [
            event.event_data["arr_delay"]
            for event in events
            if event.event_data["event_type"] == EventType.ARRIVED.value
        ]
        avg_arr_delay = float(np.mean(arrived)) if len(arrived) > 0 else None

        departed = [
            event.event_data["dep_delay"]
            for event in events
            if event.event_data["event_type"] == EventType.DEPARTED.value
        ]
        avg_dep_delay = float(np.mean(departed)) if len(departed) > 0 else None

        num_flights = len(events)
        start_time = min(event.event_data["event_time"] for event in events)
        latest_time = max(event.event_data["event_time"] for event in events)

        return StreamingDelay(
            airport=airport,
            avg_arr_delay=avg_arr_delay,
            avg_dep_delay=avg_dep_delay,
            num_flights=num_flights,
            start_time=start_time,
            end_time=latest_time,
        )
