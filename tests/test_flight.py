"""Test flight module"""

from typing import Any

import apache_beam as beam
from apache_beam.testing.test_pipeline import TestPipeline
from apache_beam.testing.util import assert_that, equal_to, is_not_empty

from dsflightsdpr.flight import get_next_event
from dsflightsdpr.tz_convert import UTCConvert
from tests.test_tz_convert import load_airport_csv


def test_get_next_event(flight_entity, airport_location_samples):
    """Test get_next_event"""
    with TestPipeline() as pipeline:
        airports: Any = (
            pipeline
            | "Load airport location info"
            >> beam.io.ReadFromText(airport_location_samples)
            | "load" >> load_airport_csv()  # pylint: disable=no-value-for-parameter
        )
        assert_that(
            (
                pipeline
                | beam.Create([flight_entity])
                | "UTC conversion" >> UTCConvert(beam.pvalue.AsDict(airports))
                | "Next Event" >> beam.FlatMap(get_next_event)
            ),
            equal_to(
                ["2015-03-07 20:33:00", "2015-03-07 21:33:00", "2015-03-07 20:40:00"],
                lambda expected, actual: expected == actual.event_time,
            ),
        )


def test_event_serialize(flight_entity, airport_location_samples):
    """Test serialize"""
    with TestPipeline() as pipeline:
        airports: Any = (
            pipeline
            | "Load airport location info"
            >> beam.io.ReadFromText(airport_location_samples)
            | "load" >> load_airport_csv()  # pylint: disable=no-value-for-parameter
        )
        assert_that(
            (
                pipeline
                | beam.Create([flight_entity])
                | "UTC conversion" >> UTCConvert(beam.pvalue.AsDict(airports))
                | "Next Event" >> beam.FlatMap(get_next_event)
                | "Serialize" >> beam.Map(lambda event: event.serialize())
            ),
            is_not_empty(),
        )
