"""Tests tz_convert module"""

from typing import Any

import apache_beam as beam
from apache_beam.testing.test_pipeline import TestPipeline

from dsflightsetl.airport import AirportLocation
from dsflightsetl.flight import FlightPolicy
from dsflightsetl.tz_convert import UTCConversionFn


@beam.ptransform_fn
def load_airport_csv(pcoll: Any) -> Any:
    """

    :param p_col:
    :return:
    """
    return (
        pcoll
        | "Split csv into tuple" >> beam.Map(lambda line: line.split(","))
        | "As AirportLocation Entity"
        >> beam.Map(
            lambda arr: (
                arr[0],
                AirportLocation(
                    airport_seq_id=arr[0],
                    latitude=arr[1],
                    longitude=arr[2],
                    timezone=arr[3],
                ),
            )
        )
    )


def test_tz_convert_flight_sample(airport_samples, flight_sample):
    """
    Test pipeline for timezone conversion using single sample

    :param airport_samples:
    :param flight_samples:
    :return:
    """
    with TestPipeline() as pipeline:
        airports: Any = (
            pipeline
            | "Load airport location info" >> beam.io.ReadFromText(airport_samples)
            | "load" >> load_airport_csv()  # pylint: disable=no-value-for-parameter
        )

        _ = (
            pipeline
            | "Load flight samples" >> beam.Create([flight_sample])
            | "UTC conversion" >> UTCConversionFn(beam.pvalue.AsDict(airports))
        )


def test_tz_convert_flight_samples(airport_samples, flight_samples):
    """
    Test pipeline for timezone conversion using flight samples

    :param airport_samples:
    :param flight_samples:
    :return:
    """
    with TestPipeline() as pipeline:
        airports: Any = (
            pipeline
            | "Load airport location info" >> beam.io.ReadFromText(airport_samples)
            | "load" >> load_airport_csv()  # pylint: disable=no-value-for-parameter
        )

        _ = (
            pipeline
            | "Load flight samples" >> beam.io.ReadFromText(flight_samples)
            | "Filter out invalid element"
            >> beam.Filter(FlightPolicy.has_valid_num_of_fields)
            | "UTC conversion" >> UTCConversionFn(beam.pvalue.AsDict(airports))
        )
