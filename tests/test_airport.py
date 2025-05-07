"""Unit test for Airport class"""

import apache_beam as beam
from apache_beam.testing.test_pipeline import TestPipeline
from dsflightsdpr.airport import Airport, UsAirports, AirportLocation


def test_airport():
    """Test airport indexes"""
    assert Airport.AIRPORT_SEQ_ID.value == 0
    assert Airport.LATITUDE.value == 21
    assert Airport.LONGITUDE.value == 26
    assert Airport.AIRPORT_IS_LATEST.value == 31


def test_airport_location(airport_samples):
    """Test airport entities"""
    with TestPipeline() as pipeline:
        _ = (
            pipeline
            | beam.io.ReadFromText(airport_samples)
            | "Only US airports" >> UsAirports()
            | "To Airport location entities"
            >> beam.Map(lambda line: AirportLocation.from_airport_csv(line))
        )
