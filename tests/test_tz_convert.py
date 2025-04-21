"""Tests tz_convert module"""

from typing import Any

import apache_beam as beam
from apache_beam.testing.test_pipeline import TestPipeline
from apache_beam.testing.util import assert_that, equal_to

from dsflightsetl.airport import AirportLocation
from dsflightsetl.flight import ValidFlights, Flight
from dsflightsetl.tz_convert import (
    UTCConvert,
    as_utc_with_standard_time_offset,
    tz_correct,
)


@beam.ptransform_fn
def load_airport_csv(pcoll: Any) -> Any:
    """

    :param p_col:
    :return:
    """
    return (
        pcoll
        | "Split csv into tuple" >> beam.Map(AirportLocation.from_airport_location_csv)
        | "As AirportLocation Entity"
        >> beam.Map(lambda airport: (airport.airport_seq_id, airport))
    )


def test_tz_convert_flight_samples(airport_location_samples, flight_samples):
    """
    Test pipeline for timezone conversion using flight samples

    :param airport_samples:
    :param flight_samples:
    :return:
    """
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
                | "Load flight samples" >> beam.io.ReadFromText(flight_samples)
                | "Valid flights only" >> ValidFlights()
                | "UTC conversion" >> UTCConvert(beam.pvalue.AsDict(airports))
                | "Extract"
                >> beam.Map(
                    lambda flight: (flight.dep_time, flight.dep_airport_tzoffset)
                )
            ),
            equal_to(
                [
                    ("2015-03-07 20:33:00", -21600.0),
                    ("2015-01-05 11:47:00", -21600.0),
                    ("2015-11-16 00:42:00", -21600.0),
                    ("2015-05-18 12:59:00", -21600.0),
                    ("2015-01-26 13:13:00", -25200.0),
                ]
            ),
        )


def test_as_utc():
    """Test utc conversion"""
    local_time = ("2015-05-18", "0000")
    expect_dtime = "2015-05-18 07:00:00"
    expect_tz_offset = -25200.0

    dtime, tz_offset = as_utc_with_standard_time_offset(
        local_time[0], local_time[1], "America/Denver"
    )

    assert dtime == expect_dtime
    assert tz_offset == expect_tz_offset


def test_tz_correct(airport_location_samples, flight_sample):
    """Test tz convert"""
    flight: Flight = Flight.of(flight_sample)
    airports = []
    expect_origin_airport_seq_id = 1013603
    expect_dep_time = "2015-03-07 20:33:00"
    expect_dep_tz = -21600.0
    expect_dest_airport_seq_id = 1129803
    expect_arr_time = "2015-03-07 21:33:00"
    expect_arr_tz = -21600.0

    with open(airport_location_samples, "r", encoding="UTF-8") as csv_file:
        for row in csv_file:
            airports.append(AirportLocation.from_airport_location_csv(row))

    locations = {location.airport_seq_id: location for location in airports}

    actual: list[Flight] = list(tz_correct(flight, locations))

    assert actual[0].origin_airport_seq_id == expect_origin_airport_seq_id
    assert actual[0].dep_time == expect_dep_time
    assert actual[0].dep_airport_tzoffset == expect_dep_tz
    assert actual[0].dest_airport_seq_id == expect_dest_airport_seq_id
    assert actual[0].arr_time == expect_arr_time
    assert actual[0].arr_airport_tzoffset == expect_arr_tz
