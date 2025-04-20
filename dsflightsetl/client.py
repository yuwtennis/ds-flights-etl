"""Module orchestrating all client side tasks"""

import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions, SetupOptions

from dsflightsetl.airport import UsAirports
from dsflightsetl.args import parse_args
from dsflightsetl.flight import FlightPolicy
from dsflightsetl.tz_convert import UTCConvert

AIRPORT_CSV_PATH = "gs://dsongcp-452504-cf-staging/bts/airport.csv"
FLIGHT_SAMPLES = "gs://dsongcp-452504-cf-staging/flights/ch4/flights_sample.json"


def run(argv: list[str], save_main_sessions: bool = True) -> None:
    """

    :param save_main_sessions:
    :param argv:
    :return:
    """
    _, pipeline_args = parse_args(argv)  # type: ignore[attr-defined]
    options: PipelineOptions = PipelineOptions(pipeline_args)
    options.view_as(SetupOptions).save_main_session = save_main_sessions

    with beam.Pipeline(options=options) as pipeline:
        airports = pipeline | beam.io.ReadFromText(AIRPORT_CSV_PATH) | UsAirports()

        _ = (
            pipeline
            | "Load flight samples" >> beam.io.ReadFromText(FLIGHT_SAMPLES)
            | "Filter out invalid element"
            >> beam.Filter(FlightPolicy.will_flight_depart)
            | "UTC conversion" >> UTCConvert(beam.pvalue.AsDict(airports))
        )
    pipeline.run()
