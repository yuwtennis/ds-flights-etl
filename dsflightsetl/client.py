"""Module orchestrating all client side tasks"""

import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions, SetupOptions

from dsflightsetl.airport import UsAirports, AirportLocation
from dsflightsetl.args import parse_args
from dsflightsetl.flight import ValidFlights
from dsflightsetl.tz_convert import UTCConvert

AIRPORT_CSV_PATH = "gs://dsongcp-452504-cf-staging/bts/airport_minimal.csv"
FLIGHT_SAMPLES = "gs://dsongcp-452504-cf-staging/flights/ch4/flight_sample_mini.json"


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
        airports = (
            pipeline
            | beam.io.ReadFromText(AIRPORT_CSV_PATH)
            | "Only Us airports" >> UsAirports()
            | "To Airport location entities"
            >> beam.Map(lambda line: AirportLocation.from_airport_csv(line))
            | "As tuple"
            >> beam.Map(
                lambda airport_location: (
                    airport_location.airport_seq_id,
                    airport_location,
                )
            )
        )

        _ = (
            pipeline
            | "Load flight samples" >> beam.io.ReadFromText(FLIGHT_SAMPLES)
            | "Filter out invalid element" >> ValidFlights()
            | "UTC conversion" >> UTCConvert(beam.pvalue.AsDict(airports))
            | "Print out"
            >> beam.io.WriteToText(
                file_path_prefix="gs://dsongcp-452504-cf-staging/tmp/converted_events",
                file_name_suffix="csv",
            )
        )
    pipeline.run()
