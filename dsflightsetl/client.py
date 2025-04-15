"""Module orchestrating all client side tasks"""

import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions, SetupOptions

from dsflightsetl.airport import AirportLocation, AirportCsvPolicies
from dsflightsetl.args import parse_args

AIRPORT_CSV_PATH = "gs://dsongcp-452504-cf-staging/bts/airport.csv"


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
            | beam.Filter(
                lambda line: not AirportCsvPolicies.is_header(line)
                and AirportCsvPolicies.is_us_airport
            )
            | beam.Map(lambda line: AirportLocation.of(line))
        )

        _ = (
            airports
            | beam.Map(lambda airport: airport.to_csv())
            | beam.io.WriteToText(
                file_path_prefix="gs://dsongcp-452504-cf-staging/tmp/extracted_airports",
                file_name_suffix=".csv",
            )
        )

    pipeline.run()
