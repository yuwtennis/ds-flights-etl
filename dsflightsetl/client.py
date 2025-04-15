""" Module orchestrating all client side tasks """
import csv

import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions, SetupOptions

from dsflightsetl.airport import Airport
from dsflightsetl.args import parse_args

AIRPORT_CSV_PATH = 'gs://dsongcp-452504-cf-staging/bts/airport.csv'


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
        _ = (pipeline
             | beam.io.ReadFromText(AIRPORT_CSV_PATH)
             | beam.Map(lambda line: next(csv.reader([line])))
             | beam.Map(lambda fields: (
                 fields[Airport.AIRPORT_SEQ_ID.value],
                 (fields[Airport.LATITUDE.value], fields[Airport.LONGITUDE.value])))
             )

    pipeline.run()
