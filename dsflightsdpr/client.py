"""Module orchestrating all client side tasks"""

import apache_beam as beam
from apache_beam.options.pipeline_options import (
    PipelineOptions,
    SetupOptions,
    GoogleCloudOptions,
)
from apache_beam.io.gcp.internal.clients.bigquery import TableReference

from dsflightsdpr.airport import UsAirports, AirportLocation
from dsflightsdpr.args import parse_args
from dsflightsdpr.processor import Batch, Streaming
from dsflightsdpr.message import TopicResource
from dsflightsdpr.setttings import Settings


def run(argv: list[str], save_main_sessions: bool = True) -> None:
    """

    :param save_main_sessions:
    :param argv:
    :return:
    """
    _, pipeline_args = parse_args(argv)  # type: ignore[attr-defined]
    options: PipelineOptions = PipelineOptions(pipeline_args)
    options.view_as(SetupOptions).save_main_session = save_main_sessions

    project_id: str = options.view_as(GoogleCloudOptions).project
    is_streaming: bool = options.view_as(GoogleCloudOptions).streaming

    settings = Settings()
    tbrs = {
        "flights": TableReference(
            projectId=project_id,
            datasetId=settings.bq_dataset_name,
            tableId=settings.bq_flights_table_name,
        ),
        "tz_corr": TableReference(
            projectId=project_id,
            datasetId=settings.bq_dataset_name,
            tableId=settings.bq_tzcorr_table_name,
        ),
        "simevents": TableReference(
            projectId=project_id,
            datasetId=settings.bq_dataset_name,
            tableId=settings.bq_simevents_table_name,
        ),
        "streaming_events": TableReference(
            projectId=project_id,
            datasetId=settings.bq_dataset_name,
            tableId=settings.bq_streaming_events_table_name,
        ),
    }

    with beam.Pipeline(options=options) as pipeline:

        airports = (
            pipeline
            | beam.io.ReadFromText(settings.airport_csv_path)
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

        if is_streaming:
            processor = Streaming(
                [
                    TopicResource(project_id=project_id, event_type=evt)
                    for evt in ["departed", "arrived"]
                ],
                tbrs,
            )
            flights = processor.read(pipeline, airports)
        else:
            processor = Batch(tbrs)
            flights = processor.read(pipeline, airports)

        processor.write(flights, settings.all_flights_path, tbrs)
