"""Module orchestrating all client side tasks"""

from typing import Optional

import apache_beam as beam
from apache_beam.options.pipeline_options import (
    PipelineOptions,
    SetupOptions,
    GoogleCloudOptions,
    StandardOptions,
)
from apache_beam.io.gcp.internal.clients.bigquery import TableReference

from dsflightsetl.airport import UsAirports, AirportLocation
from dsflightsetl.args import parse_args
from dsflightsetl.processor import Processor, StreamAgg, TzCorr
from dsflightsetl.message import Subscription
from dsflightsetl.setttings import Settings


def run(argv: list[str], save_main_sessions: bool = True) -> None:
    """

    :param save_main_sessions:
    :param argv:
    :return:
    """
    k_args, pipeline_args = parse_args(argv)  # type: ignore[attr-defined]
    options: PipelineOptions = PipelineOptions(pipeline_args)
    options.view_as(SetupOptions).save_main_session = save_main_sessions

    project_id: str = options.view_as(GoogleCloudOptions).project
    is_streaming: bool = options.view_as(StandardOptions).streaming

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
        "streaming_delays": TableReference(
            projectId=project_id,
            datasetId=settings.bq_dataset_name,
            tableId=settings.bq_streaming_delays_table_name,
        ),
    }

    pipeline = beam.Pipeline(options=options)

    processor: Optional[Processor] = None
    if is_streaming:
        processor = StreamAgg(
            [
                Subscription(project_id=project_id, event_type=evt)
                for evt in ["departed", "arrived"]
            ],
            tbrs,
        )
    else:
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
        if settings.all_flights_path is None:
            raise RuntimeError()
        processor = TzCorr(
            tbrs, airports, settings.all_flights_path, k_args.sample_rate
        )

    flights = processor.read(pipeline)
    processor.write(flights)

    if is_streaming and isinstance(processor, StreamAgg):
        streaming_delays = processor.count_by_airport(flights)
        processor.write_streamimg_delays(streaming_delays)

    pipeline.run()
