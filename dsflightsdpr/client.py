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
from dsflightsdpr.flight import get_next_event
from dsflightsdpr.repository import WriteFlights, ReadFlights
from dsflightsdpr.setttings import Settings
from dsflightsdpr.tz_convert import UTCConvert


def run(argv: list[str], save_main_sessions: bool = True) -> None:
    """

    :param save_main_sessions:
    :param argv:
    :return:
    """
    _, pipeline_args = parse_args(argv)  # type: ignore[attr-defined]
    options: PipelineOptions = PipelineOptions(pipeline_args)
    options.view_as(SetupOptions).save_main_session = save_main_sessions
    project_id = options.view_as(GoogleCloudOptions).project

    settings = Settings()
    flights_table = TableReference(
        projectId=project_id,
        datasetId=settings.bq_dataset_name,
        tableId=settings.bq_flights_table_name,
    )
    flight_tz_corr_table = TableReference(
        projectId=project_id,
        datasetId=settings.bq_dataset_name,
        tableId=settings.bq_tzcorr_table_name,
    )
    flight_simevents_table = TableReference(
        projectId=project_id,
        datasetId=settings.bq_dataset_name,
        tableId=settings.bq_simevents_table_name,
    )

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

        flights = (
            pipeline
            | "Load flight samples as Json String" >> ReadFlights(flights_table)
            | "UTC conversion" >> UTCConvert(beam.pvalue.AsDict(airports))
        )

        # To gcs
        _ = (
            flights
            | "Serialize Flight into json string"
            >> beam.Map(lambda flight: flight.model_dump_json())
            | "Write out to gcs"
            >> beam.io.WriteToText(file_path_prefix=settings.all_flights_path)
        )

        # To BQ as tz corrected events
        _ = (
            flights
            | "Serialize into dict of Flight model"
            >> beam.Map(lambda flight: flight.model_dump())
            | "Write out to tzcorr table" >> WriteFlights(flight_tz_corr_table)
        )

        # To BQ as simeevents with event types and time
        _ = (
            flights
            | "As event" >> beam.FlatMap(get_next_event)
            | "Serialize" >> beam.Map(lambda event: event.serialize())
            | "Write out to simevents table" >> WriteFlights(flight_simevents_table)
        )
