"""Module orchestrating all client side tasks"""

import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions, SetupOptions

from dsflightsetl.airport import UsAirports, AirportLocation
from dsflightsetl.args import parse_args
from dsflightsetl.flight import ValidFlights, get_next_event
from dsflightsetl.repository import WriteFlights, ReadSamples
from dsflightsetl.setttings import Settings
from dsflightsetl.tz_convert import UTCConvert


def run(argv: list[str], save_main_sessions: bool = True) -> None:
    """

    :param save_main_sessions:
    :param argv:
    :return:
    """
    _, pipeline_args = parse_args(argv)  # type: ignore[attr-defined]
    options: PipelineOptions = PipelineOptions(pipeline_args)
    options.view_as(SetupOptions).save_main_session = save_main_sessions

    settings = Settings()

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
            | "Load flight samples"
            >> ReadSamples(
                f"{settings.bq_dataset_name}.{settings.bq_flights_table_name}"
            )
            | "Filter out invalid element" >> ValidFlights()  # TODO Remove cleansing
            | "UTC conversion" >> UTCConvert(beam.pvalue.AsDict(airports))
        )

        # To gcs
        _ = (
            flights
            | beam.Map(lambda flight: flight.model_dump_json())
            | beam.io.WriteToText(file_path_prefix=settings.all_flights_path)
        )

        # To BQ
        _ = (
            flights
            | beam.Map(lambda flight: flight.model_dump())
            | "To BQ"
            >> WriteFlights(
                f"{settings.bq_dataset_name}.{settings.bq_tzcorr_table_name}"
            )
        )

        # To BQ with simeevents
        _ = (
            flights
            | "As event" >> beam.Map(get_next_event)
            | "Serialize" >> beam.Map(lambda event: event.serialize())
            | "To BQ"
            >> WriteFlights(
                f"{settings.bq_dataset_name}.{settings.bq_simevents_table_name}"
            )
        )
    pipeline.run()
