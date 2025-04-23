"""Deals with in to/out from bigquery"""

from typing import Any
import apache_beam as beam


class WriteFlights(beam.PTransform):
    """Writing flights model to bigquery"""

    def __init__(self, table_name: str):
        super().__init__()
        self._table_name = table_name

    def expand(self, pcoll: Any):  # pylint: disable=arguments-renamed
        """

        :param pcoll:
        :return:
        """
        _ = pcoll | beam.io.WriteToBigQuery(
            table=self._table_name,
            write_disposition=beam.io.BigQueryDisposition.WRITE_TRUNCATE,
            create_disposition=beam.io.BigQueryDisposition.CREATE_NEVER,
        )


class ReadSamples(beam.PTransform):
    """Reading samples from flights table"""

    def __init__(self, table_name: str):
        super().__init__()
        self._table_name = table_name

    def expand(self, pcoll: Any) -> Any:  # pylint: disable=arguments-renamed
        """

        :param pcoll:
        :return:
        """
        query = f"SELECT * FROM {self._table_name} WHERE rand() < 0.001"

        return pcoll | beam.io.ReadFromBigQuery(query=query)
