"""Deals with in to/out from bigquery"""

from typing import Any
import apache_beam as beam
from apache_beam.io.gcp.internal.clients.bigquery import TableReference
from dsflightsetl.flight import Flight


class ReadFlights(beam.PTransform):
    """Reading samples from flights table"""

    def __init__(self, table_spec: TableReference, as_samples: bool = False):
        super().__init__()
        self._table_spec = table_spec
        self._as_samples = as_samples

    def expand(self, pcoll: Any) -> Any:  # pylint: disable=arguments-renamed
        """

        :param pcoll:
        :return: PCollection with Flight entities
        """
        as_sample = "WHERE rand() < 0.001" if self._as_samples else ""

        query = f"""
        SELECT
            CAST(FL_DATE AS STRING) AS FL_DATE,
            UNIQUE_CARRIER,
            ORIGIN_AIRPORT_SEQ_ID,
            ORIGIN,
            DEST_AIRPORT_SEQ_ID,
            DEST,
            CRS_DEP_TIME,
            DEP_TIME,
            DEP_DELAY,
            TAXI_OUT,
            WHEELS_OFF,
            WHEELS_ON,
            TAXI_IN,
            CRS_ARR_TIME,
            ARR_TIME,
            ARR_DELAY,
            CANCELLED,
            DIVERTED,
            DISTANCE
        FROM {self._table_spec.projectId}.{self._table_spec.datasetId}.{self._table_spec.tableId} {as_sample}
        """

        return (
            pcoll
            | beam.io.ReadFromBigQuery(query=query, use_standard_sql=True)
            | beam.Map(Flight.from_row_dict)
        )


class WriteFlights(beam.PTransform):
    """Writing flights model to bigquery"""

    def __init__(
        self,
        table_spec: TableReference,
        write_disposition: str,
        create_disposition: str,
    ):
        super().__init__()
        self._table_spec = table_spec
        self._write_disposition = write_disposition
        self._create_disposition = create_disposition

    def expand(self, pcoll: Any):  # pylint: disable=arguments-renamed
        """

        :param pcoll:
        :return:
        """
        _ = pcoll | beam.io.WriteToBigQuery(
            table=self._table_spec,
            write_disposition=self._write_disposition,
            create_disposition=self._create_disposition,
        )
