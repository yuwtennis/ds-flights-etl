"""Deals with environment vars"""

from pydantic_settings import BaseSettings


class Settings(BaseSettings):
    """Reading env vars as model"""

    bq_dataset_name: str = "dsongcp"
    bq_flights_table_name: str = "flights"
    bq_tzcorr_table_name: str = "flights_tzcorr"
    bq_simevents_table_name: str = "flights_simevents"
    bq_streaming_events_table_name: str = "streaming_events"
    all_flights_path: str
    airport_csv_path: str
