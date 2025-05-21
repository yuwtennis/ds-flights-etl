"""Deals with environment vars"""

from typing import Optional

from pydantic_settings import BaseSettings


class Settings(BaseSettings):
    """Reading env vars as model"""

    bq_dataset_name: str = "dsongcp"
    bq_flights_table_name: str = "flights"
    bq_tzcorr_table_name: str = "flights_tzcorr"
    bq_simevents_table_name: str = "flights_simevents"
    bq_streaming_events_table_name: str = "streaming_events"
    bq_streaming_delays_table_name: str = "streaming_delays"
    all_flights_path: Optional[str] = None
    airport_csv_path: Optional[str] = None
