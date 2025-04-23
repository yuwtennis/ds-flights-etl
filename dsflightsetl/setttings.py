"""Deals with environment vars"""

from pydantic_settings import BaseSettings


class Settings(BaseSettings):
    """Reading env vars as model"""

    bq_dataset_name: str = "dsongcp"
    bq_flights_table_name: str = "flights"
    bq_tz_corr_table_name: str = "flights_tzcorr"
    bq_simevents_table_name: str = "flights_simevents"
    all_flights_path: str
    airport_csv_path: str
