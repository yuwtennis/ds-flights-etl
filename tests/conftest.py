"""Fixtures , etc"""

import pytest


@pytest.fixture
def set_log_level_debug(monkeypatch):
    """Set loglevel to debug for testing"""
    monkeypatch.setenv("LOG_LEVEL", "DEBUG")


@pytest.fixture
def airport_location_samples():
    """Returns path as string"""
    return "tests/fixtures/extracted_airports.csv"


@pytest.fixture
def flight_samples():
    """Returns path as string"""
    return "tests/fixtures/flights_sample.json"


@pytest.fixture
def flight_sample():
    """Returns a flight sample"""
    return (
        "{"
        '"FL_DATE":"2015-03-07",'
        '"UNIQUE_CARRIER":"MQ",'
        '"ORIGIN_AIRPORT_SEQ_ID":"1013603",'
        '"ORIGIN":"ABI",'
        '"DEST_AIRPORT_SEQ_ID":"1129803",'
        '"DEST":"DFW",'
        '"CRS_DEP_TIME":"1430",'
        '"DEP_TIME":"1433",'
        '"DEP_DELAY":3,'
        '"TAXI_OUT":7,'
        '"WHEELS_OFF":"1440",'
        '"WHEELS_ON":"1513",'
        '"TAXI_IN":20,'
        '"CRS_ARR_TIME":"1520",'
        '"ARR_TIME":"1533",'
        '"ARR_DELAY":13,'
        '"CANCELLED":false,'
        '"DIVERTED":false,'
        '"DISTANCE":"158.00"}'
    )


@pytest.fixture
def airport_samples():
    """Returns path as string"""
    return "tests/fixtures/airport.csv"
