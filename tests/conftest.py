"""Fixtures , etc"""

import pytest


@pytest.fixture
def set_log_level_debug(monkeypatch):
    """Set loglevel to debug for testing"""
    monkeypatch.setenv("LOG_LEVEL", "DEBUG")


@pytest.fixture
def airport_location_samples():
    """Returns path as string"""
    return "tests/fixtures/extracted_airports_full.csv"


@pytest.fixture
def flight_samples():
    """Returns path as string"""
    return "tests/fixtures/flight_sample_minimal.json"


@pytest.fixture
def flight_sample(flight_samples):  # pylint: disable=redefined-outer-name
    """Returns a flight sample"""
    with open(flight_samples, "r", encoding="UTF-8") as descriptor:
        return descriptor.readline()


@pytest.fixture
def airport_samples():
    """Returns path as string"""
    return "tests/fixtures/airport_minimal.csv"
