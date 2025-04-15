""" Unit test for Airport class """
from dsflightsetl.airport import Airport


def test_airport():
    """ Test airport indexes """
    assert Airport.AIRPORT_SEQ_ID.value == 0
    assert Airport.LATITUDE.value == 21
    assert Airport.LONGITUDE.value == 26
    assert Airport.AIRPORT_IS_LATEST.value == 31
