import pytest
from lib.Utils import get_spark_session

@pytest.fixture
def spark():
    "creates spark session"
    spark_session =  get_spark_session("LOCAL")
    yield spark_session
    spark_session.stop()