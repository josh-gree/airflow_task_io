import os
import subprocess
import shutil
import pytest

from pathlib import Path
from unittest import mock

from airflow.utils import timezone
from airflow.models import DAG, DagRun, TaskInstance as TI
from airflow.utils.db import create_session

DEFAULT_DATE = timezone.datetime(2016, 1, 1)


class Call:
    def __init__(self, *args, **kwargs):
        self.args = args
        self.kwargs = kwargs


@pytest.fixture
def get_instrumented_callable():
    """
    Cant use a mock - not piclablele!?
    """
    calls_collection = []

    def recording_function(*args, **kwargs):
        calls_collection.append(Call(*args, **kwargs))

    return calls_collection, recording_function


@pytest.fixture(scope="session", autouse=True)
def setup_airflow():
    """
    Intialise the airflow backend sqlite DB and then
    clean up once testing session completes.
    """
    p = subprocess.Popen(["airflow", "initdb"])
    p.wait()

    yield

    airflow_home = Path(os.environ["AIRFLOW_HOME"])
    shutil.rmtree(airflow_home)


@pytest.fixture
def clean_session():
    """
    Clean up all tasks and dag runs at start and end of each test
    """
    with create_session() as session:
        session.query(DagRun).delete()
        session.query(TI).delete()

    yield

    with create_session() as session:
        session.query(DagRun).delete()
        session.query(TI).delete()


@pytest.fixture
def mocked_storage():
    """
    Fake storage mock
    """
    return mock.MagicMock()


@pytest.fixture
def default_dag():
    """
    Default dag object
    """
    dag = DAG("test_dag", default_args={"owner": "airflow", "start_date": DEFAULT_DATE})
    return dag


@pytest.fixture
def default_date():
    return DEFAULT_DATE
