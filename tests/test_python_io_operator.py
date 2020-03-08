import pytest

from airflow.utils import timezone
from airflow.utils.state import State

from dags.src.python_io_operator import PythonIoOperator

DEFAULT_OUTPUT = {"A": 45, "B": "18"}
DEFAULT_TASK_ID = "task"
OTHER_TASK_ID = "other_task"


def test_write(clean_session, mocked_storage, default_dag, default_date):
    """
    Make sure that storage implementation is called with the 
    output of the python callable. Also make sure it is stored
    with the correct name.
    """
    task = PythonIoOperator(
        task_id=DEFAULT_TASK_ID,
        python_callable=lambda: DEFAULT_OUTPUT,
        storage=mocked_storage,
        dag=default_dag,
    )

    dr = default_dag.create_dagrun(
        run_id="manual__",
        start_date=timezone.utcnow(),
        execution_date=default_date,
        state=State.RUNNING,
    )
    task.run(start_date=default_date, end_date=default_date)

    storage_call = mocked_storage.store_return_value.mock_calls[0]
    args, kwargs = storage_call[1], storage_call[2]

    assert kwargs["return_value"] == DEFAULT_OUTPUT

    # the naming logic needs to get delegated to the storage!
    assert (
        kwargs["output_name"]
        == f"{default_date}_{default_dag.safe_dag_id}_{DEFAULT_TASK_ID}.pkl"
    )

    task_instances = dr.get_task_instances()
    assert len(task_instances) == 1
    assert task_instances[0].state == State.SUCCESS


def test_read_name(clean_session, mocked_storage, default_dag, default_date):
    """
    Test that the storage tries to read the correct name.
    """
    task = PythonIoOperator(
        task_id=DEFAULT_TASK_ID,
        python_callable=lambda: DEFAULT_OUTPUT,
        input_task=OTHER_TASK_ID,
        storage=mocked_storage,
        dag=default_dag,
    )

    dr = default_dag.create_dagrun(
        run_id="manual__",
        start_date=timezone.utcnow(),
        execution_date=default_date,
        state=State.RUNNING,
    )

    task.run(start_date=default_date, end_date=default_date)

    storage_call = mocked_storage.load_inputs.mock_calls[0]
    args, kwargs = storage_call[1], storage_call[2]
    assert (
        kwargs["input_name"]
        == f"{default_date}_{default_dag.safe_dag_id}_{OTHER_TASK_ID}.pkl"
    )

    task_instances = dr.get_task_instances()
    assert len(task_instances) == 1
    assert task_instances[0].state == State.SUCCESS


def test_pass_kwargs(
    clean_session, mocked_storage, default_dag, get_instrumented_callable, default_date
):
    """
    Want to make sure that what is returned from the storage is 
    passed correctly into the python callable.
    """
    calls, f = get_instrumented_callable
    mocked_storage.load_inputs.return_value = DEFAULT_OUTPUT

    task = PythonIoOperator(
        task_id=DEFAULT_TASK_ID,
        python_callable=f,
        input_task=OTHER_TASK_ID,
        storage=mocked_storage,
        dag=default_dag,
    )

    dr = default_dag.create_dagrun(
        run_id="manual__",
        start_date=timezone.utcnow(),
        execution_date=default_date,
        state=State.RUNNING,
    )

    task.run(start_date=default_date, end_date=default_date)
    assert calls[0].kwargs == DEFAULT_OUTPUT

    task_instances = dr.get_task_instances()
    assert len(task_instances) == 1
    assert task_instances[0].state == State.SUCCESS


def test_pass_bad_data(
    clean_session, mocked_storage, default_dag, get_instrumented_callable, default_date
):
    """
    Make sure task will raise ValueError if it recieves something other than Dict[str,Any]
    """
    calls, f = get_instrumented_callable
    mocked_storage.load_inputs.return_value = "some_non_dict"

    task = PythonIoOperator(
        task_id=DEFAULT_TASK_ID,
        python_callable=f,
        input_task=OTHER_TASK_ID,
        storage=mocked_storage,
        dag=default_dag,
    )

    dr = default_dag.create_dagrun(
        run_id="manual__",
        start_date=timezone.utcnow(),
        execution_date=default_date,
        state=State.RUNNING,
    )

    # more explict error
    with pytest.raises(ValueError):
        task.run(start_date=default_date, end_date=default_date)

    task_instances = dr.get_task_instances()
    assert len(task_instances) == 1
    assert task_instances[0].state == State.FAILED
