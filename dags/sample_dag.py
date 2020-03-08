import logging

import pandas as pd

from typing import Dict, Any

from airflow import DAG
from airflow.hooks.S3_hook import S3Hook

from src.python_io_operator import PythonIoOperator
from src.storage import S3Storage, LocalStorage, RedisStorage

# constructing the various storage classes that can be
# used by PythonIoOperator...

local_storage = LocalStorage("./storage")
s3_storage = S3Storage("bobs-special-bucket", S3Hook())
redis_storage = RedisStorage(host="redis", port=6379, db=0)
# local_pandas_csv_storage = LocalPandasCSVStorage("./storage")

args = {
    "owner": "airflow",
    "start_date": "2020-01-01",
    "storage": s3_storage,
}


def log_input_output(func):
    """
    Utility decorator - logs input and output of function
    """

    def wrapper(*args, **kwargs):
        logging.info(f"Args: {args}")
        logging.info(f"Kwargs: {kwargs}")
        return_value = func(*args, **kwargs)
        logging.info(f"Return Value: {return_value}")
        return return_value

    return wrapper


@log_input_output
def t1_callable_pandas(*, k: int) -> Dict[str, int]:
    """
    Sample function. Returns a dict with two largish pandas dfs
    consiting of random vals scaled by k
    
    Args:
        k (int): scaling value
    """
    return {
        "x": k * pd.DataFrame(pd.np.random.rand(100000, 10)),
        "y": k * pd.DataFrame(pd.np.random.rand(100000, 10)),
    }


@log_input_output
def t1_callable(*, k: int) -> Dict[str, int]:
    """
    Sample function. Returns a dict with two ints 10k and 100k
    
    Args:
        k (int): value to be scaled
    """
    return {
        "x": 10 * k,
        "y": 100 * k,
    }


@log_input_output
def t2_callable(*, x: Any, y: Any) -> Dict[str, Any]:
    """
    Sample function. Just returns a dict with single value
    which is the sum of x and y
    """
    return {"sum": x + y}


with DAG(dag_id="dag", default_args=args, schedule_interval=None) as dag:
    """
    [t1a] -> [t2a]
    [t1b] -> [t2b]

    Example of using PythonIoOperator;

    The tasks t2{a,b} take as kwargs x, y returned from t1{a,b} respectively.

    Storage method (S3, Local, Redis) specified in default_args!
    """
    t1a = PythonIoOperator(
        task_id="t1a", python_callable=t1_callable_pandas, op_kwargs={"k": 42}
    )

    t1b = PythonIoOperator(
        task_id="t1b", python_callable=t1_callable, op_kwargs={"k": 42}
    )

    t2a = PythonIoOperator(task_id="t2a", python_callable=t2_callable, input_task="t1a")
    t2b = PythonIoOperator(task_id="t2b", python_callable=t2_callable, input_task="t1b")

    t1a >> t2a
    t1b >> t2b
