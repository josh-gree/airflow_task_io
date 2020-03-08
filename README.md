# PythonIoOperator

Code for an airflow operator that makes passing data between tasks simple! No need to rely on xcomm and storage in the airflow DB. Currently allows for storage to local disk, redis and S3.

## Setup

Install pyenv here -> https://github.com/pyenv/pyenv-installer

```bash
>> pyenv local 3.7.6
>> pip install pip --upgrade
>> pip install pipenv
>> pipenv install --dev
```

## Tests

```bash
>> pipenv run python -m pytest tests
```

## Example usage

See `dags/sample_dag.py`

## Trigger a DAG run

```bash
>> docker-compose up -d && sleep 30
>> docker exec -it airflow_task_io_webserver_1 /bin/bash -c 'airflow trigger_dag sample_dag -r test -e 2020-03-05'
```

You should then see some files created in `./storage`

To try with Redis - swap the storage class in `DEFAULT_ARGS` in `dags/sample_dag.py` to `REDIS_STORAGE` and then run

```bash
>> docker exec -it airflow_task_io_webserver_1 /bin/bash -c 'airflow trigger_dag sample_dag -r test -e 2020-03-06'
```

Then should see some new keys in redis!
