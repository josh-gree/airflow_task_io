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
pipenv run python -m pytest tests
```

## Example usage

See `dags/sample_dag.py`
