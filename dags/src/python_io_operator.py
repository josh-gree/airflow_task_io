"""
TODO
[summary]
"""
# pylint: disable=arguments-differ
from typing import Callable, Dict, Any
from airflow.operators.python_operator import PythonOperator
from airflow.utils.decorators import apply_defaults


class PythonIoOperator(PythonOperator):
    # TODO
    """
    [summary]
    """

    @apply_defaults
    def __init__(
        self,
        python_callable: Callable,
        *args,
        input_task: str = None,
        op_kwargs: Dict[str, Any] = None,
        storage=None,
        **kwargs,
    ):
        # TODO
        """
        [summary]

        Args:
            python_callable (Callable): [description]
            input_task (str, optional): [description]. Defaults to None.
            op_kwargs (Dict[str, Any], optional): [description]. Defaults to None.
            storage ([type], optional): [description]. Defaults to None.
        """

        self.storage = storage
        self.input_task = input_task

        super(PythonIoOperator, self).__init__(
            python_callable=python_callable, op_kwargs=op_kwargs, *args, **kwargs,
        )

    def execute(self, context):
        """
        Method overwritten from PythonOperator - called when task is run.

        We construct input/output names here because runtime information is required.

        We also call method to store return value here before actually returning anything.
        """
        # TODO
        # delegate to storage
        # input_name, output_name = self.storage.get_names(context)

        task_instance = context["ti"]
        execution_date = task_instance.execution_date
        dag_id = context["dag"].safe_dag_id

        base_name = f"{execution_date}_{dag_id}"
        output_name = f"{base_name}_{task_instance.task.task_id}.pkl"

        if self.input_task:
            input_name = f"{base_name}_{self.input_task}.pkl"
            return_value = self.execute_callable(input_name)
        else:
            return_value = self.execute_callable()

        self.storage.store_return_value(
            return_value=return_value, output_name=output_name
        )

        return return_value

    def execute_callable(self, input_name: str = None):
        """
        Method that actually calls the python callable with possibly retreived
        kwargs from S3.

        Args:
            input_name (str, optional): The S3 object name where input kwargs are stored. Defaults to None.
        """
        if input_name:
            inputs = self.storage.load_inputs(input_name=input_name)
            self.op_kwargs.update(inputs)

        return self.python_callable(*self.op_args, **self.op_kwargs)
