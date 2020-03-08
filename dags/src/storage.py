import cloudpickle
import pickle
import redis

import pandas as pd

from typing import Dict, Any, Callable
from pathlib import Path


from abc import ABC, abstractmethod


class Storage(ABC):
    @abstractmethod
    def store_return_value(self, *, return_value: Dict[str, Any]) -> None:
        # TODO
        """
        [summary]
        
        Args:
            ABC ([type]): [description]
            return_value (Dict[str, Any]): [description]
        
        Raises:
            NotImplementedError: [description]
        """
        raise NotImplementedError

    @abstractmethod
    def load_inputs(self, *, input_name: str) -> Dict[str, Any]:
        # TODO
        """
        [summary]
        
        Args:
            input_name (str): [description]
        
        Raises:
            NotImplementedError: [description]
        
        Returns:
            Dict[str, Any]: [description]
        """
        raise NotImplementedError

    # TODO
    @abstractmethod
    def get_output_name(self):
        raise NotImplementedError

    @abstractmethod
    def get_input_name(self):
        raise NotImplementedError


class LocalStorage(Storage):
    def __init__(self, storage_location):
        # TODO
        """
        [summary]
        
        Args:
            Storage ([type]): [description]
            storage_location ([type]): [description]
        """
        storage_location = Path(storage_location)
        storage_location.mkdir(parents=True, exist_ok=True)

        self.storage_location = storage_location

    def store_return_value(
        self, *, return_value: Dict[str, Any], output_name: str
    ) -> None:
        # TODO
        """
        [summary]
        
        Args:
            return_value (Dict[str, Any]): [description]
            output_name (str): [description]
        
        Raises:
            ValueError: [description]
        
        Returns:
            [type]: [description]
        """
        try:
            pickled_return_value = cloudpickle.dumps(return_value)
        except Exception as e:
            raise ValueError("Unable to pickle return value - raised exception: {e}")

        output_file = self.storage_location / output_name
        with output_file.open("wb") as f:
            f.write(pickled_return_value)

    def load_inputs(self, *, input_name: str) -> Dict[str, Any]:
        # TODO
        """
        [summary]
        
        Args:
            input_name (str): [description]
        
        Raises:
            ValueError: [description]
            ValueError: [description]
        
        Returns:
            Dict[str, Any]: [description]
        """
        try:
            input_file = self.storage_location / input_name
            with input_file.open("rb") as f:
                op_kwargs = pickle.loads(f.read())
        except pickle.UnpicklingError:
            raise ValueError("Unable to load file from local storage!")

        if not isinstance(op_kwargs, dict):
            raise ValueError(
                "Object returned from local storage is not a dict object - cannot pass as kwargs to python callable!"
            )

        return op_kwargs

    # TODO
    def get_input_name(self):
        return super().get_input_name()

    def get_output_name(self):
        return super().get_output_name()


# TODO
class LocalPandasCSVStorage(Storage):
    def __init__(self, storage_location):

        storage_location = Path(storage_location)
        storage_location.mkdir(parents=True, exist_ok=True)

        self.storage_location = storage_location

    def store_return_value(
        self, *, return_value: Dict[str, pd.DataFrame], output_name: str
    ) -> None:
        for key, df in return_value.items():
            print("saving DFF!")
            f = self.storage_location / f"{output_name}_{key}.csv"
            df.to_csv(str(f), index=False)

    def load_inputs(self, *, input_name: str) -> Dict[str, pd.DataFrame]:

        csvs = self.storage_location.glob(f"{input_name}_*.csv")
        keys = [str(csv).split(".")[0].split("_")[-1] for csv in csvs]
        print(keys)
        # dfs = {key: pd.read_csv(str(csv)) for csv, key in zip(csvs, keys)}

        print(dfs)

    # TODO
    def get_input_name(self):
        return super().get_input_name()

    def get_output_name(self):
        return super().get_output_name()


class S3Storage(Storage):
    def __init__(self, storage_bucket, s3_hook):
        # TODO
        """
        [summary]
        
        Args:
            Storage ([type]): [description]
            storage_bucket ([type]): [description]
            s3_hook ([type]): [description]
        """

        self.storage_bucket = storage_bucket
        self.s3_hook = s3_hook

    def store_return_value(
        self, *, return_value: Dict[str, Any], output_name: str
    ) -> None:
        # TODO
        """
        Stores the return value of the python callable to S3                
        
        Args:
            return_value (Dict[str, Any]): Return value of the python callable
            output_name (str): The S3 object name where python callable return value is stored.
        
        Raises:
            ValueError: When return value is not able to be pickled!
        """
        try:
            pickled_return_value = cloudpickle.dumps(return_value)
        except Exception as e:
            raise ValueError("Unable to pickle return value - raised exception: {e}")

        self.s3_hook.load_bytes(
            pickled_return_value,
            key=output_name,
            bucket_name=self.storage_bucket,
            replace=True,
        )

    def load_inputs(self, *, input_name: str) -> Dict[str, Any]:
        # TODO
        """
        Load object from S3 and unpickle! Make sure the object is a python dict!
        
        Args:
            input_name (str): The S3 object name where input kwargs are loaded from.
        
        Raises:
            ValueError: When unable to unpickle object from S3
            ValueError: When loaded object from S3 is not a python dict
        
        Returns:
            Dict[str, Any]: Dict loaded from S3 will be used as kwargs to python callable
        """

        obj = self.s3_hook.get_key(key=input_name, bucket_name=self.storage_bucket)

        try:
            op_kwargs = pickle.loads(obj.get()["Body"].read())
        except pickle.UnpicklingError:
            raise ValueError("Unable to load s3 object!")

        if not isinstance(op_kwargs, dict):
            raise ValueError(
                "Object returned from S3 is not a dict object - cannot pass as kwargs to python callable!"
            )

        return op_kwargs

    # TODO
    def get_input_name(self):
        return super().get_input_name()

    def get_output_name(self):
        return super().get_output_name()


class RedisStorage(Storage):
    def __init__(self, host, port, db):
        # TODO
        """
        [summary]
        
        Args:
            Storage ([type]): [description]
            host ([type]): [description]
            port ([type]): [description]
            db ([type]): [description]
        """
        self.host = host
        self.port = port
        self.db = db

    def store_return_value(
        self, *, return_value: Dict[str, Any], output_name: str
    ) -> None:
        # TODO
        """
        [summary]
        
        Args:
            return_value (Dict[str, Any]): [description]
            output_name (str): [description]
        
        Raises:
            ValueError: [description]
        
        Returns:
            [type]: [description]
        """
        try:
            pickled_return_value = cloudpickle.dumps(return_value)
        except Exception as e:
            raise ValueError("Unable to pickle return value - raised exception: {e}")

        r = redis.Redis(host=self.host, port=self.port, db=self.db)
        r.set(output_name, pickled_return_value)

    def load_inputs(self, *, input_name: str) -> Dict[str, Any]:
        # TODO
        """
        [summary]
        
        Args:
            input_name (str): [description]
        
        Raises:
            ValueError: [description]
            ValueError: [description]
        
        Returns:
            Dict[str, Any]: [description]
        """
        try:
            r = redis.Redis(host=self.host, port=self.port, db=self.db)
            value = r.get(input_name)
            op_kwargs = pickle.loads(value)
        except pickle.UnpicklingError:
            raise ValueError("Unable to load s3 object!")

        if not isinstance(op_kwargs, dict):
            raise ValueError(
                "Object returned from S3 is not a dict object - cannot pass as kwargs to python callable!"
            )

        return op_kwargs

    # TODO
    def get_input_name(self):
        return super().get_input_name()

    def get_output_name(self):
        return super().get_output_name()
