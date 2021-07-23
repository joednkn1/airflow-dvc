"""
Airflow sensor to wait for DVC files changes.

@Piotr Styczyński 2021
"""
import inspect
from typing import Callable, List, Optional, Union

from airflow.sensors.python import PythonSensor
from airflow.utils.decorators import apply_defaults

from airflow_dvc.dvc_hook import DVCHook
from airflow_dvc.logs import LOGS
from airflow_dvc.exceptions import add_log_exception_handler

FileListLike = Union[List[str], Callable[..., List[str]]]


class DVCExistenceSensor(PythonSensor):
    """
    Sensor that waits for the file/-s to be present in the DVC
    """

    dag_name: str  # Name of the running DAG (to compare DAG start and file timestamps)
    dvc_repo: str  # Git repo clone url
    files: FileListLike  # Files to watch for
    instance_context: str

    # Fields to apply Airflow templates
    template_fields = ['files']

    @apply_defaults
    def __init__(
        self,
        dvc_repo: str,
        files: FileListLike,
        dag,
        disable_error_message: bool = False,
        ignore_errors: bool = False,
        *args,
        **kwargs,
    ):
        """
        Airflow sensor will run exists(...) and check if the files exist.

        :param dvc_repo: Git clone URL for a repo with DVC configured
        :param files: Files to watch for
        :param dag: DAG object
        """
        super().__init__(add_log_exception_handler(
            self._poke,
            disable_error_message=disable_error_message,
            ignore_errors=ignore_errors,
        ), **kwargs)
        self.dag_name = dag.dag_id
        self.dvc_repo = dvc_repo
        self.files = files

        curframe = inspect.currentframe()
        caller = inspect.getouterframes(curframe, 2)[3]
        caller_path = caller.filename.split("/")[-1]
        self.instance_context = f"({caller_path}:{caller.lineno})"

        super(DVCExistenceSensor, self).__init__(*args, **kwargs)

    def _poke(self, *args, **kwargs):
        """
        Implementation of the Airflow interface to check if the DAG should proceed.
        """
        dvc = DVCHook(self.dvc_repo)
        files = self.files
        if callable(self.files):
            files = self.files(*args, **kwargs)
        # Check if given input files exist
        for file in files:
            if not dvc.exists(file):
                LOGS.dvc_existence_sensor.info(f"File {file} does not exist (sensor will wait)")
                # File do not exist so we do not proceed
                return False
        LOGS.dvc_existence_sensor.info(f"All files ({', '.join(files)}) exist so sensor will continue.")
        return True
