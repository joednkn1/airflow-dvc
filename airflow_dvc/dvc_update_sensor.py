"""
Airflow sensor to wait for DVC files changes.

@Piotr Styczyński 2021
"""
import inspect
import datetime
from datetime import datetime as time
from typing import List

from airflow.models.dagrun import DagRun
from airflow.sensors.python import PythonSensor

from airflow_dvc.dvc_hook import DVCHook
from airflow_dvc.logs import LOGS
from airflow_dvc.exceptions import add_log_exception_handler

TEMPLATE_FIELDS = ["templates_dict", "op_args", "op_kwargs", "files"]


class DVCUpdateSensor(PythonSensor):
    """
    Sensor that waits until the given path will be updated in DVC.
    """

    dag_name: str  # Name of the running DAG (to compare DAG start and file timestamps)
    dvc_repo: str  # Git repo clone url
    files: List[str]  # Files to watch for
    instance_context: str

    # Fields to apply Airflow templates
    template_fields = TEMPLATE_FIELDS

    def __init__(
            self,
            dvc_repo: str,
            files: List[str],
            dag,
            disable_error_message: bool = False,
            ignore_errors: bool = False,
            *args,
            **kwargs,
    ):
        """
        Airflow sensor will compare timestamp of the current DAG run and the paths of files
        tracked in DVC given as an input parameter.

        :param dvc_repo: Git clone URL for a repo with DVC configured
        :param files: Files to watch for
        :param dag: DAG object
        """
        super().__init__(
            **kwargs,
            python_callable=add_log_exception_handler(
                self._poke,
                disable_error_message=disable_error_message,
                ignore_errors=ignore_errors,
            ),
        )
        self.dag_name = dag.dag_id
        self.dvc_repo = dvc_repo
        self.files = files

        curframe = inspect.currentframe()
        caller = inspect.getouterframes(curframe, 2)[3]
        caller_path = caller.filename.split("/")[-1]
        self.instance_context = f"({caller_path}:{caller.lineno})"
        self.template_fields = TEMPLATE_FIELDS

    def _poke(self, **kwargs):
        """
        Implementation of the Airflow interface to check if the DAG should proceed.
        """
        dag_runs = DagRun.find(dag_id=self.dag_name)
        length = len(dag_runs)
        # Query the latest start date of the DAG

        last_start_date = (
            dag_runs[length - 1].start_date.replace(tzinfo=None)
            if length != 0
            else time.now()
        )

        if length == 0:
            LOGS.dvc_update_sensor.info(f"There is no running DAG, used time.now() as a date of the last DAG start.")

        update = False
        dvc = DVCHook(self.dvc_repo)
        # Check modification dates of the given files
        for file in self.files:
            if file in dvc.list_files():
                modified_date = dvc.modified_date([file, ]) - datetime.timedelta(
                    minutes=dvc.modified_date(
                        [
                            file,
                        ]
                    ).minute
                            % 10,
                    seconds=dvc.modified_date(
                        [
                            file,
                        ]
                    ).second,
                    microseconds=dvc.modified_date(
                        [
                            file,
                        ]
                    ).microsecond,
                )
                last_start_date -= datetime.timedelta(
                    minutes=last_start_date.minute % 10,
                    seconds=last_start_date.second,
                    microseconds=last_start_date.microsecond,
                )
                LOGS.dvc_update_sensor.info(
                    f"Current date = {last_start_date} vs. file modified date {modified_date}"
                )
                if modified_date >= last_start_date:
                    LOGS.dvc_update_sensor.info("DVC sensor is active.")
                    update = True
                    break
            else:
                LOGS.dvc_update_sensor.info(f"Cannot find a file: {file}, waiting for upload.")
        return update
