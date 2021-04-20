"""
Airflow sensor to wait for DVC files changes.

@Piotr Styczyński 2021
"""
import inspect
from typing import List

from airflow.models.dagrun import DagRun
from airflow.sensors.base import BaseSensorOperator
from airflow.utils.decorators import apply_defaults
from airflow_dvc.dvc_hook import DVCHook


class DVCUpdateSensor(BaseSensorOperator):
    """
    Sensor that waits until the given path will be updated in DVC.
    """

    dag_name: str  # Name of the running DAG (to compare DAG start and file timestamps)
    dvc_repo: str  # Git repo clone url
    files: List[str]  # Files to watch for
    instance_context: str

    @apply_defaults
    def __init__(
        self,
        dvc_repo: str,
        files: List[str],
        dag,
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
        self.dag_name = dag.dag_id
        self.dvc_repo = dvc_repo
        self.files = files

        curframe = inspect.currentframe()
        caller = inspect.getouterframes(curframe, 2)[3]
        caller_path = caller.filename.split("/")[-1]
        self.instance_context = f"({caller_path}:{caller.lineno})"

        super(DVCUpdateSensor, self).__init__(*args, **kwargs)

    def poke(self, context):
        """
        Implementation of the Airflow interface to check if the DAG should proceed.
        """
        dag_runs = DagRun.find(dag_id=self.dag_name)
        length = len(dag_runs)
        # Query the latest start date of the DAG
        last_start_date = dag_runs[length - 1].start_date.replace(tzinfo=None)

        update = False
        dvc = DVCHook(self.dvc_repo)
        # Check modification dates of the given files
        for file in self.files:
            print(
                f"Current date = {last_start_date} vs. file modified date {dvc.modified_date(file)}"
            )
            if dvc.modified_date(file) >= last_start_date:
                update = True
                break
        return update
