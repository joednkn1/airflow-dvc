"""
Example usage of the DVC update sensor in the Airflow DAG.

@Piotr Styczyński 2021
"""
import os
from datetime import datetime

from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow_dvc import DVCUpdateSensor

with DAG(
    "dvc_update_sensor_example",
    description="Another tutorial DAG",
    start_date=datetime(2017, 3, 20),
    catchup=False,
) as dag:

    dummy_task = DummyOperator(task_id="dummy_task", dag=dag)

    sensor_task = DVCUpdateSensor(
        task_id="dvc_sensor_task",
        dag=dag,
        dvc_repo=os.environ["REPO"],
        files=["data/1.txt"],
    )

    task = BashOperator(
        task_id="task_triggered_by_sensor",
        bash_command='echo "OK" && ( echo $[ ( $RANDOM % 30 )  + 1 ] > meowu.txt ) && cat meowu.txt',
    )

    dummy_task >> sensor_task >> task
