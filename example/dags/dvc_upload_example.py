"""
Example usage of the DVC upload operator (uploading a string) in an advanced Airflow DAG.

@Piotr Styczyński 2021
"""
import os
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python import PythonOperator
from airflow.version import version

from airflow_dvc import DVCStringUpload, DVCUpdateOperator


def custom_python_task_handler(ts, **kwargs):
    print(
        f"I am task number {kwargs['task_number']}. "
        f"This DAG Run execution date is {ts} and the "
        f"current time is {datetime.now()}"
    )
    print(
        "Here is the full DAG Run context. It is available "
        "because provide_context=True"
    )
    print(kwargs)


# Default settings applied to all tasks
default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

# Using a DAG context manager, you don't have to specify the dag property of each task
with DAG(
    "dvc_upload_example",
    start_date=datetime(2019, 1, 1),
    max_active_runs=1,
    default_args=default_args,
    catchup=False,
) as dag:

    t0 = DummyOperator(task_id="sample-task")

    t1 = DummyOperator(task_id="group_bash_tasks")

    t2 = BashOperator(
        task_id="bash_task1",
        bash_command='echo "OK" && ( echo $[ ( $RANDOM % 30 )  + 1 ] > meowu.txt ) && cat meowu.txt',
    )

    t3 = BashOperator(
        task_id="bash_task2",
        bash_command="sleep $[ ( $RANDOM % 30 )  + 1 ]s && date",
    )

    upload_task = DVCUpdateOperator(
        dvc_repo=os.environ["REPO"],
        files=[
            DVCStringUpload(
                "data/1.txt",
                f"This will be saved into DVC. Current time: {datetime.now()}",
            ),
        ],
        task_id="update_dvc",
    )

    # generate tasks with a loop. task_id must be unique
    for task in range(5):
        if version.startswith("2"):
            tn = PythonOperator(
                task_id=f"python_custom_task_{task}",
                python_callable=custom_python_task_handler,  # make sure you don't include the () of the function
                op_kwargs={"task_number": task},
            )
        else:
            tn = PythonOperator(
                task_id=f"python_custom_task_{task}",
                python_callable=custom_python_task_handler,  # make sure you don't include the () of the function
                op_kwargs={"task_number": task},
                provide_context=True,
            )

        t0 >> tn

    t0 >> t1
    t2 >> upload_task
    t1 >> [t2, t3]
