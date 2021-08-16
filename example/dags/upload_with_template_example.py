"""
Example usage of the DVC upload operator (uploading a string) in an advanced Airflow DAG.

@Piotr Styczyński 2021
"""
import os
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.dummy import DummyOperator
from airflow.operators.python import PythonOperator
from airflow.version import version

from airflow_dvc import DVCStringUpload, DVCUpdateOperator


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
    "dvc_upload_with_template_example",
    start_date=datetime(2019, 1, 1),
    max_active_runs=1,
    default_args=default_args,
    catchup=False,
) as dag:

    upload_task = DVCUpdateOperator(
        dvc_repo=os.environ["REPO"],
        files=[
            DVCStringUpload(
                "data/{{ yesterday_ds_nodash }}.txt",
                f"This is jinja Airflow template. DAG date: {{ yesterday_ds_nodash }}",
            ),
        ],
        task_id="update_dvc",
    )
