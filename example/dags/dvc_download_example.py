"""
Example usage of the DVC upload operator (uploading a string) in an advanced Airflow DAG.

@Piotr Styczyński 2021
"""
import os
from datetime import datetime, timedelta

from airflow import DAG
from airflow_dvc import DVCDownloadOperator, DVCPathDownload

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
    "dvc_download_example",
    start_date=datetime(2019, 1, 1),
    max_active_runs=1,
    default_args=default_args,
    catchup=False,
) as dag:

    download_task = DVCDownloadOperator(
        dvc_repo=os.environ["REPO"],
        files=[
            DVCPathDownload(
                "non_existing_path/data.txt",
                "output_file.txt",
            ),
        ],
        task_id="download_task",
        empty_fallback=True,
    )
