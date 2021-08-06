#!/usr/bin/env python3

import os
import sys

from airflow.operators.bash import BashOperator

sys.path.append(os.path.join(os.path.dirname(__file__), "helpers"))

import os
from typing import Tuple

from helpers import execute_test_task
from dvc_fs.management.create_dvc_repo_github import \
    create_github_dvc_temporary_repo_with_s3

from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow_dvc import DVCUpdateOperator, DVCPathUpload, DVCDownloadOperator, DVCStringUpload, DVCPathDownload, \
    DVCExistenceSensor
import dvc.api
import os


def test_dvc_existence_sensor():
    with DAG(
            "dvc_existence_sensor_example",
            description="Existence sensor example",
            start_date=datetime(2017, 3, 20),
            catchup=False,
    ) as dag:
        dvc_url = f"https://{os.environ['DVC_GITHUB_REPO_TOKEN']}@github.com/covid-genomics/private-airflow-dvc"

        execute_test_task(
            DVCExistenceSensor,
            dvc_repo=dvc_url,
            # dag=dag,
            files=["data/3.txt"],
        )

        execute_test_task(
            BashOperator,
            bash_command='echo "OK"',
        )

        execute_test_task(
            DVCExistenceSensor,
            dvc_repo=dvc_url,
            # dag=dag,
            files=["data/4.txt"],
        )

        execute_test_task(
            BashOperator,
            bash_command='echo "OK"',
        )

        execute_test_task(
            DVCExistenceSensor,
            dvc_repo=dvc_url,
            # dag=dag,
            files=["data/5.txt", "data/6.txt"],
        )

        execute_test_task(
            BashOperator,
            bash_command='echo "OK"',
        )

        execute_test_task(
            DVCExistenceSensor,
            dvc_repo=dvc_url,
            # dag=dag,
            files=["data/3213215.txt", "data/6.txt"],
        )


if __name__ == '__main__':
    test_dvc_existence_sensor()
