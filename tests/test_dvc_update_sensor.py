#!/usr/bin/env python3

import os
import sys

from airflow.operators.bash import BashOperator

sys.path.append(os.path.join(os.path.dirname(__file__), "helpers"))

import os
import random
from typing import Tuple

from helpers import execute_test_task, generate_string
from dvc_fs.management.create_dvc_repo_github import (
    create_github_dvc_temporary_repo_with_s3,
)

from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow_dvc import (
    DVCUpdateOperator,
    DVCPathUpload,
    DVCDownloadOperator,
    DVCStringUpload,
    DVCPathDownload,
    DVCExistenceSensor,
    DVCUpdateSensor,
    DVCCallbackDownload,
    DVCCallbackUpload,
)

# import dvc.api
import os


def test_dvc_update_sensor():
    with DAG(
        "dvc_existence_sensor_example",
        description="Existence sensor example",
        start_date=datetime(2017, 3, 20),
        catchup=False,
    ) as dag:
        dvc_url = f"https://{os.environ['DVC_GITHUB_REPO_TOKEN']}@github.com/covid-genomics/private-airflow-dvc"

        execute_test_task(
            DVCUpdateOperator,
            dvc_repo=dvc_url,
            files=[
                DVCStringUpload(
                    "data/4.txt",
                    f"This will be saved into DVC. Current time 213#@!2131XYZXYZ: {datetime.now()}",
                ),
            ],
        )

        execute_test_task(
            BashOperator,
            bash_command='echo "OK"',
        )

        execute_test_task(
            DVCUpdateSensor,
            dvc_repo=dvc_url,
            files=[
                "data/4.txt",
            ],
        )

        execute_test_task(
            BashOperator,
            bash_command='echo "OK"',
        )

        with open("data/random3.txt", "w") as f:
            f.write(random.randint(1, 1000) * "random ")

        execute_test_task(
            DVCUpdateOperator,
            dvc_repo=dvc_url,
            files=[
                DVCPathUpload("data/5.txt", "data/random3.txt"),
            ],
        )

        execute_test_task(
            DVCUpdateSensor,
            dvc_repo=dvc_url,
            files=["data/5.txt"],
        )

        execute_test_task(
            BashOperator,
            bash_command='echo "OK"',
        )

        execute_test_task(
            DVCUpdateOperator,
            dvc_repo=dvc_url,
            files=[
                DVCCallbackUpload(
                    "data/6.txt", lambda: random.randint(1, 1000) * "ok "
                ),
            ],
        )

        execute_test_task(
            DVCUpdateSensor,
            dvc_repo=dvc_url,
            files=["data/6.txt"],
        )

        execute_test_task(
            BashOperator,
            bash_command='echo "OK"',
        )


if __name__ == "__main__":
    test_dvc_update_sensor()
