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
from airflow_dvc import DVCUpdateOperator, DVCPathUpload, DVCDownloadOperator, DVCStringUpload
import dvc.api
import os


def generate_string(reps: int) -> str:
    return "ok " * reps


def test_dvc_update():
    # repo = create_github_dvc_temporary_repo_with_s3(
    #     "covid-genomics", "temporary_dvc_repo"
    # )
    # with repo as fs:
    dvc_url = f"https://{os.environ['DVC_GITHUB_REPO_TOKEN']}@github.com/covid-genomics/private-airflow-dvc"

    execute_test_task(
        DVCUpdateOperator,
        dvc_repo=dvc_url,
        files=[
            DVCStringUpload("data/4.txt", f"This will be saved into DVC. Current time 2132131XYZXYZ: {datetime.now()}"),
        ],
    )

    execute_test_task(
        DVCUpdateOperator,
        dvc_repo=dvc_url,
        files=[
            DVCStringUpload("data/5.txt", "data/random3.txt"),
        ],
    )

    execute_test_task(
        DVCUpdateOperator,
        dvc_repo=dvc_url,
        files=[
            DVCStringUpload("data/6.txt", generate_string(5)),
        ],
    )


if __name__ == '__main__':
    test_dvc_update()
