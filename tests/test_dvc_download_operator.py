#!/usr/bin/env python3

import os
import sys

from airflow.operators.bash import BashOperator

sys.path.append(os.path.join(os.path.dirname(__file__), "helpers"))

from helpers import execute_test_task
from datetime import datetime
from airflow_dvc import (
    DVCUpdateOperator,
    DVCDownloadOperator,
    DVCStringUpload,
    DVCPathDownload,
)
import os


def test_dvc_download():
    dvc_url = f"https://{os.environ['DVC_GITHUB_REPO_TOKEN']}@github.com/covid-genomics/private-airflow-dvc"

    execute_test_task(
        DVCUpdateOperator,
        dvc_repo=dvc_url,
        files=[
            DVCStringUpload(
                "data/3.txt",
                f"This will be saved into DVC. Current time XYZXYZ: {datetime.now()}",
            ),
        ],
    )

    execute_test_task(
        BashOperator,
        bash_command='echo "OK"',
    )

    execute_test_task(
        DVCDownloadOperator,
        dvc_repo=dvc_url,
        files=[
            DVCPathDownload("data/4.txt", "data/downloda_test1"),
        ],
    )

    execute_test_task(
        BashOperator,
        bash_command='echo "OK"',
    )


if __name__ == "__main__":
    test_dvc_download()
