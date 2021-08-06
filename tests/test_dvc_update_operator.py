#!/usr/bin/env python3

import os
import sys

from airflow.operators.bash import BashOperator

sys.path.append(os.path.join(os.path.dirname(__file__), "helpers"))

from datetime import datetime
from helpers import execute_test_task
from airflow_dvc import DVCUpdateOperator, DVCPathUpload, DVCStringUpload, DVCCallbackUpload
import os


def test_dvc_update():
    dvc_url = f"https://{os.environ['DVC_GITHUB_REPO_TOKEN']}@github.com/covid-genomics/private-airflow-dvc"

    execute_test_task(
        DVCUpdateOperator,
        dvc_repo=dvc_url,
        files=[
            DVCPathUpload("data/5.txt", "data/random3.txt"),
        ],
    )

    execute_test_task(
        BashOperator,
        bash_command='echo "OK"',
    )

    execute_test_task(
        DVCUpdateOperator,
        dvc_repo=dvc_url,
        files=[
            DVCCallbackUpload("data/6.txt", lambda: 50 * "ok "),
        ],
    )

    execute_test_task(
        BashOperator,
        bash_command='echo "OK"',
    )

    execute_test_task(
        DVCUpdateOperator,
        dvc_repo=dvc_url,
        files=[
            DVCStringUpload("data/4.txt", f"This will be saved into DVC. Current time 2132131XYZXYZ: {datetime.now()}"),
        ],
    )

    execute_test_task(
        BashOperator,
        bash_command='echo "OK"',
    )

    execute_test_task(
        DVCUpdateOperator,
        dvc_repo=dvc_url,
        files=[
            DVCCallbackUpload("data/6.txt", lambda: 50 * "ok "),
            DVCPathUpload("data/5.txt", "data/random3.txt"),
            DVCStringUpload("data/4.txt", f"This will be saved into DVC. Current time 2132131XYZXYZ: {datetime.now()}"),
        ],
    )

    execute_test_task(
        BashOperator,
        bash_command='echo "OK"',
    )


if __name__ == '__main__':
    test_dvc_update()
