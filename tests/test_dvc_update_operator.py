#!/usr/bin/env python3

import os
from airflow.operators.bash import BashOperator
from datetime import datetime
from airflow_dvc import (
    DVCUpdateOperator,
    DVCPathUpload,
    DVCStringUpload,
    DVCCallbackUpload,
    execute_test_task,
)


def test_dvc_update():
    repo = create_github_dvc_temporary_repo_with_s3(
        "covid-genomics", "temporary_dvc_repo"
    )
    default_args = {
        "owner": "airflow",
    }
    with repo as fs:
        dvc_url = f"https://{os.environ['DVC_GITHUB_REPO_TOKEN']}@github.com/{repo.owner}/{repo.repo_name}"
        print("OK OK")
        with DAG(
                "dvc_download_test",
                description="Existence sensor example",
                start_date=datetime(2017, 3, 20),
                catchup=False,
        ) as dag:
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
                    DVCStringUpload(
                        "data/4.txt",
                        f"This will be saved into DVC. Current time: {datetime.now()}",
                    ),
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
                    DVCStringUpload(
                        "data/4.txt",
                        f"This will be saved into DVC. Current time: {datetime.now()}",
                    ),
                ],
            )

            execute_test_task(
                BashOperator,
                bash_command='echo "OK"',
            )


if __name__ == "__main__":
    test_dvc_update()
