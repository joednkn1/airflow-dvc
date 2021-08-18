#!/usr/bin/env python3

import os
import random
import time
import threading
from airflow.operators.bash import BashOperator
from dvc_fs.management.create_dvc_repo_github import (
    create_github_dvc_temporary_repo_with_s3,
    GithubDVCRepo,
)
from datetime import datetime
from airflow import DAG
from airflow_dvc import (
    DVCUpdateOperator,
    DVCPathUpload,
    DVCStringUpload,
    DVCUpdateSensor,
    DVCCallbackUpload,
    execute_test_task,
)


def upload_file(fs: GithubDVCRepo) -> None:
    for i in range(1, 11):
        time.sleep(1)
        print(f"Uploading in {11 - i} seconds...")

    with open("data/file1.txt", "w") as file1:
        file1.write(random.randint(1, 100) * "UPDATEq SENSOR TEST ")

    with open("data/file1.txt", "r") as file1:
        fs.writetext("file1.txt", file1.read())
        print("File file1.txt has been uploaded.")


def sensor_foo(dvc_url: str) -> None:
    with DAG(
        "dvc_existence_sensor_example",
        description="Existence sensor example",
        start_date=datetime(2017, 3, 20),
        catchup=False,
    ) as dag:
        execute_test_task(
            DVCUpdateSensor,
            dvc_repo=dvc_url,
            files=[
                "file1.txt",
            ],
        )


def test_dvc_update_sensor():
    repo = create_github_dvc_temporary_repo_with_s3(
        "covid-genomics", "temporary_dvc_repo"
    )
    with repo as fs:
        dvc_url = f"https://{os.environ['DVC_GITHUB_REPO_TOKEN']}@github.com/{repo.owner}/{repo.repo_name}"

        start_time = time.time()

        threading.Thread(target=upload_file, args=(fs,)).start()
        sensor_foo(dvc_url)

        print("EXECUTING TIME: " + str(time.time() - start_time))
        assert time.time() - start_time < 100

        execute_test_task(
            BashOperator,
            bash_command='echo "TEST PASSED SUCCESSFULLY"',
        )


if __name__ == "__main__":
    test_dvc_update_sensor()
