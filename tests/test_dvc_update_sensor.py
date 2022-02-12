#!/usr/bin/env python3

import os
import random
import subprocess
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
from airflow.models import Variable


def upload_file(fs: GithubDVCRepo) -> None:
    for i in range(1, 11):
        time.sleep(1)
        print(f"Uploading in {11 - i} seconds...")

    with open("data/file1.txt", "w") as file1:
        file1.write(random.randint(1, 100) * "UPDATE SENSOR TEST ")

    with open("data/file1.txt", "r") as file1:
        fs.writetext("file1.txt", file1.read())
        print("File file1.txt has been uploaded.")


def run_sensor(dvc_url: str, dag_var: DAG) -> None:
    with dag_var as dag:
        execute_test_task(
            DVCUpdateSensor,
            dag,
            dvc_repo=dvc_url,
            files=[
                "file1.txt",
            ],
        )


def test_dvc_update_sensor(dag: DAG = None) -> None:
    repo = create_github_dvc_temporary_repo_with_s3(
        "covid-genomics", "temporary_dvc_repo"
    )
    with repo as fs:
        token = (
            Variable.get("DVC_GITHUB_REPO_TOKEN")
            if os.environ["DVC_GITHUB_REPO_TOKEN"] == ""
            else os.environ["DVC_GITHUB_REPO_TOKEN"]
        )
        dvc_url = f"https://{token}@github.com/{repo.owner}/{repo.repo_name}"

        start_time = time.time()
        threading.Thread(target=upload_file, args=(fs,)).start()
        run_sensor(dvc_url, dag)
        finish_time = time.time()
        print(f"EXECUTING TIME: {finish_time - start_time}")
        assert finish_time - start_time < 100

        execute_test_task(
            BashOperator,
            bash_command='echo "TEST PASSED SUCCESSFULLY"',
        )


if __name__ == "__main__":
    res = subprocess.run(["python3", "update_sensor_test_script.py"])
    exit(res.returncode)
