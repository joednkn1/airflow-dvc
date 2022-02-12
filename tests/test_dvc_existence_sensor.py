#!/usr/bin/env python3

import random
import os
from airflow.operators.bash import BashOperator
from airflow_dvc import execute_test_task
from datetime import datetime
from airflow import DAG
from airflow_dvc import DVCExistenceSensor
from dvc_fs.management.create_dvc_repo_github import (
    create_github_dvc_temporary_repo_with_s3,
)


def test_dvc_existence_sensor():
    repo = create_github_dvc_temporary_repo_with_s3(
        "covid-genomics", "temporary_dvc_repo"
    )
    with repo as fs:
        dvc_url = f"https://{os.environ['DVC_GITHUB_REPO_TOKEN']}@github.com/{repo.owner}/{repo.repo_name}"

        with DAG(
            "dvc_existence_sensor_test",
            description="Existence sensor example",
            start_date=datetime(2017, 3, 20),
            catchup=False,
        ) as dag:
            with open("data/file1.txt", "w") as file1:
                file1.write(random.randint(1, 100) * "EXISTENCE SENSOR TEST ")

            with open("data/file1.txt", "r") as file1:
                fs.writetext("data/file1.txt", file1.read())

            execute_test_task(
                DVCExistenceSensor,
                dvc_repo=dvc_url,
                files=["data/file1.txt"],
            )

            execute_test_task(
                BashOperator,
                bash_command='echo "TEST PASSED SUCCESSFULLY"',
            )


if __name__ == "__main__":
    test_dvc_existence_sensor()
