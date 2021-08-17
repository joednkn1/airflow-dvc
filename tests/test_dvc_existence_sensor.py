#!/usr/bin/env python3

import os
from airflow.operators.bash import BashOperator
from airflow_dvc import execute_test_task
from datetime import datetime
from airflow import DAG
from airflow_dvc import DVCExistenceSensor


def test_dvc_existence_sensor():
    repo = create_github_dvc_temporary_repo_with_s3(
        "covid-genomics", "temporary_dvc_repo"
    )
    default_args = {
        "owner": "airflow",
    }
    with repo as fs:
        dvc_url = f"https://{os.environ['DVC_GITHUB_REPO_TOKEN']}@github.com/{repo.owner}/{repo.repo_name}"
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
                files=["data/3.txt"],
            )

            execute_test_task(
                BashOperator,
                bash_command='echo "OK"',
            )

            execute_test_task(
                DVCExistenceSensor,
                dvc_repo=dvc_url,
                files=["data/4.txt"],
            )

            execute_test_task(
                BashOperator,
                bash_command='echo "OK"',
            )

            execute_test_task(
                DVCExistenceSensor,
                dvc_repo=dvc_url,
                files=["data/5.txt", "data/6.txt"],
            )

            execute_test_task(
                BashOperator,
                bash_command='echo "OK"',
            )

            # Sensor stops executing the dag
            # execute_test_task(
            #     DVCExistenceSensor,
            #     dvc_repo=dvc_url,
            #     files=["data/3213215.txt", "data/6.txt"],
            # )


if __name__ == "__main__":
    test_dvc_existence_sensor()
