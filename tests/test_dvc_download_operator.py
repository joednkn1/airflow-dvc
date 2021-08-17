#!/usr/bin/env python3
import os
from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime
from airflow_dvc import (
    DVCUpdateOperator,
    DVCDownloadOperator,
    DVCStringUpload,
    DVCPathDownload,
    execute_test_task,
)
from dvc_fs.management.create_dvc_repo_github import create_github_dvc_temporary_repo_with_s3


def test_dvc_download():
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
                description="Dag testing the dvc download operator",
                start_date=datetime(2017, 3, 20),
                catchup=False,
        ) as dag:
            execute_test_task(
                DVCUpdateOperator,
                dvc_repo=dvc_url,
                files=[
                    DVCStringUpload("data/3.txt", f"This will be saved into DVC. Current time: {datetime.now()}"),
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
                    DVCPathDownload("data/3.txt", "data/download_test1"),
                ],
            )

            execute_test_task(
                BashOperator,
                bash_command='echo "OK"',
            )


if __name__ == '__main__':
    test_dvc_download()
