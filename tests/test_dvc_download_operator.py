#!/usr/bin/env python3

import os
from airflow.operators.bash import BashOperator
from datetime import datetime
from airflow_dvc import (
    DVCUpdateOperator,
    DVCDownloadOperator,
    DVCStringUpload,
    DVCPathDownload,
    execute_test_task,
)
from dvc_fs.management.create_dvc_repo_github import (
    create_github_dvc_temporary_repo_with_s3,
)
from airflow.decorators import task, dag
from airflow.utils.dates import days_ago

repo = create_github_dvc_temporary_repo_with_s3(
    "covid-genomics", "temporary_dvc_repo"
)
default_args = {
    "owner": "airflow",
}

with repo as fs:

    @dag(
        default_args=default_args,
        schedule_interval=None,
        start_date=days_ago(2),
        tags=["example_test"],
    )
    def test_dvc_download():
        dvc_url = f"https://{os.environ['DVC_GITHUB_REPO_TOKEN']}@github.com/{repo.owner}/{repo.repo_name}"

        @task()
        def upload_1() -> DVCUpdateOperator:
            return DVCUpdateOperator(
                task_id="upload_1",
                dvc_repo=dvc_url,
                files=[
                    DVCStringUpload(
                        "data/3.txt",
                        f"This will be saved into DVC. Current time XYZXYZ: {datetime.now()}",
                    )
                ],
            )

        @task()
        def echo_ok(ex: DVCDownloadOperator):
            return BashOperator(
                bash_command="echo 'OK OK OK OK'", task_id="echo_ok"
            )

        @task()
        def download_1(ex: DVCUpdateOperator) -> DVCDownloadOperator:
            return DVCDownloadOperator(
                task_id="download_1",
                dvc_repo=dvc_url,
                files=[
                    DVCPathDownload("data/4.txt", "data/downloda_test1"),
                ],
            )

        echo_ok(download_1(upload_1))

    dag = test_dvc_download()
