#!/usr/bin/env python3
import os
import random
import filecmp
from airflow.operators.bash import BashOperator
from airflow.api.client.local_client import Client
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


def test_dvc_download():
    repo = create_github_dvc_temporary_repo_with_s3(
        "covid-genomics", "temporary_dvc_repo"
    )
    with repo as fs:
        dvc_url = f"https://{os.environ['DVC_GITHUB_REPO_TOKEN']}@github.com/{repo.owner}/{repo.repo_name}"

        with open("data/file1.txt", "w") as file1:
            file1.write(random.randint(1, 100) * "DOWNLOAD TEST ")

        with open("data/file1.txt", "r") as file1:
            fs.writetext("data/file1.txt", file1.read())

        execute_test_task(
            DVCDownloadOperator,
            dvc_repo=dvc_url,
            files=[
                DVCPathDownload("data/file1.txt", "data/download_test1.txt"),
            ],
        )

        assert filecmp.cmp("data/file1.txt", "data/download_test1.txt")

        execute_test_task(
            BashOperator,
            bash_command='echo "TEST PASSED SUCCESSFULLY"',
        )


if __name__ == "__main__":
    test_dvc_download()
