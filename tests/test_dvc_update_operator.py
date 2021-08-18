#!/usr/bin/env python3

import os
import random
import filecmp
from airflow.operators.bash import BashOperator
from datetime import datetime
from dvc_fs.management.create_dvc_repo_github import create_github_dvc_temporary_repo_with_s3
from airflow_dvc import (
    DVCUpdateOperator,
    DVCPathUpload,
    DVCStringUpload,
    DVCCallbackUpload,
    execute_test_task, DVCDownloadOperator,
    DVCPathDownload
)


def test_dvc_update():
    repo = create_github_dvc_temporary_repo_with_s3(
        "covid-genomics", "temporary_dvc_repo"
    )
    with repo as fs:
        dvc_url = f"https://{os.environ['DVC_GITHUB_REPO_TOKEN']}@github.com/{repo.owner}/{repo.repo_name}"

        with open('data/update_sample1.txt', 'w') as file1:
            file1.write(random.randint(1, 100) * "UPDATE TEST ")

        execute_test_task(
            DVCUpdateOperator,
            dvc_repo=dvc_url,
            files=[
                DVCPathUpload("data/update_file1.txt", "data/update_sample1.txt"),
            ],
        )

        execute_test_task(
            DVCUpdateOperator,
            dvc_repo=dvc_url,
            files=[
                DVCCallbackUpload("data/update_file2.txt", lambda: random.randint(1, 100) * "UPDATE TEST  "),
            ],
        )

        execute_test_task(
            DVCUpdateOperator,
            dvc_repo=dvc_url,
            files=[
                DVCStringUpload(
                    "data/update_file3.txt",
                    f"This will be saved into DVC. Current time: {datetime.now()}",
                ),
            ],
        )

        with open('data/update_sample2.txt', 'w') as file1:
            file1.write(random.randint(1, 100) * "UPDATE TEST ")

        execute_test_task(
            DVCUpdateOperator,
            dvc_repo=dvc_url,
            files=[
                DVCCallbackUpload("data/update_file4.txt", lambda: random.randint(1, 100) * "UPDATE TEST "),
                DVCPathUpload("data/update_file5.txt", "data/update_sample2.txt"),
                DVCStringUpload(
                    "data/update_file6.txt",
                    f"This will be saved into DVC. Current time: {datetime.now()}",
                ),
            ],
        )

        for i in range(1, 2):
            execute_test_task(
                DVCDownloadOperator,
                dvc_repo=dvc_url,
                files=[
                    DVCPathDownload(f"data/update_file{i}.txt", f"data/update_test{i}.txt"),
                ],
            )

            assert filecmp.cmp(f'data/update_sample{i}.txt', f'data/update_test{i}.txt')

        execute_test_task(
            BashOperator,
            bash_command='echo "TEST PASSED SUCCESSFULLY"',
        )


if __name__ == "__main__":
    test_dvc_update()
