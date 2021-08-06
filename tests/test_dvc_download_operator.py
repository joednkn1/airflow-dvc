#!/usr/bin/env python3

import os
import sys

from airflow.operators.bash import BashOperator

sys.path.append(os.path.join(os.path.dirname(__file__), "helpers"))

import os
from typing import Tuple

from helpers import execute_test_task
from dvc_fs.management.create_dvc_repo_github import \
    create_github_dvc_temporary_repo_with_s3

from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow_dvc import DVCUpdateOperator, DVCPathUpload, DVCDownloadOperator, DVCStringUpload, DVCPathDownload
import dvc.api
import os


class DVCFileDownloadOperator(PythonOperator):
    dvc_repo: str
    dvc_file: str
    output_path: str

    def __init__(
            self,
            dvc_repo: str,
            dvc_file: str,
            output_path: str,
            **kwargs
    ) -> None:
        super().__init__(**kwargs, python_callable=self._execute_operator)
        self.dvc_repo = dvc_repo
        self.dvc_file = dvc_file
        self.output_path = output_path

    def _execute_operator(self, *args, **kwargs) -> str:
        with dvc.api.open(
                self.dvc_file,
                repo=self.dvc_repo,
        ) as fd:
            with open(self.output_path, "w") as out:
                out.write(fd.read())
        return self.output_path


def test_dvc_integration():
    # repo = create_github_dvc_temporary_repo_with_s3(
    #     "covid-genomics", "temporary_dvc_repo"
    # )
    # with repo as fs:
    dvc_url = f"https://{os.environ['DVC_GITHUB_REPO_TOKEN']}@github.com/covid-genomics/private-airflow-dvc"

    execute_test_task(
        DVCUpdateOperator,
        dvc_repo=dvc_url,
        files=[
            DVCStringUpload("data/3.txt", f"This will be saved into DVC. Current time XYZXYZ: {datetime.now()}"),
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
            DVCPathDownload("data/4.txt", "data/downloda_test1"),
        ],
    )


if __name__ == '__main__':
    test_dvc_integration()
