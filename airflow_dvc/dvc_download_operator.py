"""
Airflow operator to upload files to DVC.

@Piotr Styczyński 2021
"""
from typing import Optional, List, Tuple, Union
from airflow.models.python_operator import PythonOperator
from airflow.utils.decorators import apply_defaults
from collections.abc import Callable

from airflow_dvc.dvc_client import DVCClient
from airflow_dvc.dvc_download import DVCDownload

Downloads = Union[List[DVCDownload], Callable[any, List[DVCDownload]]]


class DVCDownloadOperator(PythonOperator):
    """
    Operator that downloads given DVC files.
    """

    dvc_repo: str # Clone URL for a GIT repo
    files: Downloads # List of files to be downloaded or function that returns it

    @apply_defaults
    def __init__(
            self,
            dvc_repo: str,
            files: Downloads,
            **kwargs
        ) -> None:
        """
        Creates Airflow download operator.

        :param dvc_repo: Git clone url for repo with configured DVC
        :param files: Files to be downloaded (please see DVCDownload class for more details)
        """
        super().__init__(**kwargs, python_callable=self._execute_operator)
        self.dvc_repo = dvc_repo
        self.files = files

    def _execute_operator(self, *args, **kwargs):
        """
        Perform the DVC uploads.
        """
        files = self.files
        if callable(self.files):
            files = self.files(*args, **kwargs)
        dvc = DVCClient(self.dvc_repo)
        dvc.download(
            downloaded_files=files,
        )
