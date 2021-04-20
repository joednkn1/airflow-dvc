"""
Airflow operator to upload files to DVC.

@Piotr Styczyński 2021
"""
from typing import Callable, List, Optional, Tuple, Union

from airflow.operators.python_operator import PythonOperator
from airflow.utils.decorators import apply_defaults
from airflow_dvc.dvc_download import DVCDownload
from airflow_dvc.dvc_hook import DVCHook

Downloads = Union[List[DVCDownload], Callable[..., List[DVCDownload]]]


class DVCDownloadOperator(PythonOperator):
    """
    Operator that downloads given DVC files.
    """

    dvc_repo: str  # Clone URL for a GIT repo
    files: Downloads  # List of files to be downloaded or function that returns it

    @property
    def affected_files(self) -> List[DVCDownload]:
        if callable(self.files):
            return []
        return self.files

    @apply_defaults
    def __init__(self, dvc_repo: str, files: Downloads, **kwargs) -> None:
        """
        Creates Airflow download operator.

        :param dvc_repo: Git clone url for repo with configured DVC
        :param files: Files to be downloaded (please see DVCDownload class for more details)
        """
        super().__init__(**kwargs, python_callable=self._execute_operator)
        self.dvc_repo = dvc_repo
        self.files = files
        if not callable(self.files):
            for file in self.files:
                file.dvc_repo = dvc_repo

    def _execute_operator(self, *args, **kwargs):
        """
        Perform the DVC uploads.
        """
        files = self.files
        if callable(self.files):
            files = self.files(*args, **kwargs)
        dvc = DVCHook(self.dvc_repo)
        dvc.download(
            downloaded_files=files,
        )
