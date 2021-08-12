"""
Airflow operator to upload files to DVC.

@Piotr Styczyński 2021
"""
from typing import Callable, List, Union

from airflow.operators.python import PythonOperator

from dvc_hook import DVCHook
from logs import LOGS
from exceptions import add_log_exception_handler
from stats import DVCDownloadMetadata

from dvc_fs.dvc_download import DVCDownload

Downloads = Union[List[DVCDownload], Callable[..., List[DVCDownload]]]

TEMPLATE_FIELDS = ["files", "templates_dict", "op_args", "op_kwargs"]


class DVCDownloadOperator(PythonOperator):
    """
    Operator that downloads given DVC files.
    """

    # Fields to apply Airflow templates
    template_fields = TEMPLATE_FIELDS

    dvc_repo: str  # Clone URL for a GIT repo
    files: Downloads  # List of files to be downloaded or function that returns it
    empty_fallback: bool # Create empty file if it does not exists remotely

    @property
    def affected_files(self) -> List[DVCDownload]:
        if callable(self.files):
            return []
        return self.files

    def __init__(
        self,
        dvc_repo: str,
        files: Downloads,
        empty_fallback: bool = False,
        disable_error_message: bool = False,
        ignore_errors: bool = False,
        **kwargs,
    ) -> None:
        """
        Creates Airflow download operator.

        :param dvc_repo: Git clone url for repo with configured DVC
        :param files: Files to be downloaded (please see DVCDownload class for more details)
        """
        super().__init__(**kwargs, python_callable=add_log_exception_handler(
            self._execute_operator,
            disable_error_message=disable_error_message,
            ignore_errors=ignore_errors,
        ))
        self.dvc_repo = dvc_repo
        self.empty_fallback = empty_fallback
        self.files = files
        if not callable(self.files):
            for file in self.files:
                file.dvc_repo = dvc_repo
        self.template_fields = TEMPLATE_FIELDS

    def _execute_operator(self, *args, **kwargs) -> DVCDownloadMetadata:
        """
        Perform the DVC uploads.
        """
        files = self.files
        if callable(self.files):
            files = self.files(*args, **kwargs)
        dvc = DVCHook(self.dvc_repo)
        LOGS.dvc_download_operator.info(
            f"Download operator executed for files: {', '.join([file.dvc_path for file in files])}"
        )
        meta = dvc.download(
            downloaded_files=files,
            empty_fallback=self.empty_fallback,
        )
        LOGS.dvc_download_operator.info("Download completed.")
        return meta
