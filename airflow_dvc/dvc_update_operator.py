"""
Airflow operator to upload files to DVC.

@Piotr Styczyński 2021
"""
import os
from typing import Callable, List, Optional, Union
from airflow.operators.python import PythonOperator
from airflow_dvc.dvc_hook import DVCHook
from airflow_dvc.exceptions import add_log_exception_handler
from airflow_dvc.logs import LOGS
from airflow_dvc.stats import DVCUpdateMetadata

Uploads = Union[List[DVCUpload], Callable[..., List[DVCUpload]]]

TEMPLATE_FIELDS = [
    "files",
    "commit_message",
    "temp_path",
    "templates_dict",
    "op_args",
    "op_kwargs",
]


class DVCUpdateOperator(PythonOperator):
    """
    Operator that allows DAGs to update DVC files.
    You can use it to upload various types of sources.
    For more information please see DVCUpload abstract class.
    """

    # Fields to apply Airflow templates
    template_fields = TEMPLATE_FIELDS

    dvc_repo: str  # Clone URL for a GIT repo
    files: Uploads  # List of files to be uploaded or function that returns it
    commit_message: Optional[str]  # Optional Git custom commit message
    temp_path: Optional[str]  # Path to a temporary clone directory

    @property
    def affected_files(self) -> List[DVCUpload]:
        if callable(self.files):
            return []
        return self.files

    def __init__(
        self,
        dvc_repo: str,
        files: Uploads,
        commit_message: Optional[str] = None,
        temp_path: Optional[str] = None,
        disable_error_message: bool = False,
        ignore_errors: bool = False,
        **kwargs,
    ) -> None:
        """
        Creates Airflow upload operator.

        :param dvc_repo: Git clone url for repo with configured DVC
        :param files: Files to be uploaded (please see DVCUpload class for more details)
        """
        super().__init__(
            **kwargs,
            python_callable=add_log_exception_handler(
                self._execute_operator,
                disable_error_message=disable_error_message,
                ignore_errors=ignore_errors,
            ),
        )
        self.dvc_repo = dvc_repo
        self.files = files
        self.commit_message = commit_message
        self.temp_path = temp_path
        if not callable(self.files):
            for file in self.files:
                file.dvc_repo = dvc_repo
        self.template_fields = TEMPLATE_FIELDS

    def _execute_operator(self, *args, **kwargs) -> DVCUpdateMetadata:
        """
        Perform the DVC uploads.
        """
        files = self.files
        if callable(self.files):
            files = self.files(*args, **kwargs)
        dvc = DVCHook(self.dvc_repo)
        LOGS.dvc_update_operator.info(
            f"Update operator executed for files: {', '.join([file.dvc_path for file in files])}"
        )
        commit_message = self.commit_message
        if commit_message is None:
            file_list_str = ", ".join(
                [os.path.basename(file.dvc_path) for file in self.files]
            )
            commit_message = (
                f"DVC Automatically updated files: {file_list_str}"
            )
        commit_message = f"{commit_message}\ndag: {self.dag_id}"
        meta = dvc.update(
            updated_files=files,
            commit_message=commit_message,
        )
        LOGS.dvc_update_operator.info("Update completed.")
        return meta
