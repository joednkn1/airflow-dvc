"""
Airflow operator to upload files to DVC.

@Piotr Styczyński 2021
"""
from typing import Optional, List, Tuple
from airflow.models.baseoperator import BaseOperator
from airflow.utils.decorators import apply_defaults

from airflow_dvc.dvc_client import DVCClient
from airflow_dvc.dvc_upload import DVCUpload


class DVCUpdateOperator(BaseOperator):
    """
    Operator that allows DAGs to update DVC files.
    You can use it to upload various types of sources.
    For more information please see DVCUpload abstract class.
    """

    dvc_repo: str # Clone URL for a GIT repo
    files: List[DVCUpload] # List of files to be uploaded
    commit_message: Optional[str] # Optional Git custom commit message
    temp_path: Optional[str] # Path to a temporary clone directory

    @apply_defaults
    def __init__(
            self,
            dvc_repo: str,
            files: List[DVCUpload],
            commit_message: Optional[str] = None,
            temp_path: Optional[str] = None,
            **kwargs
        ) -> None:
        super().__init__(**kwargs)
        self.dvc_repo = dvc_repo
        self.files = files
        self.commit_message = commit_message
        self.temp_path = temp_path

    def execute(self, context):
        """
        Perform the DVC uploads.
        """
        dvc = DVCClient(self.dvc_repo)
        dvc.update(
            updated_files=self.files,
            commit_message=self.commit_message,
            temp_path=self.temp_path,
        )
