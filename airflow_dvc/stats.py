"""
Definitions of metadata containers that represent information about operator execution.

@Piotr Styczyński 2021
"""
from dataclasses import dataclass
from typing import List, Optional
import time


@dataclass(frozen=True)
class DVCUpdateMetadata:
    """
    Additional information about performed update operation
    """
    dvc_repo: str
    dvc_files_updated: List[str]
    dvc_files_update_requested: List[str]
    dag_id: str
    commit_message: Optional[str]
    temp_path: Optional[str]
    commit_hexsha: Optional[str]
    committed_date: Optional[int]
    duration: time.time


@dataclass(frozen=True)
class DVCDownloadMetadata:
    """
    Additional information about the performed download operation
    """
    dvc_repo: str
    downloaded_dvc_files: List[str]
    downloaded_dvc_files_sizes: List[int]
    duration: time.time

