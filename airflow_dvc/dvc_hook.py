"""
High-level DVC client for building aced workflows.

@Piotr Styczyński 2021
"""
from git import Repo
import os
import io
import shutil
import tempfile
import datetime
from typing import Optional, List, TextIO, Any
from dataclasses import dataclass
from airflow.hooks.base import BaseHook

from airflow_dvc.dvc_upload import DVCUpload
from airflow_dvc.dvc_download import DVCDownload
from airflow_dvc.dvc_cli import DVCLocalCli

try:
    from StringIO import StringIO ## for Python 2
except ImportError:
    from io import StringIO ## for Python 3

@dataclass
class ClonedRepo:
    """
    Collection of information about the cloned repository.
    """
    clone_path: str # Path to the clone directory
    temp_dir: tempfile.TemporaryDirectory # Temporary directory object pointing to the clone path
    repo: Repo # Git repo handler
    dvc: DVCLocalCli # DVC interface


def clone_repo(
    dvc_repo: str,
    temp_path: Optional[str] = None,
):
    """
        Clone GIT repo configured for DVC.
        :param dvc_repo: Repository clone URL
        :param temp_path: Optional temporary path to store fetched data
        :returns: Cloned repo access object
    """
    if temp_path is None:
        temp_dir = tempfile.TemporaryDirectory()
    else:
        temp_dir = tempfile.TemporaryDirectory(dir=temp_path)
    clone_path = os.path.join(temp_dir.name, 'repo')
    if os.path.isdir(clone_path):
        shutil.rmtree(clone_path)
    os.makedirs(clone_path, exist_ok=True)
    repo = Repo.clone_from(dvc_repo, clone_path)
    dvc = DVCLocalCli(clone_path)
    return clone_path, temp_dir, repo, dvc


try:
    import dvc.api as dvc_api

    def dvc_open(repo: str, path: str) -> TextIO:
        return dvc_api.open(
            path,
            repo=repo,
        )
except ModuleNotFoundError:
    def dvc_open(repo: str, path: str) -> TextIO:
        clone_path, temp_dir, repo, dvc = clone_repo(repo)
        temp_dir.cleanup()
        with open(os.path.join(clone_path, path), 'r') as dvc_file:
            input_stream = io.StringIO(dvc_file.read())
        return input_stream


def repo_add_dvc_files(repo: Repo, files: List[str]):
    """
    Add DVC metadata files corresponding to the given ones to the GIT stage.
    :param repo: GIT repository object
    :param files: List of file paths
    """
    dvc_files = [f'{file}.dvc' for file in files]
    for dvc_file in dvc_files:
        repo.index.add([dvc_file])


class DVCFile:
    """
    Information about exisitng DVC file object
    """
    path: str # File path
    dvc_repo: str # Clone URL for the GIT repository that have DVC configured
    descriptor = None # File-like object used for access

    def __init__(
        self,
        path: str,
        dvc_repo: str,
    ):
        """
        Create DVC file access object
        :param path: DVC file path
        :param dvc_repo: Clone URL for the GIT repo that have DVC configured
        """
        self.path = path
        self.dvc_repo = dvc_repo
        self.descriptor = None

    def __enter__(self):
        """
        Open file for read and return file-like accessor
        """
        if self.descriptor is None:
            self.descriptor = dvc_api.open(
                self.path,
                repo=self.dvc_repo,
            )
        return self.descriptor.__enter__()

    def __exit__(self, type, value, traceback):
        """
        Close file (close file-like accessor returned by __enter__)
        """
        if self.descriptor is not None:
            self.descriptor.__exit__(type, value, traceback)
        self.descriptor = None


class DVCHook(BaseHook):
    """
    Interface for all high-level DVC operations.
    For low-level DVC operations please see DVCLocalCli class.
    """
    dvc_repo: str

    def __init__(
        self,
        dvc_repo: str,
    ):
        """
        :param dvc_repo: Clone URL for the GIT repo that has DVC configured
        """
        super().__init__()
        self.dvc_repo = dvc_repo

    def get_conn(self) -> Any:
        return self

    def modified_date(
        self,
        paths: List[str],
        temp_path: Optional[str] = None,
    ) -> datetime.datetime:
        """
        Get latest modification time for a given DVC-tracked file.
        The function will find the latest commit involving change to the DVC metadata
        file tracked in the repo that corresponds to the given input path.
        For multiple paths the max(...) of last modification dates is taken.

        :param paths: Paths to query the last modification date (max of dates will be taken)
        :param temp_path: Optional temporary clone path
        :returns: Time of last modification of the given files
        """
        _, temp_dir, repo, _ = clone_repo(self.dvc_repo, temp_path)
        commits = list(repo.iter_commits(max_count=100, paths=[f'{file_path}.dvc' for file_path in paths]))
        last_modification_date = datetime.datetime.fromtimestamp(commits[0].committed_date)
        temp_dir.cleanup()
        return last_modification_date

    def download(
        self,
        downloaded_files: List[DVCDownload],
    ):
        """
        Download files from the DVC.
        For single-file access please see get(...) method.

        :param downloaded_files: Files to be downloaded
          (for more details see DVCDownload class)
        """
        if len(downloaded_files) == 0:
            return

        for downloaded_file in downloaded_files:
            with self.get(downloaded_file.dvc_path) as data_input:
                downloaded_file.write(data_input.read())

    def update(
        self,
        updated_files: List[DVCUpload],
        commit_message: Optional[str] = None,
        temp_path: Optional[str] = None,
    ):
        """
        Update given files and upload them to DVC repo.
        The procedure involves pushing DVC changes and commiting
        changed metadata files back to the GIT repo.

        By default the commit message is as follows:
           DVC Automatically updated files: <list of files specified>

        :param updated_files: List of files to be uploaded as DVCUpload objects (please see DVCUpload class for more details)
        :param commit_message: Optional GIT commit message
        :param temp_path: Optional temporary clone path
        """
        if len(updated_files) == 0:
            return

        if commit_message is None:
            file_list_str = ', '.join([os.path.basename(file.dvc_path) for file in updated_files])
            commit_message = f"DVC Automatically updated files: {file_list_str}"

        print("Add files to DVC")
        clone_path, temp_dir, repo, dvc = clone_repo(self.dvc_repo, temp_path)
        for file in updated_files:
            with file as input_file:
                with open(os.path.join(clone_path, file.dvc_path), 'w') as out:
                    out.write(input_file.read())
            dvc.add(file.dvc_path)

        print("Push DVC")
        dvc.push()

        print("Add DVC files to git")
        repo_add_dvc_files(repo, [file.dvc_path for file in updated_files])

        print("Commit")
        repo.index.commit(commit_message)

        print("Git push")
        repo.remotes.origin.push()

        print("Perform cleanup")
        temp_dir.cleanup()

    def get(self, path: str) -> DVCFile:
        """
        Returns existing DVC file handler.
        This is useful to read the files.

        :param path: Path inside the DVC repo to the file you want to access
        :returns: DVCFile handler corresponding to the given file
        """
        return DVCFile(
            path=path,
            dvc_repo=self.dvc_repo,
        )


