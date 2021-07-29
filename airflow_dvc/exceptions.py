"""
Definitions of possible DVC errors

@Piotr Styczyński 2021
"""
from typing import Optional, List
from git import exc
import traceback
from semantic_version import Version, SimpleSpec
from airflow_dvc.logs import LOGS


class DVCFileMissingError(FileNotFoundError):
    """
    DVC file is missing (remotely)
    """
    repo: str
    file_path: str

    def __init__(self, repo: str, file_path: str):
        self.repo = repo
        self.file_path = file_path
        super().__init__(f"Missing DVC file {self.file_path} in repo {self.repo}")


class DVCCliCommandError(Exception):
    """
    DVC command in shell failed
    """
    dvc_command: str
    dvc_output: Optional[str]
    dvc_exit_code: int
    execution_path: str

    def __init__(
        self,
        dvc_command: str,
        dvc_output: Optional[str],
        dvc_exit_code: int,
        execution_path: str,
    ):
        self.dvc_command = dvc_command
        self.dvc_output = dvc_output
        self.dvc_exit_code = dvc_exit_code
        self.execution_path = execution_path
        super().__init__(f"DVC Command {self.dvc_command} failed with status code {self.dvc_exit_code} and output: {self.dvc_output}")


class DVCMissingExecutableError(Exception):
    """
    DVC executable is missing (not callable from shell)
    """
    def __init__(self):
        super().__init__("DVC Python library is missing and DVC "
                         "executable cannot be found in PATH. "
                         "Please configure your system correctly "
                         "by installing dvc Python package or application "
                         "package suitable for your operating system.")


class DVCGitRepoNotAccessibleError(Exception):
    """
    Repository is not cloneable (access was denied or repository was not found)
    """
    git_exception: exc.GitError
    repo: str

    def __init__(self, repo: str, git_exception: exc.GitError):
        self.git_exception = git_exception
        self.repo = repo
        super().__init__(f"Could not clone git repository: {repo}. Error: {git_exception}")


class DVCGitUpdateError(Exception):
    """
    Problem with committing changes via git
    """
    git_exception: exc.GitError
    updated_files: List[str]
    repo: str

    def __init__(self, repo: str, updated_files: List[str], git_exception: exc.GitError):
        self.git_exception = git_exception
        self.updated_files = updated_files
        self.repo = repo
        super().__init__(f"Cannot update DVC. Git push failed for repository "
                         f"{self.repo} (upload files {', '.join(self.updated_files)}). "
                         f"Error: {self.git_exception}")


class DVCInvalidVersion(Exception):
    """
    Installed DVC has invalid version
    """
    version: Version
    constraint: SimpleSpec
    description: str

    def __init__(self, description: str, version: Version, constraint: SimpleSpec):
        self.version = version
        self.description = description
        self.constraint = constraint
        super().__init__(f"{self.description}. Required version: {constraint}. Got: {version}")


def add_log_exception_handler(
    fn,
    disable_error_message: bool = False,
    ignore_errors: bool = False,
):
    """
    Utility that wraps function and adds log statement that prints all the error information
    and then reraises that error.
    """
    inner_fn = fn

    def wrapped_fn(*args, **kwargs):
        try:
            return inner_fn(*args, **kwargs)
        except Exception as e:
            if not disable_error_message:
                error_trace = traceback.format_exc()
                LOGS.exceptions.error(f"Error was thrown inside the airflow-dvc code. "
                                      f"This is just a useful message to help with Airflow "
                                      f"pipeline debugging. The error will be reraised. Error message: {e}."
                                      f"Error trace: {error_trace}")
            if ignore_errors:
                return None
            raise e
    return wrapped_fn
