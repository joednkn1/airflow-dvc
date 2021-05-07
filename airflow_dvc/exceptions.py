
from typing import Optional, List
from git import exc
from semantic_version import Version, SimpleSpec


class DVCFileMissingError(FileNotFoundError):
    repo: str
    file_path: str

    def __init__(self, repo: str, file_path: str):
        self.repo = repo
        self.file_path = file_path
        super().__init__(f"Missing DVC file {self.file_path} in repo {self.repo}")


class DVCCliCommandError(Exception):
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
    def __init__(self):
        super().__init__("DVC Python library is missing and DVC "
                         "executable cannot be found in PATH. "
                         "Please configure your system correctly "
                         "by installing dvc Python package or application "
                         "package suitable for your operating system.")


class DVCGitRepoNotAccessibleError(Exception):
    git_exception: exc.GitError
    repo: str

    def __init__(self, repo: str, git_exception: exc.GitError):
        self.git_exception = git_exception
        self.repo = repo
        super().__init__(f"Could not clone git repository: {repo}. Error: {git_exception}")


class DVCGitUpdateError(Exception):
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
    version: Version
    constraint: SimpleSpec
    description: str

    def __init__(self, description: str, version: Version, constraint: SimpleSpec):
        self.version = version
        self.description = description
        self.constraint = constraint
        super().__init__(f"{self.description}. Required version: {constraint}. Got: {version}")
