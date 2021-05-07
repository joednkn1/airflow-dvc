"""
Abstraction for DVC download targets.

@Piotr Styczyński 2021
"""
import inspect
from abc import ABCMeta, abstractmethod
from typing import Callable, Optional

from airflow.providers.amazon.aws.hooks.s3 import S3Hook

try:
    # flake8: noqa
    from StringIO import StringIO  # # for Python 2
except ImportError:
    # flake8: noqa
    from io import StringIO  # # for Python 3


class DVCDownload(metaclass=ABCMeta):
    """
    Base class for all DVC udownloads.
    The DVCDownload corresponds to an abstract request to download a file from the upstream.
    """

    dvc_repo: Optional[str] = None
    dvc_path: str  # Path to he GIT repo that is an upstream target
    instance_context: str

    def __init__(self, dvc_path: str):
        self.dvc_path = dvc_path
        curframe = inspect.currentframe()
        caller = inspect.getouterframes(curframe, 2)[2]
        caller_path = caller.filename.split("/")[-1]
        self.instance_context = f"({caller_path}:{caller.lineno})"

    @abstractmethod
    def describe_target(self) -> str:
        """
        Human-readable message about the upload source
        """
        raise Exception(
            "Operation is not supported: describe_target() invoked on abstract base class - DVCDownload"
        )

    @abstractmethod
    def write(self, content: str):
        """
        Custom implementation of the download behaviour.
        write() should write a data string to the target resource
        """
        raise Exception(
            "Operation is not supported: write() invoked on abstract base class - DVCDownload"
        )


class DVCCallbackDownload(DVCDownload):
    """
    Download local file from DVC and run Python callback with the file content
    """

    # Fields to apply Airflow templates
    template_fields = ['dvc_path']

    callback: Callable[[str], None]

    def __init__(self, dvc_path: str, callback: Callable[[str], None]):
        super().__init__(dvc_path=dvc_path)
        self.callback = callback

    def describe_target(self) -> str:
        return f"Callback {self.instance_context}"

    def write(self, content: str):
        self.callback(content)


class DVCPathDownload(DVCDownload):
    """
    Download local file from DVC and save it to the given path
    """

    # Fields to apply Airflow templates
    template_fields = ['dvc_path', 'src']

    # Path to the local file that will be written
    src: str

    def __init__(self, dvc_path: str, local_path: str):
        super().__init__(dvc_path=dvc_path)
        self.src = local_path

    def describe_target(self) -> str:
        return f"Path {self.src}"

    def write(self, content: str):
        with open(self.src, "w") as out:
            out.write(content)


class DVCS3Download(DVCDownload):
    """
    Download item from DVC and save it to S3
    This is useful when you have S3Hook in your workflows used
    as a temporary cache for files and you're not using shared-filesystem,
    so using DVCPathDownload is not an option.
    """

    # Fields to apply Airflow templates
    template_fields = ['dvc_path', 'bucket_name', 'bucket_path']

    # Connection ID (the same as for Airflow S3Hook)
    # For more details please see:
    # - https://airflow.apache.org/docs/apache-airflow/1.10.14/_modules/airflow/hooks/S3_hook.html
    # - https://www.programcreek.com/python/example/120741/airflow.hooks.S3_hook.S3Hook
    aws_conn_id: str
    # Bucket name (see above)
    bucket_name: str
    # Bucket path for the downloaded file (see above)
    bucket_path: str

    def __init__(
        self,
        dvc_path: str,
        aws_conn_id: str,
        bucket_name: str,
        bucket_path: str,
    ):
        super().__init__(dvc_path=dvc_path)
        self.aws_conn_id = aws_conn_id
        self.bucket_name = bucket_name
        self.bucket_path = bucket_path

    def describe_target(self) -> str:
        return f"S3 {self.bucket_name}/{self.bucket_path}"

    def write(self, content: str):
        # Open connection to the S3 and download the file
        s3_hook = S3Hook(aws_conn_id=self.aws_conn_id)
        s3_hook.load_string(
            content,
            self.bucket_path,
            bucket_name=self.bucket_name,
            replace=True,
        )
