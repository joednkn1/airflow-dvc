"""
Abstraction for DVC download targets.
@Piotr Styczyński 2021
"""
from dvc_fs import DVCDownload
from airflow.providers.amazon.aws.hooks.s3 import S3Hook


class DVCS3Download(DVCDownload):
    """
    Download item from DVC and save it to S3
    This is useful when you have S3Hook in your workflows used
    as a temporary cache for files and you're not using shared-filesystem,
    so using DVCPathDownload is not an option.
    """

    # Fields to apply Airflow templates
    template_fields = ["dvc_path", "bucket_name", "bucket_path"]

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
