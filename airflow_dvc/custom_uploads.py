"""
Abstraction for DVC upload sources.
@Piotr Styczyński 2021
"""
from io import StringIO

from dvc_fs.dvc_upload import DVCUpload

from airflow.providers.amazon.aws.hooks.s3 import S3Hook


class DVCS3Upload(DVCUpload):
    """
    Upload item from S3 to DVC
    This is useful when you have S3Hook in your workflows used
    as a temporary cache for files and you're not using shared-filesystem,
    so using DVCPathUpload is not an option.
    """

    # Fields to apply Airflow templates
    template_fields = ["bucket_path", "bucket_name", "dvc_path"]

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

    def describe_source(self) -> str:
        return f"S3 {self.bucket_name}/{self.bucket_path}"

    def open(self):
        # Open connection to the S3 and download the file
        s3_hook = S3Hook(aws_conn_id=self.aws_conn_id)
        return StringIO(
            s3_hook.read_key(
                key=self.bucket_path, bucket_name=self.bucket_name
            )
        )

    def close(self, resource):
        # Closing is not necessary for S3
        pass
