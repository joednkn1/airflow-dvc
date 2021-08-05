import os
import sys


import os
from typing import Tuple

# from dag_tests import execute_test_task
from dvc_fs.management.create_dvc_repo_github import \
    create_github_dvc_temporary_repo_with_s3



sys.path.append(os.path.join(os.path.dirname(__file__), "helpers"))

def test_dvc_download_operator():
    repo = create_github_dvc_temporary_repo_with_s3(
        "covid-genomics", "temporary_dvc_repo"
    )