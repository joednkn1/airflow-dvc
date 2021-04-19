import sys, os
sys.path.insert(0, os.path.abspath(
    os.path.join(
        os.path.dirname(__file__),
        "..",
        "..",
    )
))

from airflow_dvc import DVCPlugin

