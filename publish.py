"""
Script to publish Python package.
Run via "poetry run publish"

@Piotr Styczyński 2021
"""
from pathlib import Path

from poetry_publish.publish import poetry_publish

import airflow_dvc


def publish():
    poetry_publish(
        package_root=Path(airflow_dvc.__file__).parent.parent,
        version=airflow_dvc.__version__,
    )
