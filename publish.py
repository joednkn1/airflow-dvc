from pathlib import Path
import airflow_dvc
from poetry_publish.publish import poetry_publish


def publish():
    poetry_publish(
        package_root=Path(airflow_dvc.__file__).parent.parent,
        version=airflow_dvc.__version__,
    )
