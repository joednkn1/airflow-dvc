import glob
import importlib
import os

from airflow_dvc.tests.helpers import fake_env

fake_env()


def test_dag_loading():
    for dag_path in glob.glob("example/dags/*.py"):
        spec = importlib.util.spec_from_file_location(
            os.path.basename(dag_path).split(".")[0], dag_path
        )
        dag_module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(dag_module)
        assert hasattr(dag_module, "dag")

