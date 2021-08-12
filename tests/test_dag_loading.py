import glob
import importlib
import os
import pytest

from airflow_dvc.tests.helpers import fake_env

fake_env()


@pytest.mark.parametrize("dag_path", glob.glob("example/dags/*.py"))
def test_dag_loading(dag_path: str):
    file_name = os.path.basename(dag_path).split(".")[0]
    spec = importlib.util.spec_from_file_location(
        file_name, dag_path
    )
    dag_module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(dag_module)
    assert hasattr(dag_module, "dag")

    dag = dag_module.dag
    assert dag.dag_id == file_name

