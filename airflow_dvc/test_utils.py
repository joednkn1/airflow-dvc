from airflow import DAG
from airflow.models import TaskInstance, BaseOperator
from datetime import datetime


def execute_test_task(operator: BaseOperator, *args, **kwargs):
    dag = DAG(dag_id="test_dag", start_date=datetime.now())
    task = operator(*args, **kwargs, dag=dag, task_id='test_task')
    ti = TaskInstance(task=task, execution_date=datetime.now())
    result = task.prepare_for_execution().execute(ti.get_template_context())
    return result