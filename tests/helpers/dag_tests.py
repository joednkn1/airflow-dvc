from airflow import DAG
from airflow.models import TaskInstance, BaseOperator
from datetime import datetime
from random import choices


def execute_test_task(operator: BaseOperator, *args, **kwargs):
    post_fix = "".join(str(e) for e in choices(population=range(10), k=10))
    dag = DAG(dag_id=f"test_dag{post_fix}", start_date=datetime.now())
    task = operator(dag=dag, task_id=f"test_task{post_fix}")
    ti = TaskInstance(task=task, execution_date=datetime.now())
    result = task.prepare_for_execution().execute(ti.get_template_context())
    return result

