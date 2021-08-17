"""
Example usage of the DVC existence sensor in the Airflow DAG.

@Piotr Styczyński 2021
"""
from datetime import datetime

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.dummy import DummyOperator

from airflow_dvc import DVCExistenceSensor


def test_dvc_existence_sensor_dag_creation_test():
    repo = create_github_dvc_temporary_repo_with_s3(
        "covid-genomics", "temporary_dvc_repo"
    )
    default_args = {
        "owner": "airflow",
    }
    with repo as fs:
        dvc_url = f"https://{os.environ['DVC_GITHUB_REPO_TOKEN']}@github.com/{repo.owner}/{repo.repo_name}"
        print("OK OK")
        with DAG(
                "dvc_download_test",
                description="Existence sensor example",
                start_date=datetime(2017, 3, 20),
                catchup=False,
        ) as dag:
            dummy_task = DummyOperator(task_id="dummy_task", dag=dag)

            sensor_task_missing = DVCExistenceSensor(
                task_id="sensor_task_missing",
                dag=dag,
                dvc_repo="",
                files=["gisaid/some_missing_file.txt"],
            )

            sensor_task_exists = DVCExistenceSensor(
                task_id="sensor_task_exists",
                dag=dag,
                dvc_repo="",
                files=["gisaid/all.fasta"],
            )

            task_for_existing_file = BashOperator(
                task_id="task_for_existing_file",
                bash_command='echo "OK" && ( echo $[ ( $RANDOM % 30 )  + 1 ] > meowu.txt ) && cat meowu.txt',
            )

            task_for_missing_file = BashOperator(
                task_id="task_for_missing_file",
                bash_command='echo "OK" && ( echo $[ ( $RANDOM % 30 )  + 1 ] > meowu.txt ) && cat meowu.txt',
            )

            final_task = DummyOperator(task_id="final_task", dag=dag)

            dummy_task >> sensor_task_exists >> task_for_existing_file
            dummy_task >> sensor_task_missing >> task_for_missing_file
            [task_for_existing_file, task_for_existing_file] >> final_task


if __name__ == "__main__":
    test_dvc_existence_sensor_dag_creation_test()
