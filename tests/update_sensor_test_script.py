#!/usr/bin/env python3

import os
import time
import subprocess

path = os.getcwd().replace("tests", "airflow")
os.system("export TMP=$AIRFLOW_HOME")
os.system(f"export AIRFLOW_HOME={path}")
os.system("cd .. && ./run_airflow.sh")

time.sleep(10)
os.system("airflow dags test dvc_update_sensor_test 2021-08-22")
output = subprocess.check_output(
    ["airflow", "dags", "state", "dvc_update_sensor_test", "2021-08-22"]
).decode()

os.system("killall airflow")
os.system("export AIRFLOW_HOME=$TMP")

if "success" in output:
    exit(0)
else:
    exit(1)
