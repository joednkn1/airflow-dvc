#!/bin/bash

export AIRFLOW_HOME=$(pwd)/airflow
export AIRFLOW_CONFIG=$AIRFLOW_HOME/airflow.cfg
mkdir -p $AIRFLOW_HOME > /dev/null 2> /dev/null

poetry run airflow db init

poetry run airflow users create \
    --username admin \
    --firstname Peter \
    --lastname Parker \
    --role Admin \
    --email spiderman@superhero.org

poetry run airflow webserver --port 8080 &
poetry run  airflow scheduler &