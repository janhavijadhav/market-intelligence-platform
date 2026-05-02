#!/bin/bash
airflow db migrate
airflow users create --username admin --password admin123 --firstname Admin --lastname User --role Admin --email admin@example.com
airflow scheduler &
airflow webserver
