from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator

from resources import *

with DAG(
        dag_id="dag_id_2",
        start_date=config["dag_id_2"]["start_date"],
        schedule=config["dag_id_2"]["schedule_interval"]
) as dag:
    task1 = PythonOperator(
        task_id="log_database_connection",
        python_callable=log_context,
        op_kwargs={"dag_id": "dag_id_2", "database": "airflow"},
        dag=dag
    )
    task2 = EmptyOperator(
        task_id="insert_new_row"
    )
    task3 = EmptyOperator(
        task_id="query_table"
    )
    task1 >> task2 >> task3

