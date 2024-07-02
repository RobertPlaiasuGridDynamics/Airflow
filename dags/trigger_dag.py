import datetime
from airflow.operators.bash import BashOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.sensors.filesystem import FileSensor
from resources import *
from airflow import DAG
from airflow.models import Variable

path = Variable.get("path")

with DAG(
    dag_id="trigger_dag",
    start_date=datetime(2024,1,1),
    schedule=None
) as dag:

    task1 = FileSensor(
        task_id="wait_for_run_file",
        filepath=f"{path}",
        dag=dag
    )
    task2 = TriggerDagRunOperator(
        task_id="trigger_dag_id_3",
        trigger_dag_id="dag_id_3",
        dag=dag
    )
    task3 = BashOperator(
        task_id="remove_small_text.txt",
        bash_command=f"rm {path}",
        dag=dag
    )
task1 >> task2 >> task3