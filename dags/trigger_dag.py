from datetime import timedelta
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.sensors.external_task import ExternalTaskSensor
from airflow.sensors.filesystem import FileSensor
from resources import *
from airflow import DAG
from airflow.models import Variable
from airflow.utils.task_group import TaskGroup
from airflow.models import DagRun


def get_most_recent_dag_run(dt):
    dag_runs = DagRun.find(dag_id="dag_id_3")
    dag_runs.sort(key=lambda x: x.execution_date, reverse=True)
    if dag_runs:
        return dag_runs[0].execution_date
    raise Exception("Dag wasn't found")


path = Variable.get("path")


def print_log(**context):
    logger = logging.getLogger(__name__)
    logger.info(context["ti"].xcom_pull(dag_id="dag_id_3", task_ids="query_table", key="log", include_prior_dates=True))


with DAG(
        dag_id="trigger_dag",
        start_date=datetime(2024, 1, 1),
        schedule=None
) as dag:
    task1 = FileSensor(
        task_id="wait_for_run_file",
        filepath=f"{path}",
        dag=dag
    )
    with TaskGroup(group_id="process_results") as tg1:
        t1 = ExternalTaskSensor(
            task_id='wait_for_dag',
            allowed_states=["success"],
            failed_states=["failed", "skipped", "upstream_failed"],
            execution_date_fn=get_most_recent_dag_run,
            mode="reschedule",
            external_dag_id="dag_id_3",
            external_task_id="query_table",
        )
        t2 = PythonOperator(
            task_id='print_log',
            python_callable=print_log,
            provide_context=True,
        )
        t3 = BashOperator(
            task_id="remove_small_text.txt",
            bash_command=f"rm {path}",
            dag=dag
        )
        t4 = BashOperator(
            task_id="add_timestamp_file",
            bash_command="touch /Users/rplaiasu/PycharmProjects/DagCreation/data/finished_#{{ ts_nodash }}",
            dag=dag
        )

    t1 >> t2 >> t3 >> t4

    task2 = TriggerDagRunOperator(
        task_id="trigger_dag_id_3",
        trigger_dag_id="dag_id_3",
        dag=dag
    )

task1 >> task2 >> tg1
