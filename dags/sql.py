import subprocess
import uuid

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook

from PostgreSQLCountRows import PostgreSQLCountRows
from resources import *


def get_current_user(ti):
    user = subprocess.check_output('whoami')
    ti.xcom_push(key="user", value=user.decode('utf-8'))


def check_table_exist(sql_to_check_table_exist,
                      table_name):
    """ callable function to get schema name and after that check if table exist """
    hook = PostgresHook()
    # check table exist
    query = hook.get_first(sql=sql_to_check_table_exist.format(table_name))
    print(query)
    if query:
        return 'dummy_operator'

    return 'create_table'


def number_of_rows(ti):
    hook = PostgresHook()

    query = hook.get_first(sql="SELECT COUNT(*) FROM TABLE_NAME;")
    ti.xcom_push(key="number_rows", value=query)


with DAG(
        dag_id="sql_dag",
        start_date=config["dag_id_1"]["start_date"],
        schedule=config["dag_id_1"]["schedule_interval"]
) as dag:
    task1 = BashOperator(
        task_id="print_process_start",
        bash_command='echo "Process start"'
    )
    task2 = PythonOperator(
        task_id="get_current_user",
        python_callable=get_current_user,
        provide_context=True
    )
    task3 = BranchPythonOperator(
        task_id="check_if_table_exists",
        python_callable=check_table_exist,
        op_args=[
                 """
                    SELECT COUNT(*)
                    FROM information_schema.tables
                    WHERE table_name = '{0}'
                    """, "table_name"],
        dag=dag
    )
    task4 = EmptyOperator(
        task_id='dummy_operator',
        dag=dag
    )
    task5 = SQLExecuteQueryOperator(
        task_id="create_table",
        conn_id="postgres_default",
        sql="""CREATE TABLE table_name(custom_id integer NOT NULL,
                user_name VARCHAR (50) NOT NULL, timestamp TIMESTAMP NOT NULL);""",
        dag=dag
    )
    task6 = SQLExecuteQueryOperator(
        task_id="insert_new_row",
        conn_id="postgres_default",
        trigger_rule='none_failed',
        sql="INSERT INTO table_name VALUES(%s, %s, %s);",
        parameters=(uuid.uuid4().int % 123456789, uuid.uuid4().hex[:10], datetime.now()),
        dag=dag
    )
    task7 = PostgreSQLCountRows(
        task_id="query_table",
        table_name="table_name",
        dag=dag
    )

    task1 >> task2
    task2 >> task3
    task3 >> task4
    task3 >> task5
    task4 >> task6
    task5 >> task6
    task6 >> task7
