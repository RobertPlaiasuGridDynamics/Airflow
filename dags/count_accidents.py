import datetime
import os

from airflow import DAG
from airflow.models import Variable
from pyspark import SparkContext
from pyspark.sql import SparkSession
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator

path = Variable.get('container_path')


with DAG(
        dag_id="count_accidents",
        start_date=datetime.datetime(2024,1,1),
        schedule=None
) as dag:
    task1 = SparkSubmitOperator(
        task_id="spark_count_number_of_accidents",
        application=path + "/spark/count_rows.py",
        conn_id="spark",
        dag=dag
    )


    task1


