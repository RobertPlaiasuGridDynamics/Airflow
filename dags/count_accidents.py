import datetime
import os

from airflow.decorators import dag, task
from airflow.models import Variable
from pyspark import SparkContext
from pyspark.sql import SparkSession
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator

path = os.getenv('DATA_FOLDER') + Variable.get('csv_monroe')


@dag(start_date=datetime.datetime(2024, 1, 1), schedule=None, dag_id="count_accidents")
def count_accidents():
    @task.pyspark(task_id="count_accidents_spark")
    def count_accidents_spark(spark: SparkSession, sc: SparkContext):
        df = spark.read.csv(path=path,
                            sep=",",
                            header=True,
                            inferSchema=True)
        rows = df.count()
        print("Numer of rows" + rows)
    count_accidents_spark()

count_accidents()

