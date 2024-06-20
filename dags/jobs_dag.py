from datetime import datetime
from airflow.decorators import dag, task

config = {
    'dag_id_1': {'schedule_interval': "", "start_date": datetime(2018, 11, 11)},
    'dag_id_2': {'schedule_interval': "", "start_date": datetime(2018, 11, 11)},
    'dag_id_3': {'schedule_interval': "", "start_date": datetime(2018, 11, 11)}
}

for config_name, config in config.items():

    @dag(dag_id=config_name, start_date=config["start_date"])
    def dynamic_generated_dag():
        @task
        def print_message():
            print("message")

        print_message()

    dynamic_generated_dag()

