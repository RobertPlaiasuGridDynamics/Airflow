import logging
import random
from datetime import datetime

config = {
    'dag_id_1': {'schedule_interval': None,
                 "start_date": datetime(2018, 11, 11),
                 "table_name": "table_name_1"},
    'dag_id_2': {'schedule_interval': None,
                 "start_date": datetime(2018, 11, 11),
                 "table_name": "table_name_2"},
    'dag_id_3': {'schedule_interval': None,
                 "start_date": datetime(2018, 11, 11),
                 "table_name": "table_name_3"}
}


def log_context(ds=None, **kwargs):
    logger = logging.getLogger(__name__)
    logger.info(f"{kwargs["dag_id"]} start processing tables in database:{kwargs["database"]}")


def branch_condition():
    a = random.randint(1, 2)
    if a == 1:
        return "create_table"
    else:
        return "insert_new_row"