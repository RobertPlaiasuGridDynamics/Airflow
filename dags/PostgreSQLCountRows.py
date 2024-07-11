from typing import Callable, Mapping, Iterable, Any

from airflow.providers.common.sql.hooks.sql import fetch_all_handler
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook


class PostgreSQLCountRows(SQLExecuteQueryOperator):
    def __init__(self,
                 table_name: str,
                 autocommit: bool = False,
                 parameters: Mapping | Iterable | None = None,
                 handler: Callable[[Any], Any] = fetch_all_handler,
                 conn_id: str | None = None,
                 database: str | None = None,
                 split_statements: bool | None = None,
                 return_last: bool = True,
                 show_return_value_in_logs: bool = False,
                 **kwargs,
                 ) -> None:
        self.sql = f'SELECT COUNT(*) FROM {table_name};'
        self.autocommit = autocommit
        self.parameters = parameters
        self.handler = handler
        self.conn_id = conn_id
        self.database = database
        self.split_statements = split_statements
        self.return_last = return_last
        self.show_return_value_in_logs = show_return_value_in_logs
        super().__init__(
            sql=self.sql,
            autocommit=autocommit,
            parameters=parameters,
            handler=handler,
            conn_id=conn_id,
            database=database,
            split_statements=split_statements,
            return_last=return_last,
            show_return_value_in_logs=show_return_value_in_logs,
            **kwargs
        )

    def execute(self, context):
        hook = PostgresHook()

        query = hook.get_first(sql=self.sql)
        context['ti'].xcom_push(key="number_rows", value=query)
