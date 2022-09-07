from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults


class CreateTablesOperator(BaseOperator):

    @apply_defaults
    def __init__(self, redshift_conn_id, create_table_sql_file, *args, **kwargs):

        super(CreateTablesOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.create_table_sql_file = create_table_sql_file

    def execute(self, context):
        self.log.info('Starting CreateTablesOperator for all required tables')

        # Create AWS and Redshift hook
        redshift_hook = PostgresHook(self.redshift_conn_id)

        # Read CREATE table queries in create_table_sql_file
        sql_file = open(self.create_table_sql_file, 'r')
        create_queries = sql_file.read().split(";")
        sql_file.close()

        for create_query in create_queries:
            create_query = create_query.strip()
            if create_query != "":
                redshift_hook.run(create_query)
        self.log.info('Finish CreateTablesOperator for all required tables')
