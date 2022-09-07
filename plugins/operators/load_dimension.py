from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults


class LoadDimensionOperator(BaseOperator):
    ui_color = '#80BD9E'

    @apply_defaults
    def __init__(self, redshift_conn_id, select_query, table, delete_load=True, *args, **kwargs):
        super(LoadDimensionOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.select_query = select_query
        self.table = table
        self.delete_load = delete_load

    def execute(self, context):
        self.log.info('Starting LoadDimensionOperator for {} table'.format(self.table))

        # Create Redshift hook
        redshift_hook = PostgresHook(self.redshift_conn_id)

        # DELETE data from destination Redshift table
        if self.delete_load:
            self.log.info("Clearing data from destination Redshift table")
            delete_query = "DELETE FROM {}".format(self.table)
            redshift_hook.run(delete_query)

        # INSERT records into dimension table
        insert_query = "INSERT INTO {} {}".format(self.table, self.select_query)
        redshift_hook.run(insert_query)

        self.log.info("Finish LoadDimensionOperator for {} table".format(self.table))
