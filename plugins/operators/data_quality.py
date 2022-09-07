from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults


class DataQualityOperator(BaseOperator):
    ui_color = '#89DA59'

    check_null_query = """
    SELECT COUNT(*) FROM public.{} WHERE {} IS NULL
    """

    get_pk_query = """
    SELECT kcu.column_name
    FROM information_schema.table_constraints AS tco
    JOIN information_schema.key_column_usage AS kcu 
        ON kcu.constraint_name = tco.constraint_name 
        AND kcu.constraint_schema = tco.constraint_schema
        AND kcu.constraint_name = tco.constraint_name
    WHERE tco.constraint_type = 'PRIMARY KEY' 
        AND tco.table_schema = 'public'
        AND tco.table_name = '{}'
    """

    @apply_defaults
    def __init__(self, redshift_conn_id, tables=None, *args, **kwargs):
        super(DataQualityOperator, self).__init__(*args, **kwargs)
        if tables is None:
            tables = ['songplays', 'users', 'artists', 'time', 'songs']
        self.redshift_conn_id = redshift_conn_id
        self.tables = tables

    def execute(self, context):
        self.log.info('Starting DataQualityOperator for all existing tables')

        # Create AWS and Redshift hook, and get AWS credential
        redshift_hook = PostgresHook(self.redshift_conn_id)

        # Check Empty tables
        for one_table in self.tables:
            record = redshift_hook.get_records('SELECT COUNT(*) FROM {}'.format(one_table))

            if len(record) < 1 or len(record[0]) < 1:
                raise ValueError("DataQualityOperator is Failed, because {} table did not return any results"
                                 .format(one_table))
            else:
                no_records = record[0][0]
                if no_records < 1:
                    raise ValueError("DataQualityOperator is Failed, because {} table has 0 row".format(one_table))
                else:
                    self.log.info("DataQualityOperator is Success, {} table contains {} records".format(one_table,
                                                                                                        no_records))

        # Check NULL values in Primary Key
        for one_table in self.tables:
            record_pk_name = redshift_hook.get_records(self.get_pk_query.format(one_table))
            if len(record_pk_name) < 1:
                self.log.info('Table {} does not have Primary Key'.format(one_table))
                continue
            else:
                for one_pk in record_pk_name:
                    col_name = one_pk[0]
                    check_null_record = redshift_hook.get_records(self.check_null_query.format(one_table, col_name))

                    if check_null_record[0][0] > 0:
                        raise ValueError("DataQualityOperator is Failed, because {} table contains {} null "
                                         "values in Primary Key".format(one_table, check_null_record[0][0]))
                    else:
                        self.log.info("DataQualityOperator is Success, {} table contains "
                                      "0 null value".format(one_table))

        self.log.info('Finish DataQualityOperator for all existing tables')
