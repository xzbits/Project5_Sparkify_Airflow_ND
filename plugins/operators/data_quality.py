from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults


class DataQualityOperator(BaseOperator):
    ui_color = '#89DA59'

    @apply_defaults
    def __init__(self, redshift_conn_id, dq_check_queries, *args, **kwargs):
        super(DataQualityOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.dq_chek_queries = dq_check_queries

    def execute(self, context):
        self.log.info('Starting DataQualityOperator for Data Quality check queries')

        # Create AWS and Redshift hook, and get AWS credential
        redshift_hook = PostgresHook(self.redshift_conn_id)

        # Execute queries in Data Quality check queries and check with expected result
        for dq_query in self.dq_chek_queries:
            record = redshift_hook.get_records(dq_query['check_sql'])
            if len(record) < 1 or len(record[0]) < 1:
                raise ValueError('DataQualityOperator is Failed, because query: '
                                 '{} did not return any results'.format(dq_query['check_sql']))
            else:
                dq_flag = eval(str(record[0][0]) + dq_query['expected_result'])
                if dq_flag:
                    self.log.info('DataQualityOperator is Success, {} query return {} {}, which is expected result'
                                  .format(dq_query, record[0][0], dq_query['expected_result']))
                else:
                    raise ValueError("DataQualityOperator is Failed, {} query return {} {}, "
                                     "which is not expected result".format(dq_query,
                                                                           record[0][0],
                                                                           dq_query['expected_result']))

        self.log.info('Finish DataQualityOperator for Data Quality check queries')
