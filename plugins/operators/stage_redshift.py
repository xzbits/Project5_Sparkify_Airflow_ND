from airflow.hooks.postgres_hook import PostgresHook
from airflow.contrib.hooks.aws_hook import AwsHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults


class StageToRedshiftOperator(BaseOperator):
    ui_color = '#358140'

    @apply_defaults
    def __init__(self, redshift_conn_id, aws_credential, copy_staging_query, s3_bucket, s3_bucket_region,
                 s3_key, table, json_path, delete_load=True, *args, **kwargs):

        super(StageToRedshiftOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.aws_credential = aws_credential

        # SQL queries
        self.copy_staging_query = copy_staging_query

        # S3 configuration
        self.s3_bucket = s3_bucket
        self.s3_key = s3_key
        self.s3_bucket_region = s3_bucket_region

        self.table = table
        self.json_path = json_path
        self.delete_load = delete_load

    def execute(self, context):
        self.log.info('Starting StageToRedshiftOperator for {} table'.format(self.table))

        # Create AWS and Redshift hook, and get AWS credential
        aws_hook = AwsHook(self.aws_credential)
        redshift_hook = PostgresHook(self.redshift_conn_id)
        credentials = aws_hook.get_credentials()
        key = credentials.access_key
        secret = credentials.secret_key

        # DELETE data from destination Redshift table
        if self.delete_load:
            self.log.info("Clearing data from destination Redshift table")
            delete_query = "DELETE FROM {}".format(self.table)
            redshift_hook.run(delete_query)

        # COPY records into staging tables
        self.log.info("Copying data from S3 to Redshift")
        rendered_key = self.s3_key.format(**context)
        s3_path = "s3://{}/{}".format(self.s3_bucket, rendered_key)

        copy_query = self.copy_staging_query.format(table=self.table,
                                                    json_files_path=s3_path,
                                                    aws_key=key,
                                                    aws_secret=secret,
                                                    json_path=self.json_path,
                                                    s3_bucket_region=self.s3_bucket_region)
        redshift_hook.run(copy_query)

        self.log.info("Finish StageToRedshiftOperator for {} table".format(self.table))
