from datetime import datetime, timedelta
import os
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators import (StageToRedshiftOperator, LoadFactOperator,
                               LoadDimensionOperator, DataQualityOperator, CreateTablesOperator)
from helpers import SqlQueries

# AWS credential
aws_credential = "aws_credentials"

# Redshift connection
redshift_conn_id = "redshift"

# S3 Bucket configuration
s3_region = 'us-west-2'
s3_bucket = 'udacity-dend'
s3_key_log = 'log_data'
s3_key_song = 'song_data'
json_path = 's3://udacity-dend/log_json_path.json'

default_args = {
    'owner': 'udacity',
    'start_date': datetime(2019, 1, 12),
    'depends_on_past': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
    'catchup': False,
    'email_on_retry': False
}

dag = DAG('udac_example_dag',
          default_args=default_args,
          description='Load and transform data in Redshift with Airflow',
          schedule_interval='@hourly')

start_operator = DummyOperator(task_id='Begin_execution',  dag=dag)

create_tables = CreateTablesOperator(
    task_id='Create_tables_task',
    dag=dag,
    redshift_conn_id=redshift_conn_id,
    create_table_sql_file=os.path.join(os.path.dirname(os.path.dirname(os.path.realpath(__file__))),
                                       'create_tables.sql')
)

stage_events_to_redshift = StageToRedshiftOperator(
    task_id='Stage_events',
    dag=dag,
    redshift_conn_id=redshift_conn_id,
    aws_credential=aws_credential,
    copy_staging_query=SqlQueries.staging_tables_insert,
    s3_bucket=s3_bucket,
    s3_bucket_region=s3_region,
    s3_key=s3_key_log,
    table='staging_events',
    json_path=json_path)

stage_songs_to_redshift = StageToRedshiftOperator(
    task_id='Stage_songs',
    dag=dag,
    redshift_conn_id=redshift_conn_id,
    aws_credential=aws_credential,
    copy_staging_query=SqlQueries.staging_tables_insert,
    s3_bucket=s3_bucket,
    s3_bucket_region=s3_region,
    s3_key=s3_key_song,
    table='staging_songs',
    json_path='auto')

load_songplays_table = LoadFactOperator(
    task_id='Load_songplays_fact_table',
    dag=dag,
    redshift_conn_id=redshift_conn_id,
    select_query=SqlQueries.songplay_table_insert,
    table='songplays')

load_user_dimension_table = LoadDimensionOperator(
    task_id='Load_user_dim_table',
    dag=dag,
    redshift_conn_id=redshift_conn_id,
    select_query=SqlQueries.user_table_insert,
    table='users')

load_song_dimension_table = LoadDimensionOperator(
    task_id='Load_song_dim_table',
    dag=dag,
    redshift_conn_id=redshift_conn_id,
    select_query=SqlQueries.song_table_insert,
    table='songs')

load_artist_dimension_table = LoadDimensionOperator(
    task_id='Load_artist_dim_table',
    dag=dag,
    redshift_conn_id=redshift_conn_id,
    select_query=SqlQueries.artist_table_insert,
    table='artists')

load_time_dimension_table = LoadDimensionOperator(
    task_id='Load_time_dim_table',
    dag=dag,
    redshift_conn_id=redshift_conn_id,
    select_query=SqlQueries.time_table_insert,
    table='"time"')

run_quality_checks = DataQualityOperator(
    task_id='Run_data_quality_checks',
    dag=dag,
    redshift_conn_id=redshift_conn_id)

end_operator = DummyOperator(task_id='Stop_execution',  dag=dag)

start_operator >> create_tables

create_tables >> stage_events_to_redshift
create_tables >> stage_songs_to_redshift

stage_events_to_redshift >> load_songplays_table
stage_songs_to_redshift >> load_songplays_table

load_songplays_table >> load_user_dimension_table
load_songplays_table >> load_song_dimension_table
load_songplays_table >> load_artist_dimension_table
load_songplays_table >> load_time_dimension_table

load_user_dimension_table >> run_quality_checks
load_song_dimension_table >> run_quality_checks
load_artist_dimension_table >> run_quality_checks
load_time_dimension_table >> run_quality_checks

run_quality_checks >> end_operator
