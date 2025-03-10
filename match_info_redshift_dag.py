from airflow import DAG
from airflow.providers.amazon.aws.transfers.s3_to_redshift import S3ToRedshiftOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from datetime import datetime,timedelta
import logging
from airflow.operators.dummy import DummyOperator

dag = DAG(
    dag_id = 'parquet_to_redshift',
    start_date = datetime(2025,3,7), 
    schedule = '30 1 * * *',  
    max_active_runs = 1,
    catchup = False,
    tags = ['s3','match_info','redshift','parquet'],
    default_args = {
        'retries': 1,
        'retry_delay': timedelta(minutes=3),
    }
)

S3_BUCKET = "de5-finalproj-team2"

update_redshift = DummyOperator(task_id="update_redshift_task")

s3_to_redshift_match_info = S3ToRedshiftOperator(
    task_id = 's3_to_redshift_match_data',
    s3_bucket = S3_BUCKET,
    s3_key = "analytics/match_data/{{ dag_run.logical_date.strftime('%Y-%m-%d') }}/",
    schema = 'analytics',
    table = 'match_info',
    copy_options=["FORMAT AS PARQUET"],
    method = 'REPLACE',
    redshift_conn_id = "redshift_conn_team",
    aws_conn_id = "s3_conn_team",
    dag = dag
)

s3_to_redshift_match_trend_info = S3ToRedshiftOperator(
    task_id = 's3_to_redshift_match_trend_data',
    s3_bucket = S3_BUCKET,
    s3_key = "trend_analysis/match_data/{{ dag_run.logical_date.strftime('%Y-%m-%d') }}/",
    schema = 'trend_analytics',
    table = 'match_info',
    copy_options=["FORMAT AS PARQUET"],
    method = 'UPSERT',
    upsert_keys = ["match_id","match_date"],
    redshift_conn_id = "redshift_conn_team",
    aws_conn_id = "s3_conn_team",
    dag = dag
)


update_redshift >> s3_to_redshift_match_info >> s3_to_redshift_match_trend_info