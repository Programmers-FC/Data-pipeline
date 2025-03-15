from airflow import DAG
from airflow.providers.amazon.aws.transfers.s3_to_redshift import S3ToRedshiftOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from datetime import datetime,timedelta
import logging
from airflow.operators.dummy import DummyOperator
from airflow.operators.bash import BashOperator


default_args = {
    "owner": "airflow",
    "start_date": None,
    "retries": 1,
    "wait_for_downstream": True,
    "depends_on_past":True
}

 
dag = DAG(
    dag_id = 'match_info_to_redshift_dag',
    start_date = datetime(2025,3,7), 
    schedule_interval = None, #기존: schedule_interval = '0 3 * * *', 
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
    s3_key = "analytics/match_data/{{ data_interval_end | ds }}/",
    schema = 'analytics',
    table = 'match_info',
    copy_options=["FORMAT AS PARQUET"],
    method = 'REPLACE',
    redshift_conn_id = "redshift_conn_team",
    aws_conn_id = "s3_conn_team",
    dag = dag
)

# s3_to_redshift_match_trend_info = S3ToRedshiftOperator(
#     task_id = 's3_to_redshift_match_trend_data',
#     s3_bucket = S3_BUCKET,
#     s3_key = "trend_analysis/match_data/{{ data_interval_end | ds }}/",
#     schema = 'trend_analytics',
#     table = 'match_info',
#     copy_options=["FORMAT AS PARQUET"],
#     method = 'UPSERT',
#     upsert_keys = ["match_id","match_date"],
#     redshift_conn_id = "redshift_conn_team",
#     aws_conn_id = "s3_conn_team",
#     dag = dag
# )

# hello_task = BashOperator(
#     task_id='print_hello',  # 태스크 ID
#     bash_command='echo "Hello Redshift"',  # 실행할 명령어
#     dag = dag
# )

# update_redshift >> s3_to_redshift_match_info #>> s3_to_redshift_match_trend_info


update_redshift >> s3_to_redshift_match_info