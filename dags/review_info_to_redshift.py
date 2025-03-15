from airflow import DAG
from airflow.providers.amazon.aws.transfers.s3_to_redshift import S3ToRedshiftOperator
from airflow.operators.dummy import DummyOperator
from datetime import datetime, timedelta
from airflow.operators.trigger_dagrun import TriggerDagRunOperator

# ✅ S3 & Redshift 설정
S3_BUCKET = "de5-finalproj-team2"
SCHEMA = "analytics"
TABLE = "player_review_info"

# ✅ DAG 기본 설정
default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2025, 3, 9),
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

dag = DAG(
    "daily_review_data_update",
    default_args=default_args,
    description="매일 review_data_ML 데이터를 Redshift에 적재",
    schedule_interval=None, #"50 3 * * *",  # ✅ 매일 12:50(KTC) 실행
    catchup=False,
)

# ✅ 시작 태스크
start_task = DummyOperator(
    task_id="start_task",
    dag=dag
)

# ✅ S3 → Redshift 적재
s3_to_redshift_review_data = S3ToRedshiftOperator(
    task_id="load_review_data_to_redshift",
    schema=SCHEMA,
    table=TABLE,
    s3_bucket=S3_BUCKET,
    s3_key="analytics/review_data_ML/{{ data_interval_end | ds }}/",
    copy_options=[
        f"FORMAT AS PARQUET"
    ],
    method="REPLACE",
    aws_conn_id="s3_conn_team",
    redshift_conn_id="redshift_conn_team",
    dag=dag
)

# ✅ 실행 순서 정의
start_task >> s3_to_redshift_review_data
