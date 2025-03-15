from airflow import DAG
from airflow.providers.amazon.aws.transfers.s3_to_redshift import S3ToRedshiftOperator
from airflow.operators.dummy import DummyOperator
from airflow.models import Variable  
from datetime import datetime, timedelta
from airflow.operators.trigger_dagrun import TriggerDagRunOperator

# ✅ Airflow Variable에서 IAM_ROLE 가져오기
IAM_ROLE = Variable.get("IAM_ROLE")

# ✅ S3 & Redshift 설정
S3_BUCKET = "de5-finalproj-team2"
REDSHIFT_TABLE = "analytics.ranking_info"

# ✅ DAG 기본 설정
default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2025, 3, 9),
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

dag = DAG(
    "daily_ranking_info_update",
    default_args=default_args,
    description="매일 Redshift 테이블을 드롭 후 최신 데이터로 재적재",
    schedule_interval=None, #"35 3 * * *",  # 매일 12:35(KTC) 실행
    catchup=False,
)

# ✅ DummyOperator (시작 태스크)
start_task = DummyOperator(
    task_id="start_task",
    dag=dag
)

# ✅ Redshift 적재
s3_to_redshift_ranking_info = S3ToRedshiftOperator(
    task_id="load_ranking_info_to_redshift",
    schema="analytics",
    table="ranking_info",
    s3_bucket=S3_BUCKET,
    s3_key="analytics/ranking_info/{{ data_interval_end | ds }}/",  
    copy_options=[
        f"FORMAT AS PARQUET"],
    method="REPLACE",
    aws_conn_id="s3_conn_team",
    redshift_conn_id="redshift_conn_team",
    dag=dag
)


# ✅ 실행 순서 정의
start_task >> s3_to_redshift_ranking_info 