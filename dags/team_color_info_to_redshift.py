from airflow import DAG
from airflow.providers.amazon.aws.transfers.s3_to_redshift import S3ToRedshiftOperator
from airflow.operators.dummy import DummyOperator
from datetime import datetime, timedelta
from airflow.operators.trigger_dagrun import TriggerDagRunOperator

# ✅ S3 & Redshift 설정
S3_BUCKET = "de5-finalproj-team2"
REDSHIFT_TABLE = "analytics.team_color_info"

# ✅ DAG 기본 설정
default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2025, 3, 9),
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

dag = DAG(
    "daily_team_color_info_update",
    default_args=default_args,
    description="매일 Redshift team_color_info 테이블을 최신 데이터로 업데이트",
    schedule_interval= None,    #"45 3 * * *",  # 매일 12:35(KTC) 실행
    catchup=False,
)

# ✅ 시작 태스크 (DummyOperator)
start_task = DummyOperator(
    task_id="start_task",
    dag=dag
)

# ✅ Redshift 적재 (team_color_info)
s3_to_redshift_team_color_info = S3ToRedshiftOperator(
    task_id="load_team_color_info_to_redshift",
    schema="analytics",
    table="team_color_info",
    s3_bucket=S3_BUCKET,
    s3_key="analytics/team_color_info/{{ data_interval_end | ds }}/",
    copy_options=[
        f"FORMAT AS PARQUET"
    ],
    method="REPLACE",
    aws_conn_id="s3_conn_team",
    redshift_conn_id="redshift_conn_team",
    dag=dag
)


trigger_match_info_api_dag = TriggerDagRunOperator(
    task_id="trigger_match_info_api_task",
    trigger_dag_id="collect_match_info_dag",
    wait_for_completion=False
)

# ✅ 실행 순서 정의
start_task >> s3_to_redshift_team_color_info >> trigger_match_info_api_dag
