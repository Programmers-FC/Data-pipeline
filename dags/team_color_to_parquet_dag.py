from airflow import DAG
from airflow.providers.ssh.operators.ssh import SSHOperator
from datetime import datetime, timedelta
from airflow.operators.trigger_dagrun import TriggerDagRunOperator

# DAG 기본 설정
default_args = {
    "owner": "airflow",
    "start_date": datetime(2025, 3, 1),  # 실행 시작 날짜
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
    "wait_for_downstream": True,
    "depends_on_past":True
}

dag = DAG(
    "team_color_to_parquet_dag",
    default_args=default_args,
    schedule_interval = None,  #기존 : "40 1 * * *",
    catchup=False
)

# Spark 실행 스크립트 경로 (Spark 서버 내 위치)
SPARK_SCRIPT_PATH = "/home/ubuntu/spark_scripts/team_color_to_parquet.py"

# SSHOperator를 이용한 Spark 실행
ssh_spark_submit = SSHOperator(
    task_id="submit_spark_via_ssh",
    ssh_conn_id="spark_ssh_conn",  # Airflow에서 설정한 SSH 연결 ID
    command="""
    set -e
    echo "Starting Spark Job on {{ data_interval_end | ds }}"
    /spark-3.5.3/bin/spark-submit {{ params.spark_script }} {{ data_interval_end | ds }}
    exit_code=$?

    if [ $exit_code -ne 0 ]; then
        echo "Spark Job Failed! Check logs."
        exit 1
    else
        echo "Spark Job Succeeded!"
    fi
    """,
    params={"spark_script": SPARK_SCRIPT_PATH},  # SPARK_SCRIPT_PATH 전달
    conn_timeout=600,
    cmd_timeout=600,
    dag=dag,
)

#Team color table을 생성하는 Dag 트기거
trigger_match_info_dag = TriggerDagRunOperator(
    task_id="trigger_team_color_info_redshift_task",
    trigger_dag_id="daily_team_color_info_update",
    wait_for_completion=False
)

ssh_spark_submit >> trigger_match_info_dag
