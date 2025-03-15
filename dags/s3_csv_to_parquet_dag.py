from airflow import DAG
from airflow.providers.ssh.operators.ssh import SSHOperator
from datetime import datetime, timedelta
from airflow.operators.trigger_dagrun import TriggerDagRunOperator

# DAG 기본 설정
default_args = {
    "owner": "airflow",
    "start_date": datetime(2025, 3, 9), 
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
    "wait_for_downstream": True,
    "depends_on_past":True
}

dag = DAG(
    "s3_csv_to_parquet_dag",
    default_args=default_args,
    schedule_interval="10 2 * * *", # KST 오전 11시 10분에 시작
    catchup=False,
)

# Spark 실행 스크립트 경로 (Spark 서버 내 위치)
SPARK_SCRIPT_PATH = "/home/ubuntu/spark_scripts/rankingInfo_To_Parquet.py"

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
trigger_team_color_dag = TriggerDagRunOperator(
    task_id="trigger_team_color_table_task",
    trigger_dag_id="team_color_to_parquet_dag",
    wait_for_completion=False
)

#Team color table을 생성하는 Dag 트기거
trigger_ranking_redshift_dag = TriggerDagRunOperator(
    task_id="trigger_ranking_info_redshift_task",
    trigger_dag_id="daily_ranking_info_update",
    wait_for_completion=False
)

ssh_spark_submit >> [trigger_team_color_dag,trigger_ranking_redshift_dag]
