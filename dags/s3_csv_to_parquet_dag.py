from airflow import DAG
from airflow.providers.ssh.operators.ssh import SSHOperator
from datetime import datetime, timedelta

# DAG 기본 설정
default_args = {
    "owner": "airflow",
    "start_date": datetime(2025, 3, 9), 
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

dag = DAG(
    "s3_csv_to_parquet_dag",
    default_args=default_args,
    schedule_interval="0 12 * * *",
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

ssh_spark_submit
