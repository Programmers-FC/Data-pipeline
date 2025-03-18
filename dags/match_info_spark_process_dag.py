from airflow import DAG
from airflow.providers.ssh.operators.ssh import SSHOperator
from datetime import datetime
from airflow.operators.dummy import DummyOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.operators.bash import BashOperator

DEFAULT_ARGS = {
    "owner": "airflow",
    "start_date": None,
    "retries": 1,
    "wait_for_downstream": True,
    "depends_on_past":True
}

dag = DAG(
    dag_id="match_info_spark_processing_dag",
    default_args=DEFAULT_ARGS,
    schedule_interval=None,
    tags=['s3', 'match_info', 'raw_data', 'parquet', 'spark'],
    catchup=True,
    max_active_runs = 1
)

process_match_info = DummyOperator(task_id="process_match_info_task")

#Spark 서버에 있는 Spark script 위치
SPARK_SCRIPT_PATH = "/home/ubuntu/spark_scripts/match_info_pyspark.py"

ssh_spark_submit = SSHOperator(
    task_id="submit_spark_via_ssh",
    ssh_conn_id="spark_ssh_conn",
    command="""
    set -e  # 명령어 실패 시 즉시 종료
    echo "Starting Spark Job on {{ data_interval_end | ds }}"

    echo "Submitting Spark Job... for {{ data_interval_end | ds }} data"
   /spark-3.5.3/bin/spark-submit {{ params.spark_script }} {{ data_interval_end | ds }} > spark_job.log 2>&1
    exit_code=$?

    # 실행 로그 출력 (Airflow에서 확인 가능)
    if [ -f spark_job.log ]; then
        echo "=== Spark Job Log Start ==="
        cat spark_job.log
        echo "=== Spark Job Log End ==="
    else
        echo "Warning: spark_job.log not found!"
    fi

    # Spark 실패 감지
    if [ $exit_code -ne 0 ]; then
        echo "Spark Job Failed! Check logs."
        exit 1
    else
        echo "Spark Job Succeeded!"
    fi
    """,
    conn_timeout=600,  # SSH 연결 타임아웃 (초 단위)
    cmd_timeout=600,  # 명령어 실행 타임아웃 (초 단위)
    params={
        "spark_script": SPARK_SCRIPT_PATH,
    },
    dag=dag,
)




trigger_redshift_dag = TriggerDagRunOperator(
    task_id="trigger_redshift_dag_task",
    trigger_dag_id="match_info_to_redshift_dag",
    wait_for_completion=False
)


process_match_info >> ssh_spark_submit >> trigger_redshift_dag
