from airflow import DAG
from airflow.providers.ssh.operators.ssh import SSHOperator
from datetime import datetime,timedelta
from airflow.operators.dummy import DummyOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator

default_args = {
    "owner": "airflow",
    "start_date": datetime(2025, 3, 10),
    'retries': 2,
    'retry_delay': timedelta(minutes=1),
    "wait_for_downstream": True,
    "depends_on_past":True
}

dag = DAG(
    dag_id="sparkml_prediction",
    default_args=default_args,
    schedule_interval="0 6 * * *",
    tags=['s3', 'sparkml', 'spark'],
    catchup=False,
    max_active_runs = 1
)

task1 = DummyOperator(task_id="task1")

SPARK_SERVER = "ec2-3-37-216-159.ap-northeast-2.compute.amazonaws.com"
SPARK_SCRIPT_PATH = "/home/ubuntu/spark_scripts/sparkml_result.py"

ssh_spark_submit = SSHOperator(
    task_id="submit_spark_via_ssh",
    ssh_conn_id="spark_ssh_conn", #airflow에서 정의해야할 conn_id#일단 상원님이랑 똑같이 맞췄습니다
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
        "spark_server": SPARK_SERVER,
        "spark_script": SPARK_SCRIPT_PATH,
    },
    dag=dag,
)


trigger_review_info_redshift_dag = TriggerDagRunOperator(
    task_id="trigger_review_info_redshift_task",
    trigger_dag_id="daily_review_data_update",
    wait_for_completion=False
)

task1 >> ssh_spark_submit >> trigger_review_info_redshift_dag
