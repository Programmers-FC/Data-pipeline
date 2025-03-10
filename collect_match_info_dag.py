import time
import json
import boto3
import pandas as pd
from urllib.parse import quote
from airflow.models import Variable
from airflow.utils.log.logging_mixin import LoggingMixin
from datetime import datetime
import requests
from datetime import datetime,timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
import random
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.operators.dummy import DummyOperator

log = LoggingMixin().log

AWS_ACCESS_KEY = Variable.get("s3_access_key_team")
AWS_SECRET_KEY = Variable.get("s3_secret_key_team")

# Nexon API 설정
API_KEYS = Variable.get('api_4_account_key_mix', deserialize_json=True)
API_LIMIT = 1000
api_usage = {key: 0 for key in API_KEYS}
current_key_idx = 0


def get_current_api_key():
    return API_KEYS[current_key_idx]

def switch_to_next_key():
    global current_key_idx
    current_key_idx = (current_key_idx + 1) % len(API_KEYS)
    log.warning(f"429 발생으로 API 키 교체...새로운 키: {get_current_api_key()}")

    

def call_api_with_retry(idx,url):
    global api_usage

    for attempt in range(5):
        key = get_current_api_key()
        headers = {'accept': 'application/json', 'x-nxopen-api-key': key}

        response = requests.get(url, headers=headers)

        if response.status_code == 200:
            api_usage[key] += 1
            log.info(f"[{key}] 현재 사용량: {api_usage[key]} / {API_LIMIT}...{idx+1}번째 유저 처리중")
            time.sleep(0.4)  # 초당 2.5건 맞추기
            return response.json()

        elif response.status_code == 429:
            log.warning(f"429 발생 - {attempt+1}/5회 시도 중... 키 교체")
            switch_to_next_key()
            time.sleep(0.5)  # 넉넉하게 1초 대기 후 재시도
            continue

        else:
            log.warning(f"API 호출 실패 - status={response.status_code}, url={url}")
            time.sleep(0.3)  # 네트워크 문제 등 대비 대기
            return None

    log.error(f"5회 재시도 실패 - url={url}")
    return None



def fetch_ouid(idx,nickname):
    url = f"https://open.api.nexon.com/fconline/v1/id?nickname={quote(nickname)}"
    return call_api_with_retry(idx,url)

def fetch_recent_match(idx,ouid):
    url = f"https://open.api.nexon.com/fconline/v1/user/match?ouid={ouid}&matchtype=50&offset=0&limit=1"
    match_list = call_api_with_retry(idx,url)
    if not match_list:
        return None
    if len(match_list) == 0:
        return "no_recent_match"
    return match_list[0]

def fetch_match_details(idx,match_id, ouid):
    url = f"https://open.api.nexon.com/fconline/v1/match-detail?matchid={match_id}"
    match_data = call_api_with_retry(idx,url)

    if not match_data or "matchInfo" not in match_data:
        return None

    player_info = next((p for p in match_data["matchInfo"] if p["ouid"] == ouid), None)
    if not player_info:
        return None

    return {
        "matchId": match_id,
        "matchDate": match_data.get("matchDate"),
        "seasonId": player_info['matchDetail'].get('seasonId'),
        "matchResult": player_info['matchDetail'].get('matchResult'),
        "player_info": [
            {'spId': p['spId'], 'spPosition': p['spPosition'], 'spGrade': p['spGrade']}
            for p in player_info['player']
        ]
    }

def process_user(idx,nickname, ouid_missing, no_recent_match):
    ouid_data = fetch_ouid(idx,nickname)
    if not ouid_data or "ouid" not in ouid_data:
        ouid_missing.append(nickname)
        return None

    ouid = ouid_data['ouid']
    match_id = fetch_recent_match(idx,ouid)

    if match_id == "no_recent_match":
        no_recent_match.append(nickname)
        return None

    match_details = fetch_match_details(idx,match_id, ouid)
    if not match_details:
        return None
    
    return {"nickname": nickname, "ouid": ouid, **match_details}

def process_data(execution_date,execution_date_nodash):

    log.info(f"Collecting {execution_date}'s data")

    # S3 설정
    S3_BUCKET = "de5-finalproj-team2"
    S3_KEY_CRAWL_PREFIX = f"crawl/{execution_date}/crawl_result_processed_{execution_date_nodash}.csv"
  #"s3://de5-finalproj-team2/crawl/2025-03-07/crawl_result_processed_20250307.csv"
    S3_KEY_JSON_PREFIX = "raw_data/json"


    s3 = boto3.client('s3', aws_access_key_id=AWS_ACCESS_KEY, aws_secret_access_key=AWS_SECRET_KEY)
    obj = s3.get_object(Bucket=S3_BUCKET, Key=S3_KEY_CRAWL_PREFIX)
    df = pd.read_csv(obj['Body'])
    nickname_list = df['감독명'].tolist()[:1000]

    results, ouid_missing, no_recent_match = [], [], []

    for idx,nickname in enumerate(nickname_list):
        result = process_user(idx,nickname, ouid_missing, no_recent_match)
        if result:
            results.append(result)

    s3_fs = boto3.resource('s3',aws_access_key_id=AWS_ACCESS_KEY, aws_secret_access_key=AWS_SECRET_KEY)

    s3_fs.Object(S3_BUCKET, f"{S3_KEY_JSON_PREFIX}/match_data/{execution_date}/group_0.json").put(Body=json.dumps(results, ensure_ascii=False))
    s3_fs.Object(S3_BUCKET, f"{S3_KEY_JSON_PREFIX}/errors/{execution_date}/no_ouid/no_ouid_list.json").put(Body=json.dumps(ouid_missing, ensure_ascii=False))
    s3_fs.Object(S3_BUCKET, f"{S3_KEY_JSON_PREFIX}/errors/{execution_date}/no_recent_match/no_recent_match_list.json").put(Body=json.dumps(no_recent_match, ensure_ascii=False))
    s3_fs.Object(S3_BUCKET, f"{S3_KEY_JSON_PREFIX}/api_key_usage/{execution_date}/api_key_usage.json").put(Body=json.dumps(api_usage, ensure_ascii=False))

DEFAULT_ARGS = {
    "owner": "airflow",
    "depends_on_past": False,
    "wait_for_downstream": True,
    "retries": 1,
    "retry_delay": timedelta(minutes=1),
}

default_dag = DAG(
    dag_id="fetch_nexon_data_sequential",
    default_args=DEFAULT_ARGS,
    description="Collect data from Nexon API and save to s3 as json",
    schedule_interval="0 1 * * *",
    start_date=datetime(2025,3,8),
    catchup=False,
    max_active_runs=1,
    tags=["nexon", "sync", "json", "api"],
)

# NEXON_API에서 수집한 데이터 s3에 저장하는 Task
collect_data_task = PythonOperator(
    task_id="collect_and_save_match_data_to_json_to_s3",
    python_callable=process_data,
    op_kwargs={"execution_date": "{{ dag_run.logical_date.strftime('%Y-%m-%d') }}",
                "execution_date_nodash": "{{ dag_run.logical_date.strftime('%Y%m%d') }}"},  
    dag=default_dag,
)

trigger_spark_dag = TriggerDagRunOperator(
    task_id="trigger_spark_match_info_task",
    trigger_dag_id="submit_spark_job_v3",
    wait_for_completion=True,
)

collect_match_info = DummyOperator(task_id="collect_match_info_starter")

collect_match_info >> collect_data_task >> trigger_spark_dag

