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

DEFAULT_ARGS = {
    "owner": "airflow",
    "retries": 1,
    "wait_for_downstream": True,
    "depends_on_past":True
}


default_dag = DAG(
    dag_id="collect_match_info_dag",
    default_args=DEFAULT_ARGS,
    description="Collect match data from Nexon API and save to s3 as json",
    schedule_interval= None,             #기존: "40 1 * * *",
    start_date=datetime(2025,3,8),
    catchup=False,
    max_active_runs=1,
    tags=["nexon", "sync", "json", "api"],
)

# Nexon API 설정
API_KEYS = Variable.get('api_4_account_key_mix_2', deserialize_json=True)
API_LIMIT = 1000
api_usage = {key: 0 for key in API_KEYS}
current_key_idx = 0


log = LoggingMixin().log

AWS_ACCESS_KEY = Variable.get("s3_access_key_team")
AWS_SECRET_KEY = Variable.get("s3_secret_key_team")


def get_current_api_key():
    return API_KEYS[current_key_idx]

def switch_to_next_key():
    global current_key_idx
    current_key_idx = (current_key_idx + 1) % len(API_KEYS)
    log.warning(f"429 발생으로 API 키 교체...새로운 키: {get_current_api_key()}")

    

def call_api_with_retry(idx,url):
    """
    API 키 사용량을 저장하고 1,000건 이상일시 다음 API 키 사용.
    429 오류 발생시 0.5초 대기, 그외 오류는 0.3초 대기
    아무런 오류 없을때는 초당 2.5건 데이터 수집

    input:
    - idx: i번쨰 유저
    - url: API end point

    output:
     - 아무런 오류 없으면 response 반환
     - 400 오류나 5회 시도 이후 데이터 추출 실패시 None 반환   
    """
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
            time.sleep(0.5)  # 넉넉하게 0.5초 대기 후 재시도
            continue

        else:
            log.warning(f"API 호출 실패 - status={response.status_code}, url={url}")
            time.sleep(0.3)  # 네트워크 문제 등 대비 대기
            return None

    log.error(f"5회 재시도 실패 - url={url}")
    return None



def fetch_ouid(idx,nickname):
    """
    유저 고유 ID 조회하는 API
    
    input:
    - idx: i번쨰 유저
    - nickname: 유저 닉네임

    output:
     - 아무런 오류 없으면 고유 ID (OUID) 변환
     - 오류 발생 시 None 
    """
    url = f"https://open.api.nexon.com/fconline/v1/id?nickname={quote(nickname)}"
    return call_api_with_retry(idx,url)

def fetch_recent_match(idx,ouid):
    """
    위에서 가지고 온 고유 ID를 기반으로 최근 매치 ID 1개를 API를 통해 가지고 옴.
    만약 없으면 None을 리턴하고 있으면 call_api_with_retry 함수를 통해 데이터 수집

    input:
    - idx: i번쨰 유저
    - oudi: 유저 고유ID

    output:
     - 아무런 오류 없으면 Match ID 변환
     - 오류 발생 시 None  
    """
    url = f"https://open.api.nexon.com/fconline/v1/user/match?ouid={ouid}&matchtype=50&offset=0&limit=1"
    match_list = call_api_with_retry(idx,url)
    if not match_list:
        return None
    if len(match_list) == 0:
        return "no_recent_match"
    return match_list[0]

def fetch_match_details(idx,match_id, ouid):
    """
    match_id를 기능로 메치 상세 정보를 API를 통해 가지고 오는 함수.
    모든 플레이어의 정보가 아닌 조회하고자 하는 플레이어의 데이터만 수집.

    input:
    - idx: i번쨰 유저
    - match_id: 매치 ID
    - ouid: 고유ID

    output:
     - JSON으로 된 매치 세부 정보

    """
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
    """
    위 API를 통해 가지고 오는 데이터를 1개의 Dictionary로 뭉처주는 함수

    input:
    - idx: i번쨰 유저
    - nickname: 유저 닉네임
    - ouid_missing: 고유ID 조회가 되지 않는 유저
    - no_recent_match: 최근 매치가 조회가 되지 않는 유저

    output:
     - JSON으로 된 유저명, ouid가 포함된 매치 세부 정보
    """
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
    """
    S3에 저장된 플레이어 1,000명의 닉내임을 추출 후 API를 통해 데이터 수집 및 결과 S3에 저장
    
    input:
    - execution_date: Dag 실행일 (YYYY-MM-DD)
    - execution_date_nodash: Dag 실행일 (YYYYMMDD)
    """
    log.info(f"Collecting {execution_date}'s data")

    # S3 설정
    S3_BUCKET = "de5-finalproj-team2"
    S3_KEY_CRAWL_PREFIX = f"crawl/{execution_date}/crawl_result_processed_{execution_date_nodash}.csv"
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

    log.info("collecting match_info complete!")
    s3_fs = boto3.resource('s3',aws_access_key_id=AWS_ACCESS_KEY, aws_secret_access_key=AWS_SECRET_KEY)

    s3_fs.Object(S3_BUCKET, f"{S3_KEY_JSON_PREFIX}/match_data/{execution_date}/group_0.json").put(Body=json.dumps(results, ensure_ascii=False))
    s3_fs.Object(S3_BUCKET, f"{S3_KEY_JSON_PREFIX}/errors/{execution_date}/no_ouid/no_ouid_list.json").put(Body=json.dumps(ouid_missing, ensure_ascii=False))
    s3_fs.Object(S3_BUCKET, f"{S3_KEY_JSON_PREFIX}/errors/{execution_date}/no_recent_match/no_recent_match_list.json").put(Body=json.dumps(no_recent_match, ensure_ascii=False))
    s3_fs.Object(S3_BUCKET, f"{S3_KEY_JSON_PREFIX}/api_key_usage/{execution_date}/api_key_usage.json").put(Body=json.dumps(api_usage, ensure_ascii=False))


# NEXON_API에서 수집한 데이터 s3에 저장하는 Task
collect_data_task = PythonOperator(
    task_id="collect_and_save_match_data_to_json_to_s3",
    python_callable=process_data,
    op_kwargs = {"execution_date":'{{ data_interval_end | ds }}',
                "execution_date_nodash":'{{ data_interval_end | ds_nodash }}'},
    dag=default_dag,
)

# 매치 정보를 처리하는 Dag를 트리거
trigger_spark_dag = TriggerDagRunOperator(
    task_id="trigger_spark_match_info_task",
    trigger_dag_id="match_info_spark_processing_dag",
    wait_for_completion=False,
)

collect_data_task >> trigger_spark_dag

