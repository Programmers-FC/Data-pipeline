import time
import json
import boto3
import pandas as pd
from urllib.parse import quote
from airflow.decorators import dag, task
from airflow.utils.dates import days_ago
from airflow.models import Variable
from airflow.utils.log.logging_mixin import LoggingMixin
from datetime import datetime
import requests
from ratelimit import limits, sleep_and_retry

log = LoggingMixin().log

# S3 설정
S3_BUCKET = "de-test-bucket-lsw"
S3_KEY_CRAWL_PREFIX = "test/crawl/crawl_result.csv"
S3_KEY_JSON_PREFIX = "test/json"

AWS_ACCESS_KEY = Variable.get("s3_access_key_personal")
AWS_SECRET_KEY = Variable.get("s3_secret_key_personal")

# Nexon API 설정
API_KEYS = Variable.get('nexon_api', deserialize_json=True)
API_LIMIT = 1000
RATE_LIMIT = 5  # 초당 5건
api_usage = {key: 0 for key in API_KEYS}
current_key_idx = 0
GROUP_COUNT = 5
GROUP_KEYS = [API_KEYS[i::GROUP_COUNT] for i in range(GROUP_COUNT)]

# 그룹별 키 사용 이력 관리
group_key_usage = {i: {key: 0 for key in keys} for i, keys in enumerate(GROUP_KEYS)}

# 그룹별 현재 키 인덱스
group_current_key_idx = {i: 0 for i in range(GROUP_COUNT)}


def get_group_api_key(group_index):
    keys = GROUP_KEYS[group_index]
    current_idx = group_current_key_idx[group_index]
    key = keys[current_idx]

    # 사용량 1,000건 초과 시 다음 키로 변경
    if group_key_usage[group_index][key] >= 1000:
        log.warning(f"[Group {group_index}] API Key {key} 소진됨 → 다음 키로 변경")
        group_current_key_idx[group_index] = (current_idx + 1) % len(keys)
        key = keys[group_current_key_idx[group_index]]

    return key

def log_api_usage(group_index):
    usage = group_key_usage[group_index]
    log.info(f"[Group {group_index}] 현재 API 사용량: {json.dumps(usage, indent=2)}")


def call_api_with_throttle(group_index, url):
    key = get_group_api_key(group_index)
    headers = {'accept': 'application/json', 'x-nxopen-api-key': key}

    for attempt in range(3):
        response = requests.get(url, headers=headers)

        if response.status_code == 429:
                    log.warning(f"[Group {group_index}] 429 발생 - {attempt+1}/3회 재시도 예정")
                    time.sleep(0.2)  # 재시도 전 대기
                    continue
        
        if response.status_code != 200:
            log.warning(f"[Group {group_index}] API 실패 (status={response.status_code}) - URL: {url}")
            return None

        group_key_usage[group_index][key] += 1
        log_api_usage(group_index)

        time.sleep(0.3)  # 초당 3건 정확히 맞추기
        return response.json()

def fetch_ouid(group_index, nickname):
    url = f"https://open.api.nexon.com/fconline/v1/id?nickname={quote(nickname)}"
    return call_api_with_throttle(group_index, url)

def fetch_recent_match(group_index, ouid):
    url = f"https://open.api.nexon.com/fconline/v1/user/match?ouid={ouid}&matchtype=50&offset=0&limit=1"
    match_list = call_api_with_throttle(group_index, url)
    if not match_list:
        return None
    if len(match_list) == 0:
        return "no_recent_match"
    return match_list[0]

def fetch_match_details(group_index, match_id, ouid):
    url = f"https://open.api.nexon.com/fconline/v1/match-detail?matchid={match_id}"
    match_data = call_api_with_throttle(group_index, url)

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


def process_user(group_index, nickname, ouid_missing, no_recent_match):
    ouid_data = fetch_ouid(group_index, nickname)
    if not ouid_data or "ouid" not in ouid_data:
        ouid_missing.append(nickname)
        return None

    ouid = ouid_data['ouid']
    match_id = fetch_recent_match(group_index, ouid)

    if match_id == "no_recent_match":
        no_recent_match.append(nickname)
        return None

    match_details = fetch_match_details(group_index, match_id, ouid)
    if not match_details:
        return None

    return {"nickname": nickname, "ouid": ouid, **match_details}

def process_group(group_index, collection_date):
    log.info(f"[Group {group_index}] 수집 시작")

    s3 = boto3.client('s3', aws_access_key_id=AWS_ACCESS_KEY, aws_secret_access_key=AWS_SECRET_KEY)
    obj = s3.get_object(Bucket=S3_BUCKET, Key=S3_KEY_CRAWL_PREFIX)
    df = pd.read_csv(obj['Body'])
    nickname_list = df['감독명'].tolist()[:60]

    group_size = len(nickname_list) // GROUP_COUNT
    group_nicknames = nickname_list[group_index * group_size:(group_index + 1) * group_size]

    results, ouid_missing, no_recent_match = [], [], []

    for nickname in group_nicknames:
        result = process_user(group_index, nickname, ouid_missing, no_recent_match)
        if result:
            results.append(result)

    s3_path = f"s3://{S3_BUCKET}/{S3_KEY_JSON_PREFIX}"
    s3_fs = boto3.resource('s3',aws_access_key_id=AWS_ACCESS_KEY, aws_secret_access_key=AWS_SECRET_KEY)

    s3_fs.Object(S3_BUCKET, f"{S3_KEY_JSON_PREFIX}/match_data/{collection_date}/group_{group_index}.json").put(Body=json.dumps(results, ensure_ascii=False))
    s3_fs.Object(S3_BUCKET, f"{S3_KEY_JSON_PREFIX}/match_data/{collection_date}/ouid_missing/group_{group_index}.json").put(Body=json.dumps(ouid_missing, ensure_ascii=False))
    s3_fs.Object(S3_BUCKET, f"{S3_KEY_JSON_PREFIX}/match_data/{collection_date}/no_recent_match/group_{group_index}.json").put(Body=json.dumps(no_recent_match, ensure_ascii=False))

@dag(schedule=None, start_date=days_ago(1), catchup=False,tags=["nexon", "sync", "dynamic"])
def fetch_nexon_data_sync():
    @task
    def process_group_wrapper(group_index):
        process_group(group_index, datetime.today().strftime('%Y-%m-%d'))

    process_group_wrapper.expand(group_index=list(range(5)))

fetch_nexon_data_sync()
