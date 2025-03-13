from airflow import DAG
#from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.operators.python import PythonOperator
from airflow.models import Variable
from datetime import datetime
import pandas as pd
from datetime import timedelta
#from pandas import Timestamp
import pandas as pd
import logging
import requests
from airflow.operators.python import get_current_context
from selenium import webdriver
from webdriver_manager.chrome import ChromeDriverManager
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.support import expected_conditions as EC
import os
import boto3
import io
import json
import time
import sys


# if len(sys.argv) != 2:
#     print("Usage: spark-submit <script.py> <processing_date>")
#     sys.exit(1)

# processing_date = sys.argv[1]
# print(f"Processing date: {processing_date}")




aws_key=Variable.get("aws_key")
aws_secret_key=Variable.get("aws_secret_key")

#s3에서 선수목록 가져오기
def list_from_s3(context):
    s3=boto3.client("s3", aws_access_key_id=aws_key, aws_secret_access_key=aws_secret_key)
    date = context['ds_nodash']
    date = datetime.strptime(date, "%Y%m%d").strftime("%Y-%m-%d")
    data=s3.get_object(Bucket="de5-finalproj-team2",Key= f"raw_data/json/match_data/{date}/group_0.json")
    json_data = json.loads(data['Body'].read().decode('utf-8'))
    player_list = []
    for user in range(len(json_data)):
        for player in range(18):
            try:
                player_num = json_data[user]['player_info'][player]['spId']
                player_list.append(player_num)
            except (KeyError, IndexError) as e:
                print(f"오류 발생: {e} (user: {user}, player: {player})")
                continue
    return list(set(player_list))





default_args = {
    'retries': 2,
    'retry_delay': timedelta(minutes=1),
}

dag = DAG(
    dag_id='players_review_data_final',
    start_date=datetime(2025, 3, 3),
    schedule_interval='0 8 * * *',  # 매일 08:00 실행
    max_active_runs=1,
    catchup=False,
    tags=['selenium'],
    default_args=default_args,
)

def get_data(**context):
    """FIFA Online4 선수 리뷰 데이터를 크롤링하여 CSV 파일로 저장"""
    
    # Selenium 크롤링 설정
    options = Options()
    options.add_argument("user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/133.0.0.0 Safari/537.36")
    options.add_argument("--headless")  # 브라우저 숨기기 (메모리 절약)
    options.add_argument("--disable-gpu")  # GPU 사용 안 함
    options.add_argument("--disable-software-rasterizer")  # 소프트웨어 렌더링 비활성화
    options.add_argument("--disable-dev-shm-usage")  # 메모리 공유 공간 제한 해제
    options.add_argument("--no-sandbox")  # 샌드박스 모드 비활성화 (Linux 환경)
    options.add_argument("--disable-cache")  # 캐시 비활성화
    options.add_argument("--disk-cache-size=0")  # 디스크 캐시 크기 0으로 설정
    options.add_argument("--remote-debugging-port=9222")  # 디버깅 포트 추가
    options.add_argument("--disable-background-timer-throttling")  # 백그라운드 스레드 제한 비활성화
    options.add_argument("--disable-backgrounding-occluded-windows")  # 백그라운드 창 동작 비활성화
    #새로 추가
    # options.add_argument("--single-process") #모든 크롬 프로세스를 하나의 프로세스로 실행
    # options.add_argument("--disable-breakpad") ## 크롬의 크래시 리포트 기능 비활성화
    # options.add_argument("--disable-features=TranslateUI,BlinkGenPropertyTrees") # 불필요한 기능 비활성화
    # # - TranslateUI: 페이지 번역 UI 비활성화 (크롤링 시 필요 없음)
    # # - BlinkGenPropertyTrees: Blink 엔진의 일부 최적화 기능 비활성화 (렌더링 속도와 안정성 관련)
    # options.add_argument("--disable-sync") # 브라우저 동기화 기능 비활성화
    # options.add_argument("--metrics-recording-only") # 크롬의 원격 분석 데이터 전송 비활성화

    # ChromeDriver 자동 설치 및 실행
    service = Service(ChromeDriverManager().install())

    # 크롤링 데이터 저장 변수
    reviews = []
    player_list=list_from_s3(context)

    # Selenium 드라이버 실행
    with webdriver.Chrome(service=service, options=options) as driver:
        logging.info("크롤링 시작")
        for player_id in player_list:
            try:
                driver.get(f"https://fconline.nexon.com/DataCenter/PlayerInfo?spid={player_id}&n1Strong=1")
                time.sleep(0.3)
                driver.implicitly_wait(1)
                comments = driver.find_elements(By.XPATH, '//*[@id="divCommentList"]/div[2]/ul/li')
                for comment in comments:
                    text = comment.find_element(By.XPATH, './div[1]/div[2]/span').text
                    review={"player_id": player_id, "comment": text}
                    reviews.append(review)
                logging.info(f"{player_id} 크롤링 완료")
                time.sleep(0.3)
                

            except Exception as e:
                print("{player_id} 크롤링 중 오류 발생: {e}")
                continue

    data = pd.DataFrame(reviews)

    # date = context['ds_nodash']
    file_path = f"/tmp/review_data.csv"
    data.to_csv(file_path, index=False, encoding='utf-8')

    logging.info(f"크롤링 데이터 저장 완료: {file_path}")
    
    return file_path  # 저장된 파일 경로 반환 (XCom에 저장)


def upload_to_s3(**context):
    """크롤링한 데이터를 S3에 업로드"""
    ti = context["task_instance"]
    file_path = ti.xcom_pull(task_ids="get_data")  # 파일 경로 가져오기

    if not file_path or not os.path.exists(file_path):
        raise FileNotFoundError(f"파일을 찾을 수 없음: {file_path}")

    # S3 버킷 및 경로 설정
    date = context['ds_nodash']
    date = datetime.strptime(date, "%Y%m%d").strftime("%Y-%m-%d")
    s3_key = f"raw_data/csv/review_data/{date}/review_data.csv"
    bucket_name = "de5-finalproj-team2"

    # S3 업로드 실행
    hook = S3Hook(aws_conn_id="aws_conn_id_2")
    hook.load_file(filename=file_path, key=s3_key, bucket_name=bucket_name, replace=True)

    logging.info(f"S3 업로드 완료: {s3_key}")

    os.remove(file_path)
    logging.info(f"로컬 파일 삭제 완료: {file_path}")


get_data_task = PythonOperator(
    task_id='get_data',
    python_callable=get_data,
    provide_context=True,
    dag=dag
)

upload_to_s3_task = PythonOperator(
    task_id='upload_to_s3',
    python_callable=upload_to_s3,
    provide_context=True,
    dag=dag
)




get_data_task >> upload_to_s3_task


