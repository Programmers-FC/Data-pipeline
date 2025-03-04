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


#def get_RedShift_connection(autocommit=True):
#    hook=SnowflakeHook(snowflake_conn_id='snowflake_dev_db')
#    conn = hook.get_conn()
#    conn.autocommit = autocommit
#    return conn.cursor()



def get_data(**context):
    options = Options()#크롤링을 위한 header설정
    options.add_argument("user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/133.0.0.0 Safari/537.36")
    options.add_argument("--headless")# 브라우저 창을 띄우지 않음
    options.add_argument("--disable-gpu")
    options.add_argument("--no-sandbox")
    reviews=[]
    for player_id in player_list:
        try:
            driver = webdriver.Chrome(service=Service(ChromeDriverManager().install()), options=options)
            review=[]
            driver.get(f"https://fifaonline4.inven.co.kr/dataninfo/player/?code={player_id}")
            driver.implicitly_wait(1)
            comments= driver.find_elements(By.CLASS_NAME,"comment")
            review={"player_id":player_id,
            "comment":[comment.text for comment in comments]
            
            }
            reviews.append(review)
            print(f"{player_id} 크롤링 완료")
            
        except Exception as e:
            print(f"{player_id}: {e}")
            continue
    data=pd.DataFrame(reviews)
    data=data.explode("comment").reset_index(drop=True)#감정분석 모델 input에 따라 바뀔 수 있음
    csv_data=data.to_csv(index=False, encoding='utf-8')
    return csv_data
    



def upload_to_s3(**context):
    ti=context["task_instance"]
    csv_data=ti.xcom_pull(task_ids="get_data")
    date = context['ds_nodash']
    hook = S3Hook(aws_conn_id='AWS_CONN_id')#s3 access key
    try:
        hook.load_string(string_data=csv_data, key=f"raw_data/player_review/players_review_data_{date}.csv", bucket_name="de5-finalproj-team2",replace=True)
        print("s3업로드 완료")
        
    except Exception as error:
        print(error)
        raise
        




dag = DAG(
    dag_id = 'players_review_data',
    start_date = datetime(2025, 3, 7), # 날짜가 미래인 경우 실행이 안됨
    schedule = '0 8 * * *',  # 적당히 조절
    max_active_runs = 1,
    catchup = False,
    tags=['API'],
    default_args = {
        'retries': 2,
        'retry_delay': timedelta(minutes=1),
        # 'on_failure_callback': slack.on_failure_callback,
    }
)



get_data = PythonOperator(
    task_id = 'get_data',
    python_callable = get_data,
    params = {
    
    },
    dag = dag)
    

upload_to_s3 = PythonOperator(
    task_id = 'upload_to_s3',
    python_callable = upload_to_s3,
    params = {
    },
    dag = dag)
    
    
    

get_data >> upload_to_s3


