from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
import csv
import time
import re
import os
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.action_chains import ActionChains
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import StaleElementReferenceException, NoSuchElementException
from airflow.models import Variable
from datetime import datetime, timedelta

# Airflow Variables에서 S3 정보 불러오기
S3_BUCKET_NAME = Variable.get("S3_BUCKET_NAME")
S3_FOLDER_PATH = Variable.get("S3_FOLDER_PATH")
AWS_CONN_ID = "aws_conn_id"  # Airflow Connection ID

# 파일명에서 시간 제거 (YYYYMMDD 형식)
DATE_STR = datetime.now().strftime('%Y%m%d')
LOCAL_FILE_PATH = f"/tmp/crawl_result_{DATE_STR}.csv"

# ChromeDriver 경로 지정
CHROMEDRIVER_PATH = "/usr/local/bin/chromedriver"

# DAG 기본 설정
default_args = {
    "owner": "airflow",
    "start_date": datetime(2024, 2, 25),
    "retries": 1,
    "retry_delay": timedelta(seconds=10),
}

dag = DAG(
    "fc_online_ranking_dag",
    default_args=default_args,
    schedule_interval="0 0 * * *",  # 매일 00:00 실행
    catchup=False
)

def crawl_data():
    """FC ONLINE 데이터 센터에서 랭킹 정보를 크롤링하고 CSV로 저장"""
    options = webdriver.ChromeOptions()
    options.add_argument("--headless")
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-dev-shm-usage")

    # 직접 설치한 chromedriver 사용
    service = Service(CHROMEDRIVER_PATH)
    driver = webdriver.Chrome(service=service, options=options)
    wait = WebDriverWait(driver, 10)

    url = "https://fconline.nexon.com/datacenter/rank"
    driver.get(url)

    time.sleep(3)  # 페이지 로딩 대기

    # 팝업 닫기
    try:
        close_button = WebDriverWait(driver, 3).until(
            EC.element_to_be_clickable((By.XPATH, '//*[@id="wrapper"]/div[1]/a'))
        )
        close_button.click()
        print("팝업 닫음")
    except:
        print("팝업 없음")

    def safe_find_text(element, by, selector):
        try:
            return element.find_element(by, selector).text
        except NoSuchElementException:
            return ""

    def crawl_page():
        """현재 페이지에서 유저 정보를 크롤링"""
        tbody_xpath = '//div[@class="tbody"]'
        page_data = []

        rows_count = len(wait.until(EC.presence_of_all_elements_located((By.CSS_SELECTOR, 'div.tbody div.tr'))))

        for idx in range(rows_count):
            retries = 0
            while retries < 3:
                try:
                    tbody = wait.until(EC.presence_of_element_located((By.XPATH, tbody_xpath)))
                    rows = tbody.find_elements(By.CLASS_NAME, 'tr')
                    row = rows[idx]

                    rank_no = safe_find_text(row, By.CLASS_NAME, 'rank_no')
                    coach_name = safe_find_text(row, By.CSS_SELECTOR, 'span.name.profile_pointer')
                    coach_level = safe_find_text(row, By.CSS_SELECTOR, 'span.lv .txt')
                    team_value = safe_find_text(row, By.CLASS_NAME, 'price')
                    win_point = safe_find_text(row, By.CLASS_NAME, 'rank_r_win_point')
                    win_rate = safe_find_text(row, By.CSS_SELECTOR, 'span.rank_before span.top')
                    record = safe_find_text(row, By.CSS_SELECTOR, 'span.rank_before span.bottom')
                    team_names_elements = row.find_elements(By.CSS_SELECTOR, 'span.team_color span.inner')
                    team_names = "|".join([t.text for t in team_names_elements]) if team_names_elements else ""
                    formation = safe_find_text(row, By.CLASS_NAME, 'formation')

                    try:
                        unique_id = row.find_element(By.CSS_SELECTOR, 'span.name.profile_pointer').get_attribute('data-sn')
                    except NoSuchElementException:
                        unique_id = ""

                    try:
                        img_url = row.find_element(By.CSS_SELECTOR, 'span.ico_rank img').get_attribute('src')
                        rank_match = re.search(r'ico_rank(\d+)\.png', img_url)
                        rank_num = rank_match.group(1) if rank_match else ''
                    except NoSuchElementException:
                        rank_num = ''

                    if rank_no:
                        data = [
                            rank_no, coach_name, unique_id, rank_num, coach_level,
                            team_value, win_point, win_rate, record, team_names, formation
                        ]
                        page_data.append(data)
                    break

                except StaleElementReferenceException:
                    retries += 1
                    time.sleep(1)
                    continue

        return page_data

    all_data = []
    current_page = 1
    end_page = 500

    while current_page <= end_page:
        print(f"{current_page} 페이지 크롤링 중...")
        page_data = crawl_page()
        all_data.extend(page_data)

        try:
            if current_page % 10 == 0 and current_page != end_page:
                next_btn_xpath = '//a[@class="btn_next_list ajaxNav"]'
                next_btn = wait.until(EC.element_to_be_clickable((By.XPATH, next_btn_xpath)))
                ActionChains(driver).move_to_element(next_btn).click().perform()
                time.sleep(1)

            elif current_page % 10 == 1 and current_page != 1:
                pass

            elif current_page != end_page:
                page_btn_num = current_page + 1
                page_xpath = f'//a[@onclick="goSearchDetail({page_btn_num},false);"]'
                page_btn = wait.until(EC.element_to_be_clickable((By.XPATH, page_xpath)))
                ActionChains(driver).move_to_element(page_btn).click().perform()
                time.sleep(1)

        except Exception as e:
            print(f"{current_page} 페이지 이동 중 에러: {e}, 재시도 중...")
            time.sleep(1)
            continue

        current_page += 1

    header = ['순위', '감독명', '고유번호', '랭크번호', '레벨', '팀 가치', '승점', '승률', '전적', '팀 이름', '포메이션']

    with open(LOCAL_FILE_PATH, 'w', encoding='utf-8-sig', newline='') as file:
        writer = csv.writer(file)
        writer.writerow(header)
        writer.writerows(all_data)

    print(f"📄 크롤링 완료 - {LOCAL_FILE_PATH}")

    # Airflow Variable에 파일 경로 저장
    Variable.set("crawl_result_path", LOCAL_FILE_PATH)

    driver.quit()

def upload_to_s3():
    """크롤링한 파일을 S3에 업로드"""
    file_path = Variable.get("crawl_result_path", default_var="")

    if not file_path or not os.path.exists(file_path):
        raise FileNotFoundError(f"❌ 파일이 존재하지 않음: {file_path}")

    s3_hook = S3Hook(aws_conn_id=AWS_CONN_ID)
    date_folder = datetime.now().strftime('%Y-%m-%d')
    s3_key = f"{S3_FOLDER_PATH}/{date_folder}" + os.path.basename(file_path)

    s3_hook.load_file(
        filename=file_path,
        key=s3_key,
        bucket_name=S3_BUCKET_NAME,
        replace=True
    )
    print(f"✅ S3 업로드 완료 - s3://{S3_BUCKET_NAME}/{s3_key}")

crawl_task = PythonOperator(task_id="crawl_fc_online_data", python_callable=crawl_data, dag=dag)
upload_task = PythonOperator(task_id="upload_to_s3", python_callable=upload_to_s3, dag=dag)

crawl_task >> upload_task