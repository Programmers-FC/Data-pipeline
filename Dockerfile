FROM apache/airflow:2.9.1-python3.9

# 추가 패키지 설치 (requirements.txt 기준)
COPY requirements.txt /requirements.txt
RUN pip install --no-cache-dir -r /requirements.txt