# dags/news_crawling_dag.py

from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta, timezone
import sys
import os

# 크롤러 모듈 경로 추가 (Airflow 컨테이너 내부 경로)
# /opt/airflow에 kafka_fin 디렉토리가 마운트되어 있음
sys.path.insert(0, '/opt/airflow')

# 크롤러 함수 임포트
from kafka_fin.producer.coindesk_news_crawler import fetch_coindesk_news_with_selenium
from kafka_fin.producer.coinness_breaking_crawler import fetch_coinness_breaking_news
from kafka_fin.producer.coinness_newsroom_crawler import fetch_coinness_newsroom

# 크롤링 기준 시간 설정 (분 단위)
# 현재 실행 시점에서 이 값만큼 뺀 시간 이후의 뉴스만 크롤링
CRAWLING_TIME_WINDOW_MINUTES = 720

default_args = {
    'owner': 'admin',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    'news_crawlers_dag',
    default_args=default_args,
    description='5분마다 뉴스 크롤링 및 Kafka 전송 (최근 5분간 뉴스만)',
    schedule='*/5 * * * *',  # 5분마다 실행 (cron 표현식)
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=['news', 'kafka', 'crawling'],
) as dag:

    def run_coindesk(**context):
        """Coindesk 뉴스 크롤링"""
        # 현재 실행 시점에서 설정된 시간만큼 뺀 것을 기준 시점으로 사용
        current_execution = context['data_interval_end']
        since = current_execution - timedelta(minutes=CRAWLING_TIME_WINDOW_MINUTES)
        
        # timezone-aware datetime으로 변환 (UTC)
        if since.tzinfo is None:
            since = since.replace(tzinfo=timezone.utc)
        else:
            since = since.astimezone(timezone.utc)
        
        print(f"[Coindesk] 현재 실행 시점: {current_execution}, 크롤링 기준 시간: {since} (최근 {CRAWLING_TIME_WINDOW_MINUTES}분간)")
        fetch_coindesk_news_with_selenium(
            topic_name='coindesk-news',
            num_articles=20,  # 충분히 많은 기사를 가져와서 필터링
            bootstrap_servers='kafka:29092',  # Docker 네트워크 내부에서는 컨테이너 이름 사용
            since_datetime=since,
            selenium_url='http://selenium-chrome:4444/wd/hub'  # Docker 네트워크 내부 주소
        )

    def run_coinness_breaking(**context):
        """Coinness Breaking News 크롤링"""
        # 현재 실행 시점에서 설정된 시간만큼 뺀 것을 기준 시점으로 사용
        current_execution = context['data_interval_end']
        since = current_execution - timedelta(minutes=CRAWLING_TIME_WINDOW_MINUTES)
        
        # timezone-aware datetime으로 변환 (UTC)
        if since.tzinfo is None:
            since = since.replace(tzinfo=timezone.utc)
        else:
            since = since.astimezone(timezone.utc)
        
        print(f"[Coinness Breaking] 현재 실행 시점: {current_execution}, 크롤링 기준 시간: {since} (최근 {CRAWLING_TIME_WINDOW_MINUTES}분간)")
        fetch_coinness_breaking_news(
            topic_name='coinness-breaking',
            bootstrap_servers='kafka:29092',
            since_datetime=since,
            selenium_url='http://selenium-chrome:4444/wd/hub'  # Docker 네트워크 내부 주소
        )

    def run_coinness_newsroom(**context):
        """Coinness Newsroom 크롤링"""
        # 현재 실행 시점에서 설정된 시간만큼 뺀 것을 기준 시점으로 사용
        current_execution = context['data_interval_end']
        since = current_execution - timedelta(minutes=CRAWLING_TIME_WINDOW_MINUTES)
        
        # timezone-aware datetime으로 변환 (UTC)
        if since.tzinfo is None:
            since = since.replace(tzinfo=timezone.utc)
        else:
            since = since.astimezone(timezone.utc)
        
        print(f"[Coinness Newsroom] 현재 실행 시점: {current_execution}, 크롤링 기준 시간: {since} (최근 {CRAWLING_TIME_WINDOW_MINUTES}분간)")
        fetch_coinness_newsroom(
            topic_name='coinness-newsroom',
            bootstrap_servers='kafka:29092',
            num_articles=None,  # 모든 기사를 가져와서 필터링
            since_datetime=since,
            selenium_url='http://selenium-chrome:4444/wd/hub'  # Docker 네트워크 내부 주소
        )

    # Task 정의
    task_coindesk = PythonOperator(
        task_id='crawl_coindesk',
        python_callable=run_coindesk,
    )

    task_coinness_breaking = PythonOperator(
        task_id='crawl_coinness_breaking',
        python_callable=run_coinness_breaking,
    )

    task_coinness_newsroom = PythonOperator(
        task_id='crawl_coinness_newsroom',
        python_callable=run_coinness_newsroom,
    )

    # 병렬 실행 (의존성 없음)
    [task_coindesk, task_coinness_breaking, task_coinness_newsroom]

