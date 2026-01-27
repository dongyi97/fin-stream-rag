"""
MinIO → Qdrant 임베딩 파이프라인 DAG

이 DAG는 10분마다 실행되어:
1. MinIO에 새로운 Parquet 파일이 있는지 확인
2. 파일이 있으면 임베딩 생성 후 Qdrant에 적재

특징:
- 증분 적재 (Incremental Load): data_interval_start를 사용해 특정 시간대만 처리
- 재시도 로직: 실패 시 5분 간격으로 3번 재시도
- 의존성 관리: MinIO에 파일이 없으면 스킵

Airflow 3.x / Python 3.12 호환
"""

import os
import sys
from datetime import datetime, timedelta, timezone

from airflow import DAG
from airflow.operators.python import PythonOperator, ShortCircuitOperator

# rag_pipeline 모듈 경로 추가 (Airflow 컨테이너 내부 경로)
sys.path.insert(0, "/opt/airflow/rag_pipeline")


# =============================================================================
# DAG 기본 설정
# =============================================================================

default_args = {
    "owner": "data-team",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    # 재시도 설정: 5분 간격으로 3번
    "retries": 3,
    "retry_delay": timedelta(minutes=5),
    # 실행 타임아웃: 30분
    "execution_timeout": timedelta(minutes=30),
}


# =============================================================================
# Helper 함수들
# =============================================================================

def get_time_partition(**context) -> dict:
    """
    Airflow의 data_interval_start를 파싱하여 시간 파티션 정보 반환
    
    Airflow가 자동으로 넘겨주는 값:
    - 10분마다 실행하면 data_interval_start는 이전 10분 구간의 시작점
    - 예: 15:10에 실행되면 data_interval_start는 15:00
    
    우리 데이터는 시간(hour) 단위로 파티션되어 있으므로, hour를 추출
    """
    # Airflow 3.x에서는 data_interval_start 사용
    data_interval_start = context.get("data_interval_start")
    
    if data_interval_start is None:
        # Fallback: 현재 시간 사용 (UTC)
        data_interval_start = datetime.now(timezone.utc)
    
    partition_info = {
        "year": data_interval_start.year,
        "month": data_interval_start.month,
        "day": data_interval_start.day,
        "hour": data_interval_start.hour,
    }
    
    print(f"[get_time_partition] data_interval_start: {data_interval_start}")
    print(f"[get_time_partition] partition_info: {partition_info}")
    
    # XCom으로 다음 태스크에 전달
    context["ti"].xcom_push(key="partition_info", value=partition_info)
    return partition_info


def check_files_exist(**context) -> bool:
    """
    MinIO에 파일이 있는지 직접 확인
    
    boto3를 사용해 MinIO의 특정 경로에 파일이 있는지 확인
    """
    import boto3
    from botocore.client import Config as BotoConfig
    
    ti = context["ti"]
    partition_info = ti.xcom_pull(task_ids="get_time_partition", key="partition_info")
    
    if not partition_info:
        print("[check_files_exist] No partition info, skipping")
        return False
    
    year = partition_info["year"]
    month = partition_info["month"]
    day = partition_info["day"]
    hour = partition_info["hour"]
    
    # MinIO 설정 (환경변수에서 가져오기)
    minio_endpoint = os.getenv("MINIO_ENDPOINT", "minio:9000")
    # http:// 접두사가 이미 있는지 확인
    if not minio_endpoint.startswith("http"):
        endpoint_url = f"http://{minio_endpoint}"
    else:
        endpoint_url = minio_endpoint
    
    access_key = os.getenv("MINIO_ACCESS_KEY", "admin")
    secret_key = os.getenv("MINIO_SECRET_KEY", "password123")
    bucket_name = os.getenv("MINIO_BUCKET", "news-lake")
    
    # boto3 클라이언트 생성
    s3_client = boto3.client(
        "s3",
        endpoint_url=endpoint_url,
        aws_access_key_id=access_key,
        aws_secret_access_key=secret_key,
        config=BotoConfig(signature_version="s3v4"),
    )
    
    # 경로 확인
    prefix = f"refined/year={year:04d}/month={month:02d}/day={day:02d}/hour={hour:02d}/"
    print(f"[check_files_exist] Checking bucket={bucket_name}, prefix={prefix}")
    
    try:
        response = s3_client.list_objects_v2(
            Bucket=bucket_name,
            Prefix=prefix,
            MaxKeys=1,
        )
        
        has_files = "Contents" in response and len(response["Contents"]) > 0
        
        if has_files:
            print(f"[check_files_exist] Found files at {prefix}")
        else:
            print(f"[check_files_exist] No files at {prefix}")
        
        return has_files
        
    except Exception as e:
        print(f"[check_files_exist] Error checking files: {e}")
        # 파일 확인 실패 시에도 일단 진행 (임베딩 단계에서 처리)
        return True


def run_embedding_task(**context) -> dict:
    """
    임베딩 및 Qdrant 적재 실행
    
    rag_pipeline 모듈의 run_incremental_ingest 함수 호출
    """
    # rag_pipeline 모듈 import
    from ingest_parquet_to_qdrant import run_incremental_ingest
    
    ti = context["ti"]
    partition_info = ti.xcom_pull(task_ids="get_time_partition", key="partition_info")
    
    if not partition_info:
        print("[run_embedding_task] No partition info found")
        return {"status": "skipped", "reason": "no_partition_info"}
    
    year = partition_info["year"]
    month = partition_info["month"]
    day = partition_info["day"]
    hour = partition_info["hour"]
    
    print(f"[run_embedding_task] Processing {year}/{month:02d}/{day:02d}/{hour:02d}")
    
    # 임베딩 실행
    result = run_incremental_ingest(year, month, day, hour)
    
    print(f"[run_embedding_task] Result: {result}")
    
    # XCom으로 결과 저장
    ti.xcom_push(key="embedding_result", value=result)
    
    return result


def log_completion(**context) -> None:
    """DAG 완료 로깅"""
    ti = context["ti"]
    partition_info = ti.xcom_pull(task_ids="get_time_partition", key="partition_info")
    embedding_result = ti.xcom_pull(task_ids="run_embedding", key="embedding_result")
    
    print("=" * 60)
    print("DAG Execution Completed")
    print(f"  Partition: {partition_info}")
    print(f"  Result: {embedding_result}")
    print("=" * 60)


# =============================================================================
# DAG 정의 (Airflow 3.x 권장 방식: context manager)
# =============================================================================

with DAG(
    dag_id="minio_to_qdrant_embedding",
    default_args=default_args,
    description="MinIO Parquet → OpenAI 임베딩 → Qdrant 적재 파이프라인",
    # 10분마다 실행
    schedule=timedelta(minutes=10),
    start_date=datetime(2024, 1, 1),  # 고정 날짜 사용
    catchup=False,  # 과거 미실행 분 실행하지 않음
    tags=["rag", "embedding", "qdrant", "minio"],
    # 동시 실행 방지 (리소스 보호)
    max_active_runs=1,
) as dag:
    
    # Task 1: 시간 파티션 정보 추출
    task_get_partition = PythonOperator(
        task_id="get_time_partition",
        python_callable=get_time_partition,
    )
    
    # Task 2: MinIO에 파일 존재 여부 확인 (ShortCircuit으로 없으면 스킵)
    task_check_files = ShortCircuitOperator(
        task_id="check_files_exist",
        python_callable=check_files_exist,
    )
    
    # Task 3: 임베딩 및 Qdrant 적재
    task_run_embedding = PythonOperator(
        task_id="run_embedding",
        python_callable=run_embedding_task,
    )
    
    # Task 4: 완료 로깅
    task_log_completion = PythonOperator(
        task_id="log_completion",
        python_callable=log_completion,
    )
    
    # Task 의존성 설정
    # 실행 순서: 파티션 추출 → 파일 확인 → 임베딩 → 완료 로깅
    task_get_partition >> task_check_files >> task_run_embedding >> task_log_completion
