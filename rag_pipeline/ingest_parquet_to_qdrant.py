"""
MinIO Parquet → Qdrant 임베딩 적재 모듈

이 모듈은 Airflow DAG에서 호출되어 특정 시간대의 Parquet 파일을 읽고,
OpenAI 임베딩을 생성하여 Qdrant에 적재합니다.

특징:
- URL 기반 중복 체크: 이미 적재된 기사는 스킵 (OpenAI API 비용 절약)
- Payload Index: url 필드에 인덱스를 생성하여 중복 체크 성능 최적화

사용 예시:
    # 전체 데이터 적재 (로컬 테스트용)
    python ingest_parquet_to_qdrant.py

    # Airflow에서 증분 적재 (특정 시간대만)
    from rag_pipeline.ingest_parquet_to_qdrant import run_incremental_ingest
    run_incremental_ingest(year=2026, month=1, day=27, hour=15)
"""

import os
import uuid
import logging
from typing import Optional
from datetime import datetime, timezone

import duckdb
import pandas as pd
from langchain_text_splitters import RecursiveCharacterTextSplitter
from openai import OpenAI
from qdrant_client import QdrantClient
from qdrant_client.models import PointStruct, VectorParams, Distance

# 로깅 설정
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


# =============================================================================
# 설정 클래스
# =============================================================================

class Config:
    """환경변수 기반 설정"""
    
    # MinIO (S3 호환)
    MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "localhost:9000")
    MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY", "admin")
    MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY", "password123")
    MINIO_BUCKET = os.getenv("MINIO_BUCKET", "news-lake")
    
    # Qdrant
    QDRANT_HOST = os.getenv("QDRANT_HOST", "localhost")
    QDRANT_PORT = int(os.getenv("QDRANT_PORT", "6333"))
    COLLECTION_NAME = "crypto_news"
    
    # OpenAI
    OPENAI_API_KEY = os.getenv("OPENAI_API_KEY")
    EMBEDDING_MODEL = "text-embedding-3-small"
    EMBEDDING_DIMENSION = 1536
    
    # 청킹 설정
    CHUNK_SIZE = 700
    CHUNK_OVERLAP = 100
    
    # 배치 설정
    BATCH_UPLOAD_SIZE = 100


# =============================================================================
# 클라이언트 초기화 함수
# =============================================================================

def get_qdrant_client() -> QdrantClient:
    """Qdrant 클라이언트 생성 및 컬렉션 초기화 (URL 인덱스 포함)"""
    client = QdrantClient(host=Config.QDRANT_HOST, port=Config.QDRANT_PORT)
    
    # 컬렉션이 없으면 생성
    if not client.collection_exists(Config.COLLECTION_NAME):
        client.create_collection(
            collection_name=Config.COLLECTION_NAME,
            vectors_config=VectorParams(
                size=Config.EMBEDDING_DIMENSION, 
                distance=Distance.COSINE
            ),
        )
        logger.info(f"Created Qdrant collection: {Config.COLLECTION_NAME}")
        
        # URL 필드에 Payload Index 생성 (중복 체크 성능 최적화)
        client.create_payload_index(
            collection_name=Config.COLLECTION_NAME,
            field_name="url",
            field_schema="keyword",  # 문자열 완전 일치 검색용
        )
        logger.info(f"Created payload index on 'url' field")
    
    return client


def ensure_url_index(client: QdrantClient) -> None:
    """
    기존 컬렉션에 URL 인덱스가 없으면 생성
    (이미 컬렉션이 있는 경우를 위한 함수)
    """
    try:
        collection_info = client.get_collection(Config.COLLECTION_NAME)
        # payload_schema에 url 인덱스가 있는지 확인
        if collection_info.payload_schema and "url" not in collection_info.payload_schema:
            client.create_payload_index(
                collection_name=Config.COLLECTION_NAME,
                field_name="url",
                field_schema="keyword",
            )
            logger.info("Created payload index on 'url' field for existing collection")
    except Exception as e:
        logger.warning(f"Could not check/create url index: {e}")


def get_openai_client() -> OpenAI:
    """OpenAI 클라이언트 생성"""
    if not Config.OPENAI_API_KEY:
        raise ValueError("OPENAI_API_KEY 환경 변수를 설정하세요.")
    return OpenAI(api_key=Config.OPENAI_API_KEY)


def get_duckdb_connection():
    """DuckDB 연결 생성 및 MinIO(S3) 설정"""
    con = duckdb.connect()
    con.execute("INSTALL httpfs; LOAD httpfs;")
    con.execute(f"""
        SET s3_endpoint='{Config.MINIO_ENDPOINT}';
        SET s3_access_key_id='{Config.MINIO_ACCESS_KEY}';
        SET s3_secret_access_key='{Config.MINIO_SECRET_KEY}';
        SET s3_use_ssl=false;
        SET s3_url_style='path';
    """)
    return con


# =============================================================================
# 중복 체크 함수
# =============================================================================

def is_url_exists(client: QdrantClient, url: str) -> bool:
    """
    Qdrant에서 URL로 중복 체크 (임베딩 없이 빠르게 조회)
    
    Args:
        client: Qdrant 클라이언트
        url: 확인할 기사 URL
    
    Returns:
        True면 이미 존재, False면 새 기사
    """
    if not url:
        return False
    
    try:
        # 딕셔너리 형태로 필터링 (최신 qdrant-client 지원)
        existing_points, _ = client.scroll(
            collection_name=Config.COLLECTION_NAME,
            scroll_filter={
                "must": [
                    {"key": "url", "match": {"value": url}}
                ]
            },
            limit=1,
            with_vectors=False,  # 중복 확인용이니 벡터는 필요 없음
            with_payload=False,  # payload도 필요 없음
        )
        return len(existing_points) > 0
    except Exception as e:
        logger.warning(f"Error checking URL existence: {e}")
        return False  # 에러 시 새 기사로 간주하고 진행


# =============================================================================
# 핵심 함수들
# =============================================================================

def build_minio_path(
    year: Optional[int] = None,
    month: Optional[int] = None,
    day: Optional[int] = None,
    hour: Optional[int] = None,
) -> str:
    """
    MinIO Parquet 경로 생성
    
    Args:
        year, month, day, hour: 특정 시간대 지정 (None이면 와일드카드 사용)
    
    Returns:
        S3 경로 문자열 (예: s3://news-lake/refined/year=2026/month=01/day=27/hour=15/*.parquet)
    """
    year_part = f"year={year:04d}" if year else "year=*"
    month_part = f"month={month:02d}" if month else "month=*"
    day_part = f"day={day:02d}" if day else "day=*"
    hour_part = f"hour={hour:02d}" if hour else "hour=*"
    
    return f"s3://{Config.MINIO_BUCKET}/refined/{year_part}/{month_part}/{day_part}/{hour_part}/*.parquet"


def load_parquet_from_minio(
    con,
    year: Optional[int] = None,
    month: Optional[int] = None,
    day: Optional[int] = None,
    hour: Optional[int] = None,
) -> pd.DataFrame:
    """MinIO에서 Parquet 파일 로드"""
    minio_path = build_minio_path(year, month, day, hour)
    logger.info(f"Loading data from: {minio_path}")
    
    try:
        df = con.execute(f"""
            SELECT
                title,
                content,
                link,
                published_at,
                source
            FROM read_parquet('{minio_path}')
        """).df()
        logger.info(f"Loaded {len(df)} articles")
        return df
    except Exception as e:
        logger.warning(f"No data found at {minio_path}: {e}")
        return pd.DataFrame()


def embed_and_upload(
    df: pd.DataFrame,
    qdrant_client: QdrantClient,
    openai_client: OpenAI,
) -> dict:
    """
    DataFrame의 기사들을 임베딩하고 Qdrant에 적재 (URL 기반 중복 체크 포함)
    
    Returns:
        처리 결과 딕셔너리 (적재 청크 수, 스킵된 기사 수 등)
    """
    if df.empty:
        logger.info("No data to process")
        return {"chunks_uploaded": 0, "articles_skipped": 0, "articles_processed": 0}
    
    text_splitter = RecursiveCharacterTextSplitter(
        chunk_size=Config.CHUNK_SIZE,
        chunk_overlap=Config.CHUNK_OVERLAP,
        separators=["\n\n", "\n", ".", " ", ""],
    )
    
    points = []
    total_uploaded = 0
    articles_skipped = 0
    articles_processed = 0
    
    for index, row in df.iterrows():
        content = row.get("content")
        url = row.get("link")
        
        if not content:
            continue
        
        # =====================================================================
        # URL 기반 중복 체크 (임베딩 전에 확인!)
        # =====================================================================
        if is_url_exists(qdrant_client, url):
            logger.info(f"Skipped (duplicate): {url}")
            articles_skipped += 1
            continue
        
        # 새 기사 처리
        articles_processed += 1
        
        # 텍스트 청킹
        chunks = text_splitter.split_text(str(content))
        
        for i, chunk_text in enumerate(chunks):
            try:
                # OpenAI 임베딩 생성 (새 기사만!)
                response = openai_client.embeddings.create(
                    input=chunk_text,
                    model=Config.EMBEDDING_MODEL,
                )
                vector = response.data[0].embedding
                
                # published_at 처리
                published_at = row.get("published_at")
                if isinstance(published_at, pd.Timestamp):
                    published_at = published_at.isoformat()
                elif published_at is not None:
                    published_at = str(published_at)
                
                # Qdrant Point 생성
                points.append(
                    PointStruct(
                        id=str(uuid.uuid4()),
                        vector=vector,
                        payload={
                            "source": row.get("source"),
                            "title": row.get("title"),
                            "content": chunk_text,
                            "full_content": str(content),
                            "url": url,  # 중복 체크에 사용되는 필드
                            "published_at": published_at,
                            "chunk_index": i,
                            "ingested_at": datetime.now(timezone.utc).isoformat(),
                        },
                    )
                )
            except Exception as e:
                logger.error(f"Error embedding chunk (row={index}, chunk={i}): {e}")
                raise  # Airflow 재시도를 위해 예외 전파
        
        # 배치 업로드
        if len(points) >= Config.BATCH_UPLOAD_SIZE:
            qdrant_client.upsert(collection_name=Config.COLLECTION_NAME, points=points)
            total_uploaded += len(points)
            logger.info(f"Uploaded batch: {len(points)} chunks (total: {total_uploaded})")
            points = []
    
    # 남은 데이터 업로드
    if points:
        qdrant_client.upsert(collection_name=Config.COLLECTION_NAME, points=points)
        total_uploaded += len(points)
        logger.info(f"Final batch: {len(points)} chunks (total: {total_uploaded})")
    
    return {
        "chunks_uploaded": total_uploaded,
        "articles_skipped": articles_skipped,
        "articles_processed": articles_processed,
    }


# =============================================================================
# Airflow에서 호출할 메인 함수
# =============================================================================

def run_incremental_ingest(
    year: int,
    month: int,
    day: int,
    hour: int,
) -> dict:
    """
    특정 시간대의 데이터만 증분 적재 (Airflow DAG에서 호출)
    
    - URL 기반 중복 체크로 이미 적재된 기사는 스킵
    - OpenAI API 비용 절약
    
    Args:
        year, month, day, hour: 처리할 시간대
    
    Returns:
        처리 결과 딕셔너리 (XCom으로 전달 가능)
    """
    logger.info(f"Starting incremental ingest for {year}/{month:02d}/{day:02d}/{hour:02d}")
    
    # 클라이언트 초기화
    con = get_duckdb_connection()
    qdrant_client = get_qdrant_client()
    openai_client = get_openai_client()
    
    # 기존 컬렉션에 URL 인덱스 확인/생성
    ensure_url_index(qdrant_client)
    
    # 데이터 로드
    df = load_parquet_from_minio(con, year, month, day, hour)
    
    if df.empty:
        logger.info("No new data to process")
        return {
            "status": "no_data",
            "articles_loaded": 0,
            "articles_processed": 0,
            "articles_skipped": 0,
            "chunks_uploaded": 0,
            "target_path": build_minio_path(year, month, day, hour),
        }
    
    # 임베딩 및 적재 (중복 체크 포함)
    result = embed_and_upload(df, qdrant_client, openai_client)
    
    final_result = {
        "status": "success",
        "articles_loaded": len(df),
        "articles_processed": result["articles_processed"],
        "articles_skipped": result["articles_skipped"],
        "chunks_uploaded": result["chunks_uploaded"],
        "target_path": build_minio_path(year, month, day, hour),
    }
    logger.info(f"Completed: {final_result}")
    return final_result


def run_full_ingest() -> dict:
    """전체 데이터 적재 (초기 로드 또는 테스트용)"""
    logger.info("Starting full ingest (all data)")
    
    con = get_duckdb_connection()
    qdrant_client = get_qdrant_client()
    openai_client = get_openai_client()
    
    # 기존 컬렉션에 URL 인덱스 확인/생성
    ensure_url_index(qdrant_client)
    
    df = load_parquet_from_minio(con)  # 전체 데이터
    
    if df.empty:
        return {
            "status": "no_data",
            "articles_loaded": 0,
            "articles_processed": 0,
            "articles_skipped": 0,
            "chunks_uploaded": 0,
        }
    
    result = embed_and_upload(df, qdrant_client, openai_client)
    
    return {
        "status": "success",
        "articles_loaded": len(df),
        "articles_processed": result["articles_processed"],
        "articles_skipped": result["articles_skipped"],
        "chunks_uploaded": result["chunks_uploaded"],
    }


# =============================================================================
# 직접 실행 시 (로컬 테스트)
# =============================================================================

if __name__ == "__main__":
    from dotenv import load_dotenv
    load_dotenv()
    
    import sys
    
    if len(sys.argv) == 5:
        # 특정 시간대 증분 적재: python ingest_parquet_to_qdrant.py 2026 1 27 15
        year, month, day, hour = map(int, sys.argv[1:5])
        result = run_incremental_ingest(year, month, day, hour)
    else:
        # 전체 적재
        result = run_full_ingest()
    
    print(f"\n{'='*50}")
    print("Job Finished!")
    print(f"Result: {result}")
