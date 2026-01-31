"""
News Embedding Stream Consumer

Kafka 뉴스 토픽을 구독하여 OpenAI 임베딩을 생성하고 Qdrant에 적재합니다.

특징:
- 결정적 ID(URL/본문 해시)로 멱등성 보장, 중복 시 Upsert
- OpenAI 호출 재시도 (Exponential Backoff)
- 실패 메시지는 DLQ 토픽으로 격리

사용 예시:
    # 로컬 (프로젝트 루트에서)
    python -u kafka_fin/consumer/news_embedding_stream.py

    # Docker
    python -u kafka_fin/consumer/news_embedding_stream.py
"""

import json
import hashlib
import logging
import os
import sys
import time
from datetime import datetime, timezone

from kafka import KafkaConsumer, KafkaProducer
from openai import OpenAI
from qdrant_client import QdrantClient
from qdrant_client.models import PointStruct, VectorParams, Distance

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger(__name__)


# =============================================================================
# 설정 클래스 (rag_pipeline.ingest_parquet_to_qdrant와 동일)
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


# =============================================================================
# 설정
# =============================================================================

KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
SOURCE_TOPIC = os.getenv("KAFKA_SOURCE_TOPIC", "news-raw")
DLQ_TOPIC = os.getenv("KAFKA_DLQ_TOPIC", "news-dlq-failed")
CONSUMER_GROUP_ID = os.getenv("KAFKA_EMBEDDING_GROUP_ID", "embedding_group")


def generate_deterministic_id(content_str: str) -> str:
    """내용이나 URL을 기반으로 고유 ID 생성 (멱등성 보장)."""
    return hashlib.md5(content_str.encode()).hexdigest()


def get_embedding_with_retry(text: str, openai_client, max_retries: int = 3) -> list:
    """OpenAI 임베딩 생성 (Exponential Backoff 재시도)."""
    for attempt in range(max_retries):
        try:
            response = openai_client.embeddings.create(
                input=text,
                model=Config.EMBEDDING_MODEL,
            )
            return response.data[0].embedding
        except Exception as e:
            wait_time = 2**attempt
            logger.warning(
                "OpenAI API 호출 실패. %s초 후 재시도... (%s)", wait_time, e
            )
            time.sleep(wait_time)
    raise RuntimeError("Max retries exceeded for embedding")


def build_payload(data: dict, url: str) -> dict:
    """Qdrant payload 구성 (url 필드 필수, rag_pipeline과 동일)."""
    payload = dict(data)
    payload["url"] = url or ""
    payload["ingested_at"] = datetime.now(timezone.utc).isoformat()
    return payload


def is_url_exists(client: QdrantClient, url: str) -> bool:
    """
    Qdrant에서 URL로 중복 체크 (임베딩 없이 빠르게 조회).
    이미 있으면 True → 임베딩 스킵으로 API 비용 절감.
    """
    if not url:
        return False
    try:
        existing_points, _ = client.scroll(
            collection_name=Config.COLLECTION_NAME,
            scroll_filter={
                "must": [{"key": "url", "match": {"value": url}}]
            },
            limit=1,
            with_vectors=False,
            with_payload=False,
        )
        return len(existing_points) > 0
    except Exception as e:
        logger.warning("Error checking URL existence: %s", e)
        return False  # 에러 시 새 기사로 간주하고 진행


# =============================================================================
# 메인 루프
# =============================================================================

def main() -> None:
    if not Config.OPENAI_API_KEY:
        logger.error("OPENAI_API_KEY 환경 변수를 설정하세요.")
        sys.exit(1)

    consumer = KafkaConsumer(
        SOURCE_TOPIC,
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        group_id=CONSUMER_GROUP_ID,
        value_deserializer=lambda m: json.loads(m.decode("utf-8")),
    )
    producer = KafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
    )
    qdrant = get_qdrant_client()
    ensure_url_index(qdrant)
    openai_client = get_openai_client()

    logger.info(
        "Streaming consumer started (topic=%s, collection=%s)",
        SOURCE_TOPIC,
        Config.COLLECTION_NAME,
    )

    for message in consumer:
        raw_value = message.value
        # value_deserializer가 이미 dict로 넘겨줄 수 있음
        data = raw_value if isinstance(raw_value, dict) else json.loads(raw_value)

        try:
            # 프로젝트 뉴스 스키마: link 또는 url
            article_url = data.get("link") or data.get("url") or ""
            article_text = data.get("content", "")

            if not article_text:
                continue

            # URL 이미 있으면 임베딩 스킵 (OpenAI API 비용 절감)
            if article_url and is_url_exists(qdrant, article_url):
                logger.info("Skipped (duplicate): %s", article_url)
                continue

            unique_id = generate_deterministic_id(
                article_url if article_url else article_text
            )
            vector = get_embedding_with_retry(article_text, openai_client)
            payload = build_payload(data, article_url)

            qdrant.upsert(
                collection_name=Config.COLLECTION_NAME,
                points=[
                    PointStruct(
                        id=unique_id,
                        vector=vector,
                        payload=payload,
                    )
                ],
            )
            logger.info("Saved: %s", unique_id)

        except Exception as e:
            logger.exception("Processing failed, sending to DLQ: %s", e)
            orig = message.value
            if not isinstance(orig, dict):
                try:
                    orig = json.loads(orig.decode("utf-8") if isinstance(orig, bytes) else orig)
                except Exception:
                    orig = {"_raw": str(orig)}
            error_data = {
                "original_data": orig,
                "error_msg": str(e),
                "failed_at": str(time.time()),
            }
            producer.send(DLQ_TOPIC, error_data)
            producer.flush()


if __name__ == "__main__":
    main()
