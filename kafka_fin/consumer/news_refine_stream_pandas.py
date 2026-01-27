import json
import os
import re
import sys
from datetime import datetime, timezone
from io import BytesIO
from typing import List, Dict, Any

import pandas as pd
from kafka import KafkaConsumer
import boto3


# =========================
# 설정
# =========================

# Kafka 설정
# - Docker 내부: kafka:29092 (docker-compose에서 환경변수로 설정됨)
# - Docker 외부 (로컬 테스트): localhost:9092
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
KAFKA_TOPICS = [
    "coindesk-news",
    "coinness-breaking",
    "coinness-newsroom",
]

# MinIO(S3 호환) 설정
# - Docker 내부: http://minio:9000 (docker-compose에서 환경변수로 설정됨)
# - Docker 외부 (로컬 테스트): http://localhost:9000
MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "http://localhost:9000")
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY", "admin")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY", "password123")
MINIO_BUCKET = os.getenv("MINIO_BUCKET", "news-lake")

# S3 경로 (refined 레이어)
BASE_PREFIX = "refined"


def _create_minio_client():
    """MinIO(boto3) 클라이언트 생성."""
    session = boto3.session.Session()
    s3 = session.client(
        "s3",
        endpoint_url=MINIO_ENDPOINT,
        aws_access_key_id=MINIO_ACCESS_KEY,
        aws_secret_access_key=MINIO_SECRET_KEY,
    )
    return s3


def _ensure_bucket_exists(s3) -> None:
    """버킷이 없으면 생성."""
    try:
        s3.head_bucket(Bucket=MINIO_BUCKET)
    except Exception:
        s3.create_bucket(Bucket=MINIO_BUCKET)


def clean_coinness_content(text: str) -> str:
    """Coinness 계열 특수 문구/이메일 제거 로직 (Spark 버전과 동일한 패턴)."""
    if text is None:
        return ""

    pattern = re.compile(
        r"[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}"
        r"|<저작권자.*?>"
        r"|\(.*?=.*?\).*?기자 ="
        r"|\[.*?기자\]"
    )
    cleaned = pattern.sub("", text)
    return cleaned.strip()


def transform_records(records: List[Dict[str, Any]]) -> pd.DataFrame:
    """원시 JSON 레코드 리스트를 Pandas DataFrame으로 변환 및 정제."""
    if not records:
        return pd.DataFrame()

    df = pd.DataFrame.from_records(records)

    # 누락 컬럼 보완
    expected_cols = [
        "title",
        "link",
        "published_at",
        "content",
        "content_length",
        "crawled_at",
        "source",
        "news_type",
    ]
    for col in expected_cols:
        if col not in df.columns:
            df[col] = None

    # Coinness 계열만 정제
    is_coinness = df["source"].astype(str).str.startswith("coinness")
    df.loc[is_coinness, "clean_content"] = df.loc[
        is_coinness, "content"
    ].astype(str).apply(clean_coinness_content)
    df.loc[~is_coinness, "clean_content"] = df.loc[~is_coinness, "content"]

    # 타임스탬프 → UTC datetime
    df["published_at_ts"] = pd.to_datetime(
        df["published_at"], utc=True, errors="coerce"
    )
    df["crawled_at_ts"] = pd.to_datetime(
        df["crawled_at"], utc=True, errors="coerce"
    )

    # source + news_type 합치기
    df["final_source"] = df["source"].astype(str)
    has_news_type = df["news_type"].notna() & df["news_type"].astype(str).ne("")
    df.loc[has_news_type, "final_source"] = (
        df.loc[has_news_type, "source"].astype(str)
        + "-"
        + df.loc[has_news_type, "news_type"].astype(str)
    )

    # 파티션 컬럼 (UTC 기준)
    # NaT가 있을 수 있으므로 .dt 접근 전 필터 주의
    ts = df["published_at_ts"].fillna(
        datetime.now(timezone.utc)
    )  # fallback
    df["year"] = ts.dt.year
    df["month"] = ts.dt.month
    df["day"] = ts.dt.day
    df["hour"] = ts.dt.hour
    df["min"] = ts.dt.minute

    # 최종 컬럼 정리
    final_df = df[
        [
            "title",
            "link",
            "published_at_ts",
            "clean_content",
            "content_length",
            "crawled_at_ts",
            "final_source",
            "year",
            "month",
            "day",
            "hour",
            "min",
        ]
    ].rename(
        columns={
            "published_at_ts": "published_at",
            "clean_content": "content",
            "crawled_at_ts": "crawled_at",
            "final_source": "source",
        }
    )

    return final_df


def write_batch_to_minio(df: pd.DataFrame, s3) -> None:
    """배치를 year/month/day/hour 파티션으로 나누어 Parquet로 MinIO에 저장."""
    if df.empty:
        return

    for (year, month, day, hour), group in df.groupby(
        ["year", "month", "day", "hour"]
    ):
        # 파티션 경로 구성 (Spark 스타일과 유사)
        prefix = (
            f"{BASE_PREFIX}/year={year:04d}/month={month:02d}/"
            f"day={day:02d}/hour={hour:02d}"
        )
        # 파일명: 현재 시각 기반 (Parquet)
        ts_str = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S")
        key = f"{prefix}/news_batch_{ts_str}.parquet"

        # Parquet로 직렬화 (인메모리, pyarrow 필요)
        buffer = BytesIO()
        # index는 필요 없으므로 저장하지 않음
        group.to_parquet(buffer, index=False, engine="pyarrow")
        body = buffer.getvalue()

        s3.put_object(
            Bucket=MINIO_BUCKET,
            Key=key,
            Body=body,
            ContentType="application/octet-stream",
        )
        print(f"[MinIO] Wrote batch to s3://{MINIO_BUCKET}/{key}")


def consume_and_process(batch_size: int = 500, max_batches: int = None) -> None:
    """
    Kafka에서 JSON 메시지를 읽어 Pandas로 전처리 후 MinIO에 저장.

    - batch_size: 이 개수만큼 모이면 MinIO에 한 번에 저장
    - max_batches: None이면 무한 반복, 숫자면 해당 배치 수만큼만 수행
    """
    print(f"[START] Connecting Kafka at {KAFKA_BOOTSTRAP_SERVERS}, topics={KAFKA_TOPICS}")

    consumer = KafkaConsumer(
        *KAFKA_TOPICS,
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        auto_offset_reset="latest",
        enable_auto_commit=True,
        value_deserializer=lambda v: v.decode("utf-8"),
    )

    print(
        f"[Kafka] Consuming from {KAFKA_TOPICS} "
        f"@ {KAFKA_BOOTSTRAP_SERVERS} (batch_size={batch_size})"
    )

    s3 = _create_minio_client()
    _ensure_bucket_exists(s3)

    buffer: List[Dict[str, Any]] = []
    batch_count = 0

    try:
        for msg in consumer:
            print(f"[MSG] topic={msg.topic}, partition={msg.partition}, offset={msg.offset}")
            try:
                payload = json.loads(msg.value)
                buffer.append(payload)
            except json.JSONDecodeError:
                print(f"[WARN] Invalid JSON skipped: {msg.value[:200]}")
                continue

            if len(buffer) >= batch_size:
                batch_count += 1
                print(f"[Batch {batch_count}] Processing {len(buffer)} records...")
                df = transform_records(buffer)
                write_batch_to_minio(df, s3)
                buffer.clear()

                if max_batches is not None and batch_count >= max_batches:
                    print("[INFO] Reached max_batches, exiting.")
                    break
    except KeyboardInterrupt:
        print("\n[INFO] Interrupted by user, flushing remaining records...")
    finally:
        if buffer:
            print(f"[Final Batch] Processing {len(buffer)} records...")
            df = transform_records(buffer)
            write_batch_to_minio(df, s3)
        consumer.close()


if __name__ == "__main__":
    # 예: python news_refine_stream_pandas.py  (무한 실행)
    # 또는: python news_refine_stream_pandas.py 200 10  → 200개씩 10배치 후 종료
    batch_size_arg = int(sys.argv[1]) if len(sys.argv) > 1 else 5
    max_batches_arg = int(sys.argv[2]) if len(sys.argv) > 2 else None

    consume_and_process(batch_size=batch_size_arg, max_batches=max_batches_arg)


