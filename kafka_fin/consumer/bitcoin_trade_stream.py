"""
Bitcoin Trade Stream Consumer

Binance BTC/USDT 거래 데이터를 Kafka에서 소비하여:
1. Bronze Layer: 원본 JSON을 그대로 Parquet로 저장
2. Silver Layer: 정제된 데이터를 Parquet로 저장
3. DuckDB: 실시간 분석용 테이블에 적재 (Upsert)

사용 예시:
    # Docker 내부 실행 (환경변수로 설정됨)
    python bitcoin_trade_stream.py

    # 로컬 테스트 (배치 100개씩, 최대 10배치)
    python bitcoin_trade_stream.py 100 10
"""

import json
import os
import sys
from datetime import datetime, timezone
from io import BytesIO
from typing import List, Dict, Any

import pandas as pd
import duckdb
from kafka import KafkaConsumer
import boto3


# =============================================================================
# 설정
# =============================================================================

# Kafka 설정
# - Docker 내부: kafka:29092 (docker-compose에서 환경변수로 설정됨)
# - Docker 외부 (로컬 테스트): localhost:9092
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
KAFKA_TOPIC = "binance.trades.btcusdt"

# MinIO(S3 호환) 설정
# - Docker 내부: http://minio:9000 (docker-compose에서 환경변수로 설정됨)
# - Docker 외부 (로컬 테스트): http://localhost:9000
MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "http://localhost:9000")
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY", "admin")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY", "password123")
MINIO_BUCKET = os.getenv("MINIO_BUCKET", "trade-lake")

# DuckDB 설정
# - Docker 내부에서는 /app/data에 저장
# - 로컬에서는 현재 디렉토리에 저장
DUCKDB_PATH = os.getenv("DUCKDB_PATH", "bitcoin_analytics.db")

# S3 경로
BRONZE_PREFIX = "bronze"  # 원본 데이터
SILVER_PREFIX = "silver"  # 정제 데이터


# =============================================================================
# MinIO 클라이언트
# =============================================================================

def create_minio_client():
    """MinIO(boto3) 클라이언트 생성"""
    session = boto3.session.Session()
    s3 = session.client(
        "s3",
        endpoint_url=MINIO_ENDPOINT,
        aws_access_key_id=MINIO_ACCESS_KEY,
        aws_secret_access_key=MINIO_SECRET_KEY,
    )
    return s3


def ensure_bucket_exists(s3) -> None:
    """버킷이 없으면 생성"""
    try:
        s3.head_bucket(Bucket=MINIO_BUCKET)
    except Exception:
        s3.create_bucket(Bucket=MINIO_BUCKET)
        print(f"[MinIO] Created bucket: {MINIO_BUCKET}")


# =============================================================================
# DuckDB 초기화
# =============================================================================

def init_duckdb() -> duckdb.DuckDBPyConnection:
    """DuckDB 연결 생성 및 테이블 초기화"""
    con = duckdb.connect(DUCKDB_PATH)
    
    # trades 테이블 생성 (trade_id를 PK로 사용하여 멱등성 보장)
    con.execute("""
        CREATE TABLE IF NOT EXISTS trades (
            trade_id BIGINT PRIMARY KEY,
            symbol VARCHAR,
            price DOUBLE,
            quantity DOUBLE,
            amount DOUBLE,
            side VARCHAR,
            trade_time TIMESTAMP,
            received_at TIMESTAMP
        )
    """)
    
    print(f"[DuckDB] Initialized at {DUCKDB_PATH}")
    return con


# =============================================================================
# 데이터 변환 함수
# =============================================================================

def process_trade(raw_msg: Dict[str, Any]) -> Dict[str, Any]:
    """
    바이낸스 raw 데이터를 Silver 스키마로 변환
    
    Input (raw_msg):
    {
        "source": "binance_ws",
        "stream": "trade",
        "symbol": "BTCUSDT",
        "received_at": "2026-01-27T18:38:17.200263+00:00",
        "payload": {
            "e": "trade", "E": 1769539097526, "s": "BTCUSDT", "t": 5821045789,
            "p": "87832.12000000", "q": "0.00100000", "T": 1769539097525,
            "m": False, "M": True
        }
    }
    
    Output (silver):
    {
        "trade_id": 5821045789,
        "symbol": "BTCUSDT",
        "price": 87832.12,
        "quantity": 0.001,
        "amount": 87.83212,  # price * quantity
        "side": "BUY",       # m=False → BUY, m=True → SELL
        "trade_time": Timestamp,
        "received_at": Timestamp
    }
    """
    payload = raw_msg.get("payload", {})
    
    price = float(payload.get("p", 0))
    quantity = float(payload.get("q", 0))
    
    return {
        "trade_id": payload.get("t"),
        "symbol": payload.get("s", "UNKNOWN"),
        "price": price,
        "quantity": quantity,
        "amount": price * quantity,  # 거래대금 계산
        "side": "SELL" if payload.get("m", False) else "BUY",
        "trade_time": pd.to_datetime(payload.get("T"), unit="ms", utc=True),
        "received_at": pd.to_datetime(raw_msg.get("received_at")),
    }


def transform_to_silver(records: List[Dict[str, Any]]) -> pd.DataFrame:
    """원본 레코드 리스트를 Silver DataFrame으로 변환"""
    if not records:
        return pd.DataFrame()
    
    silver_records = [process_trade(r) for r in records]
    df = pd.DataFrame(silver_records)
    
    # 파티션 컬럼 추가 (trade_time 기준)
    if "trade_time" in df.columns and not df.empty:
        ts = df["trade_time"].fillna(datetime.now(timezone.utc))
        df["year"] = ts.dt.year
        df["month"] = ts.dt.month
        df["day"] = ts.dt.day
        df["hour"] = ts.dt.hour
    
    return df


def transform_to_bronze(records: List[Dict[str, Any]]) -> pd.DataFrame:
    """원본 레코드 리스트를 Bronze DataFrame으로 변환 (JSON 그대로 보존)"""
    if not records:
        return pd.DataFrame()
    
    # 원본 JSON을 문자열로 저장
    bronze_records = []
    for r in records:
        received_at = pd.to_datetime(r.get("received_at"))
        bronze_records.append({
            "raw_json": json.dumps(r, ensure_ascii=False),
            "symbol": r.get("symbol", "UNKNOWN"),
            "received_at": received_at,
            "year": received_at.year if pd.notna(received_at) else datetime.now(timezone.utc).year,
            "month": received_at.month if pd.notna(received_at) else datetime.now(timezone.utc).month,
            "day": received_at.day if pd.notna(received_at) else datetime.now(timezone.utc).day,
            "hour": received_at.hour if pd.notna(received_at) else datetime.now(timezone.utc).hour,
        })
    
    return pd.DataFrame(bronze_records)


# =============================================================================
# 저장 함수
# =============================================================================

def write_to_minio(df: pd.DataFrame, s3, prefix: str, file_prefix: str) -> None:
    """DataFrame을 year/month/day/hour 파티션으로 나누어 MinIO에 저장"""
    if df.empty:
        return
    
    for (year, month, day, hour), group in df.groupby(["year", "month", "day", "hour"]):
        # 파티션 경로
        partition_path = (
            f"{prefix}/year={year:04d}/month={month:02d}/"
            f"day={day:02d}/hour={hour:02d}"
        )
        
        # 파일명 (현재 시각 + 밀리초 기반)
        ts_str = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S%f")
        key = f"{partition_path}/{file_prefix}_{ts_str}.parquet"
        
        # 저장할 컬럼 (파티션 컬럼 제외)
        save_cols = [c for c in group.columns if c not in ["year", "month", "day", "hour"]]
        
        # Parquet로 직렬화
        buffer = BytesIO()
        group[save_cols].to_parquet(buffer, index=False, engine="pyarrow")
        body = buffer.getvalue()
        
        s3.put_object(
            Bucket=MINIO_BUCKET,
            Key=key,
            Body=body,
            ContentType="application/octet-stream",
        )
        print(f"[MinIO] Wrote {len(group)} records to s3://{MINIO_BUCKET}/{key}")


def write_to_duckdb(df: pd.DataFrame, con: duckdb.DuckDBPyConnection) -> None:
    """Silver DataFrame을 DuckDB에 Upsert (trade_id 기준 중복 무시)"""
    if df.empty:
        return
    
    # DuckDB에 저장할 컬럼만 선택
    db_cols = ["trade_id", "symbol", "price", "quantity", "amount", "side", "trade_time", "received_at"]
    save_df = df[db_cols].copy()
    
    # DataFrame을 임시 테이블로 등록
    con.register("temp_trades", save_df)
    
    # INSERT OR IGNORE: trade_id 중복 시 무시 (멱등성)
    con.execute("""
        INSERT OR IGNORE INTO trades 
        SELECT * FROM temp_trades
    """)
    
    # 임시 테이블 해제
    con.unregister("temp_trades")
    
    print(f"[DuckDB] Upserted {len(save_df)} records")


def save_batch(
    raw_buffer: List[Dict[str, Any]],
    s3,
    con: duckdb.DuckDBPyConnection,
) -> None:
    """배치를 Bronze, Silver, DuckDB에 저장"""
    now = datetime.now(timezone.utc)
    
    # 1. Bronze 저장 (원본 JSON)
    bronze_df = transform_to_bronze(raw_buffer)
    write_to_minio(bronze_df, s3, BRONZE_PREFIX, "raw")
    
    # 2. Silver 저장 (정제 데이터)
    silver_df = transform_to_silver(raw_buffer)
    write_to_minio(silver_df, s3, SILVER_PREFIX, "trades")
    
    # 3. DuckDB 저장 (실시간 분석용)
    write_to_duckdb(silver_df, con)
    
    print(f"[Batch Complete] {len(raw_buffer)} records processed at {now.isoformat()}")


# =============================================================================
# 메인 Consumer 루프
# =============================================================================

def consume_and_process(batch_size: int = 100, max_batches: int = None) -> None:
    """
    Kafka에서 거래 데이터를 읽어 Bronze/Silver/DuckDB에 저장
    
    Args:
        batch_size: 배치 크기 (이 개수만큼 모이면 저장)
        max_batches: 최대 배치 수 (None이면 무한 실행)
    """
    print(f"[START] Connecting Kafka at {KAFKA_BOOTSTRAP_SERVERS}, topic={KAFKA_TOPIC}")
    print(f"[START] MinIO endpoint: {MINIO_ENDPOINT}, bucket: {MINIO_BUCKET}")
    print(f"[START] DuckDB path: {DUCKDB_PATH}")
    
    # Kafka Consumer 초기화
    consumer = KafkaConsumer(
        KAFKA_TOPIC,
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        auto_offset_reset="latest",  # 최신 메시지부터 소비
        enable_auto_commit=True,
        value_deserializer=lambda v: v.decode("utf-8"),
    )
    
    print(f"[Kafka] Connected! Consuming from {KAFKA_TOPIC} (batch_size={batch_size})")
    
    # MinIO 클라이언트 초기화
    s3 = create_minio_client()
    ensure_bucket_exists(s3)
    
    # DuckDB 초기화
    con = init_duckdb()
    
    # 버퍼 및 카운터
    buffer: List[Dict[str, Any]] = []
    batch_count = 0
    
    try:
        for msg in consumer:
            # 메시지 파싱
            try:
                payload = json.loads(msg.value)
                buffer.append(payload)
            except json.JSONDecodeError:
                print(f"[WARN] Invalid JSON skipped: {msg.value[:100]}")
                continue
            
            # 배치 크기 도달 시 저장
            if len(buffer) >= batch_size:
                batch_count += 1
                print(f"\n[Batch {batch_count}] Processing {len(buffer)} records...")
                
                save_batch(buffer, s3, con)
                buffer.clear()
                
                # 최대 배치 수 도달 시 종료
                if max_batches is not None and batch_count >= max_batches:
                    print("[INFO] Reached max_batches, exiting.")
                    break
                    
    except KeyboardInterrupt:
        print("\n[INFO] Interrupted by user, flushing remaining records...")
    finally:
        # 남은 버퍼 저장
        if buffer:
            print(f"[Final Batch] Processing {len(buffer)} records...")
            save_batch(buffer, s3, con)
        
        # 리소스 정리
        consumer.close()
        con.close()
        print("[SHUTDOWN] Consumer closed gracefully")


# =============================================================================
# 엔트리포인트
# =============================================================================

if __name__ == "__main__":
    # 사용법:
    # python bitcoin_trade_stream.py              → 배치 100개씩, 무한 실행
    # python bitcoin_trade_stream.py 50           → 배치 50개씩, 무한 실행
    # python bitcoin_trade_stream.py 100 10       → 배치 100개씩, 최대 10배치 후 종료
    
    batch_size_arg = int(sys.argv[1]) if len(sys.argv) > 1 else 100
    max_batches_arg = int(sys.argv[2]) if len(sys.argv) > 2 else None
    
    consume_and_process(batch_size=batch_size_arg, max_batches=max_batches_arg)
