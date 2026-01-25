import os
import uuid

import duckdb
import pandas as pd
from langchain_text_splitters import RecursiveCharacterTextSplitter
from openai import OpenAI
from qdrant_client import QdrantClient
from qdrant_client.models import PointStruct, VectorParams, Distance

from dotenv import load_dotenv

load_dotenv()  # .env 파일 로드

# --- 1. 설정 (Configuration) ---

# MinIO에 저장된 Parquet 경로 (필요에 따라 날짜/시간 바꿔 쓰기)
# 예: 모든 날짜/시간을 읽고 싶으면 year=*/month=*/day=*/hour=* 형태로도 가능
MINIO_PATH = (
    "s3://news-lake/refined/"
    # "year=2026/month=01/day=24/hour=06/*.parquet"  # 예시
    "year=*/month=*/day=*/hour=*/*.parquet"
)

# Qdrant (docker-compose.yml 기준)
QDRANT_HOST = os.getenv("QDRANT_HOST", "localhost")
QDRANT_PORT = int(os.getenv("QDRANT_PORT", "6333"))
COLLECTION_NAME = "crypto_news"

# OpenAI
OPENAI_API_KEY = os.getenv("OPENAI_API_KEY")  # 반드시 환경변수로 설정
if not OPENAI_API_KEY:
    raise ValueError("OPENAI_API_KEY 환경 변수를 설정하세요.")

# MinIO (docker-compose.yml 기준)
MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "localhost:9000")
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY", "admin")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY", "password123")

# --- 2. 클라이언트 초기화 ---

client = QdrantClient(host=QDRANT_HOST, port=QDRANT_PORT)
openai_client = OpenAI(api_key=OPENAI_API_KEY)

# Qdrant 컬렉션이 없으면 생성 (처음 한 번만 실행됨)
if not client.collection_exists(COLLECTION_NAME):
    client.create_collection(
        collection_name=COLLECTION_NAME,
        # text-embedding-3-small 벡터 차원: 1536
        vectors_config=VectorParams(size=1536, distance=Distance.COSINE),
    )

# --- 3. DuckDB로 MinIO Parquet 읽기 ---

con = duckdb.connect()
con.execute("INSTALL httpfs; LOAD httpfs;")

con.execute(f"""
    SET s3_endpoint='{MINIO_ENDPOINT}';
    SET s3_access_key_id='{MINIO_ACCESS_KEY}';
    SET s3_secret_access_key='{MINIO_SECRET_KEY}';
    SET s3_use_ssl=false;
    SET s3_url_style='path';
""")

print(f"Loading data from: {MINIO_PATH}")

# 우리의 Parquet 스키마에 맞춰 컬럼 선택
# news_refine_stream_pandas.py 에서 저장한 컬럼:
# title, link, published_at, content, content_length, crawled_at, source, year, month, day, hour, min
df = con.execute(f"""
    SELECT
        title,
        content,
        link,
        published_at,
        source
    FROM read_parquet('{MINIO_PATH}')
""").df()

print(f"Total articles loaded: {len(df)}")

# --- 4. 청킹 (Chunking) 설정 ---

text_splitter = RecursiveCharacterTextSplitter(
    chunk_size=700,      # 한 덩어리 크기 (문자 기준)
    chunk_overlap=100,   # 문맥 유지를 위해 겹치는 구간
    separators=["\n\n", "\n", ".", " ", ""],
)

# --- 5. 임베딩 및 Qdrant 적재 ---

points = []
BATCH_UPLOAD_SIZE = 100  # Qdrant로 업로드할 청크 수 기준

for index, row in df.iterrows():
    content = row.get("content")
    if not content:
        continue

    # 5-1. 텍스트 청킹
    chunks = text_splitter.split_text(content)

    for i, chunk_text in enumerate(chunks):
        try:
            # 5-2. OpenAI 임베딩
            response = openai_client.embeddings.create(
                input=chunk_text,
                model="text-embedding-3-small",
            )
            vector = response.data[0].embedding

            # 5-3. Qdrant Point 생성
            published_at = row.get("published_at")
            if isinstance(published_at, pd.Timestamp):
                published_at = published_at.isoformat()

            points.append(
                PointStruct(
                    id=str(uuid.uuid4()),
                    vector=vector,
                    payload={
                        "source": row.get("source"),
                        "title": row.get("title"),
                        "content": chunk_text,             # 이 청크 자체
                        "full_content": content,           # 전체 본문
                        "url": row.get("link"),           # 우리 스키마에서는 link 컬럼
                        "published_at": published_at,
                        "chunk_index": i,
                    },
                )
            )
        except Exception as e:
            print(f"Error embedding chunk (row={index}, chunk={i}): {e}")

    # (옵션) 메모리 관리를 위해 일정 개수마다 업로드
    if len(points) >= BATCH_UPLOAD_SIZE:
        client.upsert(collection_name=COLLECTION_NAME, points=points)
        print(f"Uploaded {len(points)} chunks to Qdrant.")
        points = []

# 남은 데이터 업로드
if points:
    client.upsert(collection_name=COLLECTION_NAME, points=points)
    print(f"Final upload: {len(points)} chunks.")

print("Job Finished!")