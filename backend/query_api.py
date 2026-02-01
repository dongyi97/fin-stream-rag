import json
import logging
import os
from typing import List, Dict, Any, Optional

import duckdb
from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from qdrant_client import QdrantClient
from openai import OpenAI
import uvicorn

from dotenv import load_dotenv

load_dotenv()  # .env 파일 로드

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# --- 설정 (환경 변수에서 읽기) ---

QDRANT_HOST = os.getenv("QDRANT_HOST", "localhost")
QDRANT_PORT = int(os.getenv("QDRANT_PORT", "6333"))
COLLECTION_NAME = os.getenv("QDRANT_COLLECTION", "crypto_news")

# MinIO (비트코인 거래 Parquet, trade-consumer가 silver/에 적재)
# Backend는 DuckDB In-Memory + httpfs로 MinIO Parquet 직접 조회 (Lock 없음)
MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "http://localhost:9000")
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY", "admin")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY", "password123")
MINIO_TRADE_BUCKET = os.getenv("MINIO_TRADE_BUCKET", "trade-lake")

OPENAI_API_KEY = os.getenv("OPENAI_API_KEY")
if not OPENAI_API_KEY:
    raise ValueError("OPENAI_API_KEY 환경 변수를 설정하세요.")

# --- 클라이언트 초기화 ---

app = FastAPI(
    title="Crypto News RAG API",
    description="가상자산 뉴스 검색 및 질의응답 API",
    version="1.0.0",
)

# CORS 설정 (프론트엔드에서 접근 가능하도록)
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # 프로덕션에서는 특정 도메인만 허용
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

client = QdrantClient(host=QDRANT_HOST, port=QDRANT_PORT)
openai_client = OpenAI(api_key=OPENAI_API_KEY)


@app.on_event("startup")
def _log_minio_trade():
    """기동 시 MinIO 비트코인 거래 Parquet 경로 로그."""
    logger.info(
        "MinIO trade Parquet: s3://%s/silver/**/*.parquet (endpoint=%s)",
        MINIO_TRADE_BUCKET,
        MINIO_ENDPOINT,
    )


# --- DuckDB In-Memory + MinIO Parquet (비트코인 거래, Lock 없음) ---

def get_duckdb_minio_connection() -> duckdb.DuckDBPyConnection:
    """
    MinIO(S3)에 연결된 In-Memory DuckDB 세션 생성.
    질문마다 새 세션으로 Stateless, Lock 문제 없음.
    """
    con = duckdb.connect(database=":memory:")
    con.execute("INSTALL httpfs; LOAD httpfs;")
    endpoint = MINIO_ENDPOINT.replace("http://", "").replace("https://", "")
    con.execute(f"SET s3_endpoint='{endpoint}';")
    con.execute(f"SET s3_access_key_id='{MINIO_ACCESS_KEY}';")
    con.execute(f"SET s3_secret_access_key='{MINIO_SECRET_KEY}';")
    con.execute("SET s3_use_ssl=false;")
    con.execute("SET s3_url_style='path';")
    return con


def _is_read_only_sql(sql: str) -> bool:
    """SELECT만 허용 (DELETE/UPDATE/INSERT/DROP 등 차단)."""
    stripped = sql.strip().upper()
    if not stripped.startswith("SELECT"):
        return False
    blocked = ("DELETE", "UPDATE", "INSERT", "DROP", "CREATE", "ALTER", "TRUNCATE")
    for w in blocked:
        if w in stripped:
            return False
    return True


def query_duckdb(sql_query: str) -> str:
    """
    MinIO Silver Parquet를 read_parquet로 조회. trades 뷰로 SELECT만 실행.
    DuckDB In-Memory 일회용 세션 → Lock 없음.
    """
    if not _is_read_only_sql(sql_query):
        return "Error: Only SELECT queries are allowed."
    con = None
    try:
        con = get_duckdb_minio_connection()
        parquet_path = f"s3://{MINIO_TRADE_BUCKET}/silver/**/*.parquet"
        con.execute(f"""
            CREATE OR REPLACE VIEW trades AS
            SELECT * FROM read_parquet('{parquet_path}')
        """)
        result = con.execute(sql_query).df()
        return result.to_json(orient="records", date_format="iso")
    except Exception as e:
        return f"Error: {str(e)}"
    finally:
        if con is not None:
            con.close()


def _search_news_impl(query: str, limit: int = 3) -> tuple[str, list]:
    """
    뉴스 벡터 검색 (RAG). 통합 /ask 에서 search_news 도구로 사용.
    Returns: (LLM용 컨텍스트 문자열, reference_news 목록)
    """
    query_response = openai_client.embeddings.create(
        input=query,
        model="text-embedding-3-small",
    )
    query_vector = query_response.data[0].embedding
    try:
        search_response = client.query_points(
            collection_name=COLLECTION_NAME,
            query=query_vector,
            limit=limit,
        )
        points = search_response.points if hasattr(search_response, "points") else []
    except Exception:
        return "뉴스 검색 중 오류가 발생했습니다.", []

    if not points:
        return "관련 뉴스를 찾을 수 없습니다.", []

    context_list = []
    sources = []
    for res in points:
        if hasattr(res, "payload"):
            payload = res.payload
            score = res.score if hasattr(res, "score") else 0.0
        else:
            payload = res.get("payload", {}) if isinstance(res, dict) else {}
            score = res.get("score", 0.0) if isinstance(res, dict) else 0.0
        title = payload.get("title", "제목 없음")
        content = payload.get("content", "")
        url = payload.get("url", "")
        context_list.append(f"[{title}]\n{content}")
        sources.append({
            "title": title,
            "url": url,
            "source": payload.get("source", ""),
            "published_at": payload.get("published_at", ""),
            "score": score,
        })
    context_text = "\n\n---\n\n".join(context_list)
    return context_text, sources


# 통합 질의: 뉴스 RAG + DuckDB 둘 다 도구로 제공
UNIFIED_TOOLS = [
    {
        "type": "function",
        "function": {
            "name": "search_news",
            "description": "가상자산(코인) 관련 최신 뉴스를 검색합니다. 뉴스 내용·가격 동향·이슈를 물어볼 때 사용하세요.",
            "parameters": {
                "type": "object",
                "properties": {
                    "query": {"type": "string", "description": "검색할 질문 또는 키워드 (예: 비트코인 가격, 이더리움 뉴스)"},
                    "limit": {"type": "integer", "description": "가져올 뉴스 개수 (기본 3)", "default": 3},
                },
                "required": ["query"],
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "query_duckdb",
            "description": "Bitcoin(BTC/USDT) 거래 데이터에서 SQL로 집계·조회합니다. 평균가, 거래량, 최근 가격 등 숫자/통계 질문에 사용하세요.",
            "parameters": {
                "type": "object",
                "properties": {
                    "sql_query": {
                        "type": "string",
                        "description": (
                            "실행할 SQL. 테이블명 'trades', 컬럼: trade_id, symbol, price, quantity, amount, side(BUY/SELL), trade_time, received_at. "
                            "예: SELECT AVG(price) as avg_price FROM trades WHERE trade_time >= now() - interval '1 hour'"
                        ),
                    }
                },
                "required": ["sql_query"],
            },
        },
    },
]


def _ask_unified(question: str, limit: int = 3) -> tuple[str, list]:
    """
    한 질문으로 뉴스·비트코인 거래 모두 처리. LLM이 필요한 도구(search_news, query_duckdb)를 선택해 호출.
    Returns: (answer, reference_news)
    """
    system = (
        "당신은 가상자산(코인) 전문가입니다. "
        "필요하면 search_news(뉴스 검색)와 query_duckdb(비트코인 거래 데이터 SQL 조회) 도구를 사용하세요. "
        "질문이 뉴스/이슈 관련이면 search_news, 평균가·거래량 등 숫자/통계면 query_duckdb를 사용하고, 둘 다 필요하면 둘 다 호출하세요. "
        "답변은 한국어로 작성하세요.\n\n"
        "뉴스 관련 답변 시 중요 규칙:\n"
        "- 뉴스에 없는 내용은 절대 지어내지 마세요.\n"
        "- 모르는 내용이면 \"제공된 뉴스에는 해당 정보가 없습니다\"라고 답변하세요.\n"
        "- 가능하면 구체적인 날짜, 숫자, 출처를 포함하세요."
    )
    messages = [
        {"role": "system", "content": system},
        {"role": "user", "content": question},
    ]
    collected_refs: list = []

    response = openai_client.chat.completions.create(
        model="gpt-4o",
        messages=messages,
        tools=UNIFIED_TOOLS,
    )
    msg = response.choices[0].message

    while getattr(msg, "tool_calls", None):
        messages.append(msg)
        for tc in msg.tool_calls:
            try:
                args = json.loads(tc.function.arguments)
            except json.JSONDecodeError:
                messages.append({"role": "tool", "tool_call_id": tc.id, "content": "Error: Invalid arguments."})
                continue
            if tc.function.name == "search_news":
                q = args.get("query", question)
                lim = args.get("limit", limit)
                context_text, sources = _search_news_impl(q, lim)
                collected_refs.extend(sources)
                content = context_text
            elif tc.function.name == "query_duckdb":
                sql = args.get("sql_query", "")
                content = query_duckdb(sql)
            else:
                content = "Unknown tool."
            messages.append({"role": "tool", "tool_call_id": tc.id, "content": content})

        response = openai_client.chat.completions.create(
            model="gpt-4o",
            messages=messages,
            tools=UNIFIED_TOOLS,
        )
        msg = response.choices[0].message

    answer = msg.content or "답변을 생성할 수 없습니다."
    return answer, collected_refs


@app.get("/")
async def root():
    """API 상태 확인."""
    return {
        "status": "ok",
        "service": "Crypto News RAG API",
        "qdrant_host": QDRANT_HOST,
        "qdrant_port": QDRANT_PORT,
        "collection": COLLECTION_NAME,
    }


@app.get("/health")
async def health_check():
    """헬스 체크 엔드포인트."""
    try:
        # Qdrant 연결 확인
        collections = client.get_collections()
        collection_exists = any(
            col.name == COLLECTION_NAME for col in collections.collections
        )
        return {
            "status": "healthy",
            "qdrant_connected": True,
            "collection_exists": collection_exists,
        }
    except Exception as e:
        return {
            "status": "unhealthy",
            "error": str(e),
        }


@app.get("/ask")
async def ask_unified(question: str, limit: int = 3):
    """
    통합 질의응답 API. 한 질문으로 뉴스(RAG)와 비트코인 거래(DuckDB) 모두 처리합니다.
    LLM이 질문에 따라 search_news, query_duckdb 도구를 선택해 호출합니다.

    Returns:
        answer: LLM이 생성한 답변
        reference_news: 참조된 뉴스 목록 (뉴스 검색 시에만)
    """
    if not question or not question.strip():
        raise HTTPException(status_code=400, detail="질문을 입력해주세요.")

    try:
        answer, reference_news = _ask_unified(question, limit=limit)
        return {"answer": answer, "reference_news": reference_news}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"처리 중 오류가 발생했습니다: {str(e)}")


@app.get("/search")
async def search_news(query: str, limit: int = 5):
    """
    벡터 검색만 수행 (LLM 답변 없이).

    Args:
        query: 검색어
        limit: 반환할 결과 개수

    Returns:
        검색된 뉴스 목록
    """
    if not query or not query.strip():
        raise HTTPException(status_code=400, detail="검색어를 입력해주세요.")

    try:
        # 질문을 벡터로 변환
        query_response = openai_client.embeddings.create(
            input=query,
            model="text-embedding-3-small",
        )
        query_vector = query_response.data[0].embedding

        # Qdrant 검색
        # 최신 qdrant-client에서는 query_points 사용 (권장)
        try:
            search_response = client.query_points(
                collection_name=COLLECTION_NAME,
                query=query_vector,
                limit=limit,
            )
            # query_points 결과는 .points 속성으로 접근
            points = search_response.points if hasattr(search_response, 'points') else []
        except Exception as e:
            # 상세한 에러 정보 포함
            error_msg = str(e)
            error_type = type(e).__name__
            raise HTTPException(
                status_code=500,
                detail=f"Qdrant 검색 중 오류 ({error_type}): {error_msg}"
            )

        results = []
        for res in points:
            # res가 ScoredPoint 객체인지 확인
            if hasattr(res, "payload"):
                payload = res.payload
                score = res.score if hasattr(res, "score") else 0.0
            else:
                payload = res.get("payload", {}) if isinstance(res, dict) else {}
                score = res.get("score", 0.0) if isinstance(res, dict) else 0.0
            
            results.append(
                {
                    "title": payload.get("title", ""),
                    "content": payload.get("content", ""),
                    "url": payload.get("url", ""),
                    "source": payload.get("source", ""),
                    "published_at": payload.get("published_at", ""),
                    "score": score,
                }
            )

        return {"query": query, "results": results}

    except Exception as e:
        raise HTTPException(status_code=500, detail=f"검색 중 오류가 발생했습니다: {str(e)}")


# --- 비트코인 거래 DuckDB + Function Calling ---

DUCKDB_TOOLS = [
    {
        "type": "function",
        "function": {
            "name": "query_duckdb",
            "description": "Bitcoin(BTC/USDT) 거래 데이터가 담긴 DuckDB에 SQL을 실행해 데이터를 조회합니다. SELECT만 허용됩니다.",
            "parameters": {
                "type": "object",
                "properties": {
                    "sql_query": {
                        "type": "string",
                        "description": (
                            "실행할 SQL 문. 테이블명은 'trades'이며, "
                            "컬럼: trade_id(BIGINT), symbol(VARCHAR), price(DOUBLE), quantity(DOUBLE), "
                            "amount(DOUBLE), side(VARCHAR: BUY/SELL), trade_time(TIMESTAMP), received_at(TIMESTAMP). "
                            "예: 최근 1시간 평균가 → SELECT AVG(price) as avg_price FROM trades WHERE trade_time >= now() - interval '1 hour'"
                        ),
                    }
                },
                "required": ["sql_query"],
            },
        },
    }
]


def ask_bitcoin_bot(user_question: str) -> str:
    """
    LLM이 query_duckdb 도구를 사용해 DuckDB에 SQL을 실행하고,
    결과를 받아 자연어로 답변합니다.
    """
    response = openai_client.chat.completions.create(
        model="gpt-4o",
        messages=[{"role": "user", "content": user_question}],
        tools=DUCKDB_TOOLS,
    )

    message = response.choices[0].message
    tool_calls = message.tool_calls

    if not tool_calls:
        return message.content or "답변을 생성할 수 없습니다."

    # 첫 번째 tool_call만 처리 (필요 시 루프로 확장)
    for tool_call in tool_calls:
        if tool_call.function.name != "query_duckdb":
            continue
        try:
            args = json.loads(tool_call.function.arguments)
            sql = args.get("sql_query", "")
        except json.JSONDecodeError:
            return "Error: Invalid tool arguments."
        db_result = query_duckdb(sql)

        final_response = openai_client.chat.completions.create(
            model="gpt-4o",
            messages=[
                {"role": "user", "content": user_question},
                message,
                {
                    "role": "tool",
                    "tool_call_id": tool_call.id,
                    "content": db_result,
                },
            ],
        )
        return final_response.choices[0].message.content or "답변을 생성할 수 없습니다."

    return message.content or "답변을 생성할 수 없습니다."


@app.get("/ask-bitcoin")
async def ask_bitcoin(question: str):
    """
    비트코인 거래 데이터(MinIO Silver Parquet) 기반 질의응답.
    DuckDB In-Memory + read_parquet로 MinIO를 조회합니다 (Lock 없음).
    """
    if not question or not question.strip():
        raise HTTPException(status_code=400, detail="질문을 입력하세요.")

    try:
        answer = ask_bitcoin_bot(question)
        return {"answer": answer}
    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"처리 중 오류가 발생했습니다: {str(e)}",
        )


if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8000)

