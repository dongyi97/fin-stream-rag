import os
from typing import List, Dict, Any

from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from qdrant_client import QdrantClient
from openai import OpenAI
import uvicorn

from dotenv import load_dotenv

load_dotenv()  # .env 파일 로드

# --- 설정 (환경 변수에서 읽기) ---

QDRANT_HOST = os.getenv("QDRANT_HOST", "localhost")
QDRANT_PORT = int(os.getenv("QDRANT_PORT", "6333"))
COLLECTION_NAME = os.getenv("QDRANT_COLLECTION", "crypto_news")

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
async def ask_news(question: str, limit: int = 3):
    """
    뉴스 기반 질의응답 API.

    Args:
        question: 사용자의 질문
        limit: 검색할 뉴스 개수 (기본값: 3)

    Returns:
        answer: LLM이 생성한 답변
        reference_news: 참조된 뉴스 목록
    """
    if not question or not question.strip():
        raise HTTPException(status_code=400, detail="질문을 입력해주세요.")

    try:
        # 1. 사용자 질문을 벡터로 변환 (적재할 때와 동일한 모델 사용)
        query_response = openai_client.embeddings.create(
            input=question,
            model="text-embedding-3-small",
        )
        query_vector = query_response.data[0].embedding

        # 2. Qdrant에서 관련 뉴스 검색
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

        if not points:
            return {
                "answer": "죄송합니다. 관련된 뉴스 정보를 찾을 수 없습니다.",
                "reference_news": [],
            }

        # 3. 검색된 뉴스들로 컨텍스트 생성
        context_list = []
        sources = []

        for res in points:
            # res가 ScoredPoint 객체인지 확인
            if hasattr(res, "payload"):
                payload = res.payload
                score = res.score if hasattr(res, "score") else 0.0
            else:
                # 다른 형태의 결과일 경우
                payload = res.get("payload", {}) if isinstance(res, dict) else {}
                score = res.get("score", 0.0) if isinstance(res, dict) else 0.0
            
            title = payload.get("title", "제목 없음")
            content = payload.get("content", "")
            url = payload.get("url", "")

            context_list.append(f"[{title}]\n{content}")
            sources.append(
                {
                    "title": title,
                    "url": url,
                    "source": payload.get("source", ""),
                    "published_at": payload.get("published_at", ""),
                    "score": score,  # 유사도 점수
                }
            )

        context_text = "\n\n---\n\n".join(context_list)

        # 4. LLM에게 답변 요청
        system_prompt = f"""당신은 실시간 가상자산 뉴스 전문가입니다. 
제공된 [뉴스 정보]를 바탕으로 사용자의 질문에 친절하고 정확하게 답변하세요.

중요 규칙:
- 뉴스에 없는 내용은 절대 지어내지 마세요.
- 모르는 내용이면 "제공된 뉴스에는 해당 정보가 없습니다"라고 답변하세요.
- 답변은 한국어로 작성하세요.
- 가능하면 구체적인 날짜, 숫자, 출처를 포함하세요.

[뉴스 정보]:
{context_text}
"""

        response = openai_client.chat.completions.create(
            model="gpt-4o",  # 또는 "gpt-3.5-turbo"
            messages=[
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": question},
            ],
            temperature=0.2,  # 답변의 일관성을 위해 낮게 설정
        )

        return {
            "answer": response.choices[0].message.content,
            "reference_news": sources,
        }

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


if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8000)

