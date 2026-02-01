import streamlit as st
import requests
from typing import Dict, Any

# 페이지 설정
st.set_page_config(
    page_title="Crypto News Chatbot",
    page_icon="📈",
    layout="wide",
)

st.title("🤖 가상자산 챗봇")
st.markdown("뉴스나 비트코인 거래 데이터를 한 번에 물어보세요. 질문에 맞춰 자동으로 검색·조회합니다.")

# API 설정
API_BASE_URL = st.sidebar.text_input(
    "API 서버 주소",
    value="http://localhost:8000",
    help="FastAPI 서버 주소를 입력하세요.",
)

# 대화 기록 초기화
if "messages" not in st.session_state:
    st.session_state.messages = []

# 사이드바에 정보 표시
with st.sidebar:
    st.header("ℹ️ 정보")
    st.markdown("""
    한 질문으로 다음을 모두 처리합니다:
    - **뉴스**: 실시간 가상자산 뉴스 검색·질의응답, 참고 뉴스 출처
    - **비트코인 거래**: 평균가·거래량 등 DuckDB 집계 질의
    """)
    
    # API 상태 확인
    if st.button("🔍 API 연결 확인"):
        try:
            response = requests.get(f"{API_BASE_URL}/health", timeout=5)
            if response.status_code == 200:
                data = response.json()
                if data.get("status") == "healthy":
                    st.success("✅ API 서버 연결 성공")
                    st.json(data)
                else:
                    st.warning(f"⚠️ API 서버 상태: {data.get('status')}")
            else:
                st.error(f"❌ API 서버 응답 오류: {response.status_code}")
        except requests.exceptions.RequestException as e:
            st.error(f"❌ API 서버 연결 실패: {str(e)}")
            st.info("백엔드 서버가 실행 중인지 확인하세요:\n`python backend/query_api.py`")

# 기존 대화 표시
for message in st.session_state.messages:
    with st.chat_message(message["role"]):
        st.markdown(message["content"])
        
        # 어시스턴트 답변인 경우 참고 뉴스도 표시
        if message["role"] == "assistant" and "reference_news" in message:
            refs = message["reference_news"]
            if refs:
                with st.expander("📰 참고 뉴스"):
                    for ref in refs:
                        st.markdown(f"**{ref.get('title', '제목 없음')}**")
                        if ref.get("url"):
                            st.markdown(f"🔗 [원문 보기]({ref['url']})")
                        st.caption(f"출처: {ref.get('source', '')} | 발행일: {ref.get('published_at', '')} | 유사도: {ref.get('score', 0):.2f}")
                        st.divider()

# 사용자 입력 받기
if prompt := st.chat_input("뉴스나 비트코인 거래 데이터를 물어보세요 (예: 비트코인 뉴스, 최근 1시간 평균가)"):
    # 사용자 메시지 추가 및 표시
    st.session_state.messages.append({"role": "user", "content": prompt})
    with st.chat_message("user"):
        st.markdown(prompt)

    # FastAPI 서버에 요청
    with st.chat_message("assistant"):
        with st.spinner("답변을 생성 중입니다..."):
            try:
                response = requests.get(
                    f"{API_BASE_URL}/ask",
                    params={"question": prompt, "limit": 3},
                    timeout=60,
                )
                response.raise_for_status()  # HTTP 에러 체크
                
                data: Dict[str, Any] = response.json()
                answer = data.get("answer", "답변을 생성할 수 없습니다.")
                refs = data.get("reference_news", [])

                # 답변 출력
                st.markdown(answer)

                # 참고 뉴스 표시
                if refs:
                    with st.expander("📰 참고 뉴스"):
                        for ref in refs:
                            st.markdown(f"**{ref.get('title', '제목 없음')}**")
                            if ref.get("url"):
                                st.markdown(f"🔗 [원문 보기]({ref['url']})")
                            st.caption(
                                f"출처: {ref.get('source', '')} | "
                                f"발행일: {ref.get('published_at', '')} | "
                                f"유사도: {ref.get('score', 0):.2f}"
                            )
                            st.divider()

                # 대화 기록에 저장 (참고 뉴스 포함)
                st.session_state.messages.append({
                    "role": "assistant",
                    "content": answer,
                    "reference_news": refs,
                })

            except requests.exceptions.Timeout:
                st.error("⏱️ 요청 시간이 초과되었습니다. 다시 시도해주세요.")
            except requests.exceptions.ConnectionError:
                st.error(
                    "❌ API 서버에 연결할 수 없습니다.\n\n"
                    "백엔드 서버가 실행 중인지 확인하세요:\n"
                    "```bash\npython backend/query_api.py\n```"
                )
            except requests.exceptions.HTTPError as e:
                st.error(f"❌ HTTP 오류 발생: {e}\n\n응답: {response.text if 'response' in locals() else 'N/A'}")
            except Exception as e:
                st.error(f"❌ 오류 발생: {str(e)}")
                st.exception(e)

# 사이드바에 대화 초기화 버튼
with st.sidebar:
    st.divider()
    if st.button("🗑️ 대화 기록 초기화", type="secondary"):
        st.session_state.messages = []
        st.rerun()

