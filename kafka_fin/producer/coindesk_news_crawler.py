import feedparser
import json
import calendar
from datetime import datetime, timezone
from kafka import KafkaProducer
from kafka.errors import KafkaError

from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.remote.webdriver import WebDriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
import time


def create_kafka_producer(bootstrap_servers='localhost:9092'):
    """Kafka Producer 생성"""
    try:
        producer = KafkaProducer(
            bootstrap_servers=[bootstrap_servers],
            value_serializer=lambda v: json.dumps(v, ensure_ascii=False).encode('utf-8'),
            key_serializer=lambda k: k.encode('utf-8') if k else None,
            acks='all',  # 모든 replica가 메시지 수신 확인
            retries=3,
            max_in_flight_requests_per_connection=1,
            enable_idempotence=True  # 중복 방지
        )
        print(f"✅ Kafka Producer 생성 완료 (bootstrap_servers: {bootstrap_servers})")
        return producer
    except Exception as e:
        print(f"❌ Kafka Producer 생성 실패: {e}")
        raise


def send_to_kafka(producer, topic, article_data, key=None):
    """Kafka 토픽에 메시지 전송"""
    try:
        future = producer.send(
            topic,
            value=article_data,
            key=key
        )
        # 메시지 전송 결과 확인
        record_metadata = future.get(timeout=10)
        print(f"✅ 메시지 전송 성공 - Topic: {record_metadata.topic}, "
              f"Partition: {record_metadata.partition}, "
              f"Offset: {record_metadata.offset}")
        return True
    except KafkaError as e:
        print(f"❌ Kafka 전송 실패: {e}")
        return False
    except Exception as e:
        print(f"❌ 예상치 못한 오류: {e}")
        return False


def fetch_coindesk_news_with_selenium(topic_name='coindesk-news', num_articles=20, bootstrap_servers='localhost:9092', since_datetime=None, selenium_url='http://selenium-chrome:4444/wd/hub'):
    """RSS를 통해 Coindesk 뉴스를 가져와서 Selenium으로 크롤링하고 Kafka에 전송
    
    Args:
        topic_name: Kafka 토픽 이름
        num_articles: 처리할 최대 기사 수
        bootstrap_servers: Kafka 서버 주소
        since_datetime: 이 시간 이후의 뉴스만 가져옴 (datetime 객체, timezone-aware)
        selenium_url: Selenium Grid Hub URL (기본값: Docker 네트워크 내부 주소)
    """
    
    # Kafka Producer 생성
    try:
        producer = create_kafka_producer(bootstrap_servers)
    except Exception as e:
        print(f"❌ Kafka Producer 생성 실패로 종료합니다: {e}")
        return
    
    # RSS 피드에서 최신 기사 목록 가져오기
    rss_url = "https://www.coindesk.com/arc/outboundfeeds/rss/"
    print(f"\n📡 RSS 피드 가져오기: {rss_url}")
    feed = feedparser.parse(rss_url)
    
    if not feed.entries:
        print("⚠️ RSS 피드에서 기사를 찾을 수 없습니다.")
        producer.close()
        return
    
    print(f"✅ RSS 피드에서 총 {len(feed.entries)}개의 기사를 찾았습니다.")
    
    # since_datetime이 있으면 먼저 필터링 (RSS 단계에서 필터링)
    filtered_entries = []
    if since_datetime:
        # since_datetime이 timezone-aware가 아니면 UTC로 가정
        if since_datetime.tzinfo is None:
            since_datetime_utc = since_datetime.replace(tzinfo=timezone.utc)
        else:
            since_datetime_utc = since_datetime.astimezone(timezone.utc)
        
        for entry in feed.entries:
            if hasattr(entry, 'published_parsed') and entry.published_parsed:
                published_timestamp = calendar.timegm(entry.published_parsed)
                published_dt = datetime.fromtimestamp(published_timestamp, tz=timezone.utc)
                
                if published_dt > since_datetime_utc:
                    filtered_entries.append(entry)
        
        print(f"📅 시간 필터링: {since_datetime_utc} 이후의 기사 {len(filtered_entries)}개 발견")
    else:
        filtered_entries = feed.entries
    
    # num_articles 제한 적용
    if num_articles and len(filtered_entries) > num_articles:
        filtered_entries = filtered_entries[:num_articles]
        print(f"📝 최신 {num_articles}개만 처리합니다.\n")
    else:
        print(f"📝 {len(filtered_entries)}개를 처리합니다.\n")
    
    # 브라우저 옵션 설정 (캐시 및 세션 문제 해결)
    chrome_options = Options()
    chrome_options.add_argument("--incognito")  # 시크릿 모드 (캐시/쿠키 완전 제거)
    chrome_options.add_argument("--disable-cache")  # 캐시 비활성화
    chrome_options.add_argument("--disable-application-cache")  # 애플리케이션 캐시 비활성화
    chrome_options.add_argument("--disable-blink-features=AutomationControlled")
    chrome_options.add_experimental_option("excludeSwitches", ["enable-automation"])
    chrome_options.add_experimental_option('useAutomationExtension', False)
    chrome_options.add_argument("user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36")
    chrome_options.add_argument("--headless")  # 원격 Selenium은 headless 권장
    chrome_options.add_argument("--no-sandbox")
    chrome_options.add_argument("--disable-dev-shm-usage")
    chrome_options.add_argument("--disable-gpu")
    
    # Remote WebDriver 사용 (매 실행마다 새로운 세션)
    print(f"🔗 Selenium 연결: {selenium_url}")
    driver = None
    try:
        driver = webdriver.Remote(
            command_executor=selenium_url,
            options=chrome_options
        )
        print(f"✅ 새로운 브라우저 세션 생성 완료 (Session ID: {driver.session_id})")
        wait = WebDriverWait(driver, 15)
    except Exception as e:
        print(f"❌ Selenium 연결 실패: {e}")
        producer.flush()
        producer.close()
        return
    
    success_count = 0
    fail_count = 0
    
    try:
        # 필터링된 기사 처리 (이미 RSS 단계에서 시간 필터링 완료)
        processed_count = 0
        skipped_count = 0
        
        for idx, entry in enumerate(filtered_entries, 1):
            try:
                processed_count += 1
                print(f"\n{'='*60}")
                print(f"[{processed_count}] 기사 처리 중 (전체 {idx}/{num_articles})...")
                print(f"제목: {entry.title}")
                print(f"링크: {entry.link}")
                print(f"날짜: {entry.published}")
                print(f"{'='*60}")
                
                # 각 기사 페이지로 이동 (캐시 방지)
                driver.get(entry.link)
                print("페이지 로딩 대기 중...")
                
                # 캐시 방지: 쿠키 삭제 및 강제 새로고침
                driver.delete_all_cookies()
                time.sleep(2)  # 쿠키 삭제 후 대기
                driver.refresh()  # 강제 새로고침
                time.sleep(3)  # AJAX 데이터 로드 대기 (매우 중요!)
                
                # 'document-body'를 포함하는 모든 div 요소 찾기
                articles = wait.until(
                    EC.presence_of_all_elements_located((By.CSS_SELECTOR, "div[class*='document-body']"))
                )
                
                all_p_texts = []
                
                # 마지막 요소를 제외하고 반복 (슬라이싱 사용)
                for area in articles[:-1]:
                    # 각 div 영역 안에서 <p> 태그만 모두 찾기
                    paragraphs = area.find_elements(By.TAG_NAME, "p")
                    
                    for p in paragraphs:
                        text = p.text.strip()
                        # 비어있지 않은 텍스트만 리스트에 추가
                        if text:
                            all_p_texts.append(text)
                
                # 모든 문단을 공백으로 합치기
                final_article = " ".join(all_p_texts)
                
                if final_article:
                    print(f"✅ 본문 크롤링 완료 (길이: {len(final_article)}자)")
                    
                    # RSS 피드의 published 시간을 UTC로 변환
                    try:
                        if hasattr(entry, 'published_parsed') and entry.published_parsed:
                            published_timestamp = calendar.timegm(entry.published_parsed)
                            published_dt_utc = datetime.fromtimestamp(published_timestamp, tz=timezone.utc)
                            published_at_iso = published_dt_utc.isoformat()
                            published_at_str = published_dt_utc.strftime("%Y-%m-%d %H:%M:%S UTC")
                        else:
                            # 파싱 실패 시 원본 문자열 사용
                            published_at_iso = entry.published
                            published_at_str = entry.published
                    except Exception as e:
                        print(f"⚠️ 시간 변환 실패: {e}, 원본 사용")
                        published_at_iso = entry.published
                        published_at_str = entry.published
                    
                    # Kafka에 전송할 데이터 구조화
                    article_data = {
                        "title": entry.title,
                        "link": entry.link,
                        "published": entry.published,  # 원본 문자열 (참고용)
                        "published_at": published_at_iso,  # UTC ISO 형식
                        "published_at_display": published_at_str,  # UTC 읽기 쉬운 형식
                        "published_at_timezone": "UTC",
                        "content": final_article,
                        "content_length": len(final_article),
                        "crawled_at": datetime.now(timezone.utc).isoformat(),  # UTC
                        "source": "coindesk"
                    }
                    
                    # 메시지 키로 링크 사용 (중복 방지)
                    message_key = entry.link
                    
                    # Kafka에 전송
                    if send_to_kafka(producer, topic_name, article_data, key=message_key):
                        success_count += 1
                        print(f"📤 Kafka 전송 완료: {topic_name}")
                    else:
                        fail_count += 1
                        print(f"❌ Kafka 전송 실패")
                else:
                    print("⚠️ 본문을 찾을 수 없습니다. 건너뜁니다.")
                    fail_count += 1
                
                # 다음 기사 처리 전 잠시 대기 (서버 부하 방지)
                time.sleep(2)
                
            except Exception as e:
                print(f"⚠️ 기사 처리 중 에러 발생 ({entry.link}): {e}")
                fail_count += 1
                continue
        
        # 최종 결과 출력
        print(f"\n{'='*60}")
        print(f"📊 크롤링 완료 통계")
        print(f"✅ 성공: {success_count}개")
        print(f"❌ 실패: {fail_count}개")
        print(f"⏩ 스킵: {skipped_count}개 (시간 필터링)")
        print(f"📝 총 처리: {success_count + fail_count}개")
        print(f"{'='*60}")
    
    except Exception as e:
        print(f"❌ 에러 발생: {e}")
        import traceback
        traceback.print_exc()
    
    finally:
        # 반드시 브라우저 세션 종료 (캐시/세션 문제 해결의 핵심!)
        if driver:
            try:
                print("🧹 브라우저 세션 정리 중...")
                driver.quit()
                print("✅ 브라우저 세션 종료 완료")
            except Exception as e:
                print(f"⚠️ 브라우저 종료 중 오류 (무시 가능): {e}")
        
        # Producer 종료 전 모든 메시지 전송 완료 대기
        try:
            producer.flush()
            producer.close()
            print("✅ Kafka Producer 종료 완료")
        except Exception as e:
            print(f"⚠️ Producer 종료 중 오류: {e}")


if __name__ == "__main__":
    # 설정값 (필요시 수정)
    TOPIC_NAME = "coindesk-news"
    NUM_ARTICLES = 20
    BOOTSTRAP_SERVERS = "localhost:9092"
    SELENIUM_URL = "http://localhost:4444/wd/hub"  # 로컬 실행 시
    # since_datetime = datetime.now(timezone.utc) - timedelta(minutes=5)  # 최근 5분간
    
    fetch_coindesk_news_with_selenium(
        topic_name=TOPIC_NAME,
        num_articles=NUM_ARTICLES,
        bootstrap_servers=BOOTSTRAP_SERVERS,
        since_datetime=None,  # None이면 필터링 안 함
        selenium_url=SELENIUM_URL
    )

