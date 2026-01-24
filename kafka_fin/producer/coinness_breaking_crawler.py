import json
from datetime import datetime, timezone, timedelta
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


def fetch_coinness_breaking_news(topic_name='coinness-breaking', bootstrap_servers='localhost:9092', since_datetime=None, selenium_url='http://selenium-chrome:4444/wd/hub'):
    """Coinness Breaking News를 크롤링하고 Kafka에 전송
    
    Args:
        topic_name: Kafka 토픽 이름
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
        url = "https://coinness.com/news"
        print(f"\n📡 페이지 접속: {url}")
        driver.get(url)
        
        # 캐시 방지: 쿠키 삭제 및 강제 새로고침
        driver.delete_all_cookies()
        time.sleep(2)  # 쿠키 삭제 후 대기
        driver.refresh()  # 강제 새로고침
        time.sleep(5)  # AJAX 데이터 로드 대기 (매우 중요!)
        
        # 명시적 대기 (Explicit Wait)
        # BreakingNewsWrap 클래스가 최소 하나라도 렌더링될 때까지 최대 15초 대기
        print("브라우저 렌더링 대기 중...")
        
        # 요소 탐색 (BeautifulSoup의 select와 대응되는 방식)
        # 클래스명에 'BreakingNewsWrap'이 포함된 모든 요소를 찾음
        breaking_news_wraps = wait.until(
            EC.presence_of_all_elements_located((By.CSS_SELECTOR, "div[class*='BreakingNewsWrap']"))
        )
        
        print(f"✅ 찾은 뉴스 래퍼 개수: {len(breaking_news_wraps)}개\n")
        
        # 반복문으로 하위 요소 탐색
        processed_count = 0
        skipped_count = 0
        
        for idx, wrap in enumerate(breaking_news_wraps, 1):
            try:
                # TimeBlock 추출
                time_element = wrap.find_element(By.CSS_SELECTOR, "div[class*='TimeBlock']")
                time_text = time_element.text.strip()  # "03:22"
                
                # time_text를 파싱하여 UTC 기준 datetime 생성 (time_text는 이미 UTC 기준)
                try:
                    # 시간 문자열 파싱 (HH:MM 형식) - 이미 UTC 기준
                    hour, minute = map(int, time_text.split(':'))
                    now_utc = datetime.now(timezone.utc)  # UTC 현재 시간
                    
                    # 오늘 날짜와 시간을 조합하여 UTC datetime 생성
                    published_at_utc = now_utc.replace(hour=hour, minute=minute, second=0, microsecond=0)
                    
                    # 만약 현재 시간이 00:05인데 뉴스 시간이 23:58이라면 어제 뉴스로 처리
                    if published_at_utc > now_utc:
                        published_at_utc = published_at_utc - timedelta(days=1)
                    
                except (ValueError, AttributeError) as e:
                    print(f"⚠️ [{idx}] 시간 파싱 실패 ({time_text}): {e}. 건너뜁니다.")
                    fail_count += 1
                    continue
                
                # 시간 필터링 로직 (UTC 기준으로 비교)
                if since_datetime:
                    # since_datetime이 timezone-aware가 아니면 UTC로 가정
                    if since_datetime.tzinfo is None:
                        since_utc = since_datetime.replace(tzinfo=timezone.utc)
                    else:
                        since_utc = since_datetime.astimezone(timezone.utc)
                    
                    if published_at_utc <= since_utc:
                        skipped_count += 1
                        print(f"⏩ 스킵 [{idx}]: 시간 {time_text} (발행시간: {published_at_utc} UTC, 기준시간: {since_utc} UTC)")
                        continue
                
                processed_count += 1
                print(f"{'='*60}")
                print(f"[{processed_count}] 뉴스 처리 중 (전체 {idx}/{len(breaking_news_wraps)})...")
                print(f"⏰ Time: {time_text} (UTC)")
                
                # Title 추출
                title_element = wrap.find_element(By.CSS_SELECTOR, "div[class*='BreakingNewsContentWrap'] div[class*='BreakingNewsTitle']")
                title_text = title_element.text.strip()
                print(f"📰 Title: {title_text}")
                
                # Content 추출
                content_element = wrap.find_element(By.CSS_SELECTOR, "div[class*='BreakingNewsContentWrap'] div[class*='BreakingNewsContents']")
                content_text = content_element.text.strip()
                print(f"📝 Content: {content_text}")
                
                # 링크가 있는지 확인 (선택적)
                link = None
                try:
                    link_element = wrap.find_element(By.CSS_SELECTOR, "a")
                    link = link_element.get_attribute("href")
                    if link:
                        print(f"🔗 Link: {link}")
                except:
                    pass  # 링크가 없어도 계속 진행
                
                # 이미 위에서 계산한 published_at_utc 사용
                published_at_iso = published_at_utc.isoformat()
                published_at_str = published_at_utc.strftime("%Y-%m-%d %H:%M:%S UTC")
                
                # Kafka에 전송할 데이터 구조화
                article_data = {
                    "title": title_text,
                    "content": content_text,
                    "time": time_text,  # 원본 시간 문자열 (UTC)
                    "published_at": published_at_iso,  # UTC ISO 형식
                    "published_at_display": published_at_str,  # UTC 읽기 쉬운 형식
                    "published_at_timezone": "UTC",
                    "link": link,
                    "source": "coinness",
                    "news_type": "breaking",
                    "crawled_at": datetime.now(timezone.utc).isoformat(),  # UTC
                    "content_length": len(content_text)
                }
                
                # 메시지 키 생성 (title + time 조합으로 unique key 생성)
                message_key = f"{title_text}_{time_text}_{idx}"
                
                # Kafka에 전송
                if send_to_kafka(producer, topic_name, article_data, key=message_key):
                    success_count += 1
                    print(f"📤 Kafka 전송 완료: {topic_name}")
                else:
                    fail_count += 1
                    print(f"❌ Kafka 전송 실패")
                
                print(f"{'='*60}\n")
                
                # 다음 뉴스 처리 전 잠시 대기 (서버 부하 방지)
                time.sleep(0.5)
                
            except Exception as e:
                print(f"⚠️ 뉴스 처리 중 에러 발생 (인덱스 {idx}): {e}")
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
    TOPIC_NAME = "coinness-breaking"
    BOOTSTRAP_SERVERS = "localhost:9092"
    SELENIUM_URL = "http://localhost:4444/wd/hub"  # 로컬 실행 시
    # since_datetime = datetime.now(timezone.utc) - timedelta(minutes=5)  # 최근 5분간
    
    fetch_coinness_breaking_news(
        topic_name=TOPIC_NAME,
        bootstrap_servers=BOOTSTRAP_SERVERS,
        since_datetime=None,  # None이면 필터링 안 함
        selenium_url=SELENIUM_URL
    )

