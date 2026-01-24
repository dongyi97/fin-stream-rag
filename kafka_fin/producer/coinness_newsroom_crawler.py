import json
import time
import re
from datetime import datetime, timezone, timedelta
from kafka import KafkaProducer
from kafka.errors import KafkaError

import trafilatura
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.remote.webdriver import WebDriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC


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


def parse_coinness_date(date_str, time_str):
    """'2026년 1월 23일 금요일' 과 '13:09'를 datetime으로 변환 (UTC 기준)"""
    try:
        # 날짜 문자열에서 숫자 추출 (년, 월, 일)
        numbers = re.findall(r'\d+', date_str)
        if len(numbers) >= 3:
            year, month, day = map(int, numbers[:3])
        else:
            # 날짜 파싱 실패 시 오늘 날짜 사용 (UTC)
            now_utc = datetime.now(timezone.utc)
            year, month, day = now_utc.year, now_utc.month, now_utc.day
        
        # 시간 문자열 파싱 (HH:MM 형식) - 이미 UTC 기준
        hour, minute = map(int, time_str.split(':'))
        
        # UTC timezone으로 datetime 생성
        return datetime(year, month, day, hour, minute, tzinfo=timezone.utc)
    except Exception as e:
        print(f"⚠️ 날짜/시간 파싱 실패: {e}, 오늘 날짜 사용")
        now_utc = datetime.now(timezone.utc)
        try:
            hour, minute = map(int, time_str.split(':'))
            return now_utc.replace(hour=hour, minute=minute, second=0, microsecond=0)
        except:
            return now_utc


def extract_article_content(url):
    """trafilatura를 사용하여 기사 본문 추출"""
    try:
        # HTML 다운로드
        downloaded = trafilatura.fetch_url(url)
        
        if not downloaded:
            print(f"⚠️ URL에서 HTML을 다운로드할 수 없습니다: {url}")
            return None
        
        # 본문 추출 (JSON 형식)
        result = trafilatura.extract(downloaded, output_format='json', include_comments=False)
        
        if not result:
            print(f"⚠️ 본문을 추출할 수 없습니다: {url}")
            return None
        
        # JSON 파싱
        article_data = json.loads(result)
        return article_data
        
    except Exception as e:
        print(f"⚠️ 본문 추출 중 오류 발생 ({url}): {e}")
        return None


def fetch_coinness_newsroom(topic_name='coinness-newsroom', bootstrap_servers='localhost:9092', num_articles=None, since_datetime=None, selenium_url='http://selenium-chrome:4444/wd/hub'):
    """Coinness Newsroom 기사를 크롤링하고 Kafka에 전송
    
    Args:
        topic_name: Kafka 토픽 이름
        bootstrap_servers: Kafka 서버 주소
        num_articles: 처리할 최대 기사 수 (None이면 전체)
        since_datetime: 이 시간 이후의 뉴스만 가져옴 (datetime 객체, timezone-aware)
        selenium_url: Selenium Grid Hub URL (기본값: Docker 네트워크 내부 주소)
    """
    
    # Kafka Producer 생성
    try:
        producer = create_kafka_producer(bootstrap_servers)
    except Exception as e:
        print(f"❌ Kafka Producer 생성 실패로 종료합니다: {e}")
        return
    
    # 브라우저 설정 (캐시 및 세션 문제 해결)
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
        url = "https://coinness.com/article"
        print(f"\n📡 페이지 접속: {url}")
        driver.get(url)
        
        # 캐시 방지: 쿠키 삭제 및 강제 새로고침
        driver.delete_all_cookies()
        time.sleep(2)  # 쿠키 삭제 후 대기
        driver.refresh()  # 강제 새로고침
        time.sleep(5)  # AJAX 데이터 로드 대기 (매우 중요!)
        
        # 메인 컨테이너(ArticleListContainer 포함 클래스)가 나타날 때까지 대기
        print("브라우저 렌더링 대기 중...")
        list_container = wait.until(EC.presence_of_element_located(
            (By.XPATH, "//*[contains(@class, 'ArticleListContainer')]")
        ))
        
        # ArticleContent를 포함하는 각 기사 아이템들을 찾기
        articles = list_container.find_elements(By.XPATH, ".//*[contains(@class, 'ArticleContent')]")
        
        total_articles = len(articles)
        if num_articles:
            articles = articles[:num_articles]
        
        print(f"✅ 총 {total_articles}개의 기사를 발견했습니다. {len(articles)}개를 처리합니다.\n")
        
        processed_count = 0
        skipped_count = 0
        
        for idx, article in enumerate(articles, 1):
            try:
                # --- 1) 시간 및 날짜 추출 (TimeWrap 포함 클래스 내부) ---
                time_wrap = article.find_element(By.XPATH, ".//*[contains(@class, 'TimeWrap')]")
                
                # span.time-badge 안의 시간 (예: 13:09)
                try:
                    time_val = time_wrap.find_element(By.CLASS_NAME, "time-badge").text.strip()
                except:
                    time_val = "00:00"  # 기본값
                
                # TimeWrap의 전체 텍스트에서 시간 부분을 제외하여 날짜 정보 추출
                full_time_text = time_wrap.text.replace(time_val, "").strip()
                date_val = full_time_text if full_time_text else "날짜 정보 없음"
                
                # 시간 필터링 로직 (UTC 기준)
                if since_datetime:
                    try:
                        # 날짜와 시간을 datetime으로 변환 (UTC 기준)
                        published_dt_utc = parse_coinness_date(date_val, time_val)
                        
                        # since_datetime이 timezone-aware가 아니면 UTC로 가정
                        if since_datetime.tzinfo is None:
                            since_utc = since_datetime.replace(tzinfo=timezone.utc)
                        else:
                            since_utc = since_datetime.astimezone(timezone.utc)
                        
                        # UTC 기준으로 직접 비교
                        if published_dt_utc <= since_utc:
                            skipped_count += 1
                            print(f"⏩ 스킵 [{idx}]: 발행시간 {published_dt_utc} UTC (기준시간: {since_utc} UTC)")
                            continue
                    except Exception as e:
                        print(f"⚠️ [{idx}] 시간 필터링 실패: {e}. 계속 진행합니다.")
                
                processed_count += 1
                print(f"{'='*60}")
                print(f"[{processed_count}] 기사 처리 중 (전체 {idx}/{len(articles)})...")
                
                # --- 2) 기사 제목 추출 (ArticleTitle 포함 클래스 내부) ---
                title_val = article.find_element(By.XPATH, ".//*[contains(@class, 'ArticleTitle')]").text.strip()
                
                # --- 3) 링크 추출 (보통 ArticleContent의 부모 <a> 태그) ---
                try:
                    link_val = article.find_element(By.XPATH, "./ancestor::a").get_attribute("href")
                except:
                    link_val = None
                
                print(f"⏰ 시간: {time_val}")
                print(f"📅 날짜: {date_val}")
                print(f"📅 Published At UTC: {published_dt_utc}")
                print(f"📌 제목: {title_val}")
                print(f"🔗 링크: {link_val}")
                
                # 링크가 없으면 건너뜀
                if not link_val or link_val == "링크 없음":
                    print("⚠️ 링크가 없어 본문을 가져올 수 없습니다. 건너뜁니다.")
                    fail_count += 1
                    continue
                
                # --- 4) trafilatura를 사용하여 본문 추출 ---
                print(f"📄 본문 크롤링 중...")
                article_content = extract_article_content(link_val)
                
                if not article_content:
                    print("⚠️ 본문을 추출할 수 없습니다. 건너뜁니다.")
                    fail_count += 1
                    continue
                
                # 본문 텍스트 추출
                content_text = article_content.get('text', '')
                content_title = article_content.get('title', title_val)  # trafilatura 제목이 있으면 사용
                content_author = article_content.get('author', '')
                content_date = article_content.get('date', '')
                
                print(f"✅ 본문 크롤링 완료 (길이: {len(content_text)}자)")
                
                # UTC 시간으로 변환 (이미 UTC 기준이므로 변환 없이 직접 사용)
                try:
                    published_dt_utc = parse_coinness_date(date_val, time_val)
                    published_at_iso = published_dt_utc.isoformat()
                    published_at_str = published_dt_utc.strftime("%Y-%m-%d %H:%M:%S UTC")
                except Exception as e:
                    print(f"⚠️ 시간 변환 실패: {e}, 현재 시간 사용")
                    published_at_iso = datetime.now(timezone.utc).isoformat()
                    published_at_str = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")
                
                # Kafka에 전송할 데이터 구조화
                article_data = {
                    "title": content_title or title_val,
                    "content": content_text,
                    "time": time_val,  # 원본 시간 문자열 (UTC)
                    "date": date_val,  # 원본 날짜 문자열 (UTC)
                    "published_at": published_at_iso,  # UTC ISO 형식
                    "published_at_display": published_at_str,  # UTC 읽기 쉬운 형식
                    "published_at_timezone": "UTC",
                    "link": link_val,
                    "author": content_author,
                    "source": "coinness",
                    "news_type": "newsroom",
                    "crawled_at": datetime.now(timezone.utc).isoformat(),  # UTC
                    "content_length": len(content_text),
                    "trafilatura_metadata": {
                        "rawtitle": article_content.get('rawtitle', ''),
                        "hostname": article_content.get('hostname', ''),
                        "description": article_content.get('description', ''),
                        "sitename": article_content.get('sitename', ''),
                        "categories": article_content.get('categories', []),
                        "tags": article_content.get('tags', []),
                        "language": article_content.get('language', '')
                    }
                }
                
                # 메시지 키 생성 (링크를 사용하여 unique key 생성)
                message_key = link_val
                
                # Kafka에 전송
                if send_to_kafka(producer, topic_name, article_data, key=message_key):
                    success_count += 1
                    print(f"📤 Kafka 전송 완료: {topic_name}")
                else:
                    fail_count += 1
                    print(f"❌ Kafka 전송 실패")
                
                print(f"{'='*60}\n")
                
                # 다음 기사 처리 전 잠시 대기 (서버 부하 방지)
                time.sleep(2)
                
            except Exception as e:
                print(f"⚠️ 기사 처리 중 에러 발생 (인덱스 {idx}): {e}")
                import traceback
                traceback.print_exc()
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
    TOPIC_NAME = "coinness-newsroom"
    BOOTSTRAP_SERVERS = "localhost:9092"
    NUM_ARTICLES = None  # None이면 모든 기사 처리, 숫자를 지정하면 해당 개수만 처리
    SELENIUM_URL = "http://localhost:4444/wd/hub"  # 로컬 실행 시
    # since_datetime = datetime.now(timezone.utc) - timedelta(minutes=5)  # 최근 5분간
    
    fetch_coinness_newsroom(
        topic_name=TOPIC_NAME,
        bootstrap_servers=BOOTSTRAP_SERVERS,
        num_articles=NUM_ARTICLES,
        since_datetime=None,  # None이면 필터링 안 함
        selenium_url=SELENIUM_URL
    )

