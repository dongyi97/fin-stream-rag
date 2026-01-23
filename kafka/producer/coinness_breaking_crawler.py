import json
from datetime import datetime
from kafka import KafkaProducer
from kafka.errors import KafkaError

from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
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


def fetch_coinness_breaking_news(topic_name='coinness-breaking', bootstrap_servers='localhost:9092'):
    """Coinness Breaking News를 크롤링하고 Kafka에 전송"""
    
    # Kafka Producer 생성
    try:
        producer = create_kafka_producer(bootstrap_servers)
    except Exception as e:
        print(f"❌ Kafka Producer 생성 실패로 종료합니다: {e}")
        return
    
    # 브라우저 옵션 설정
    chrome_options = Options()
    chrome_options.add_argument("--disable-blink-features=AutomationControlled")
    chrome_options.add_experimental_option("excludeSwitches", ["enable-automation"])
    chrome_options.add_experimental_option('useAutomationExtension', False)
    chrome_options.add_argument("user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36")
    # chrome_options.add_argument("--headless")  # 내부 동작 확인 후 필요시 활성화
    
    driver = webdriver.Chrome(options=chrome_options)
    wait = WebDriverWait(driver, 15)
    
    success_count = 0
    fail_count = 0
    
    try:
        url = "https://coinness.com/news"
        print(f"\n📡 페이지 접속: {url}")
        driver.get(url)
        
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
        for idx, wrap in enumerate(breaking_news_wraps, 1):
            try:
                print(f"{'='*60}")
                print(f"[{idx}/{len(breaking_news_wraps)}] 뉴스 처리 중...")
                
                # TimeBlock 추출
                time_element = wrap.find_element(By.CSS_SELECTOR, "div[class*='TimeBlock']")
                time_text = time_element.text.strip()  # "03:22"
                print(f"⏰ Time: {time_text}")
                
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
                
                # 오늘 날짜와 시간을 조합하여 published_at 생성
                today = datetime.now().strftime("%Y-%m-%d")
                published_at = f"{today} {time_text}"
                
                # Kafka에 전송할 데이터 구조화
                article_data = {
                    "title": title_text,
                    "content": content_text,
                    "time": time_text,
                    "published_at": published_at,
                    "link": link,
                    "source": "coinness",
                    "news_type": "breaking",
                    "crawled_at": datetime.now().isoformat(),
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
        print(f"📝 총 처리: {success_count + fail_count}개")
        print(f"{'='*60}")
    
    except Exception as e:
        print(f"❌ 에러 발생: {e}")
        import traceback
        traceback.print_exc()
    
    finally:
        # Producer 종료 전 모든 메시지 전송 완료 대기
        producer.flush()
        producer.close()
        print("\n확인을 위해 5초 대기합니다. (직접 끄셔도 됩니다)")
        time.sleep(5)
        driver.quit()
        print("✅ 브라우저 종료 완료")


if __name__ == "__main__":
    # 설정값 (필요시 수정)
    TOPIC_NAME = "coinness-breaking"
    BOOTSTRAP_SERVERS = "localhost:9092"
    
    fetch_coinness_breaking_news(
        topic_name=TOPIC_NAME,
        bootstrap_servers=BOOTSTRAP_SERVERS
    )

