import feedparser
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


def fetch_coindesk_news_with_selenium(topic_name='coindesk-news', num_articles=5, bootstrap_servers='localhost:9092'):
    """RSS를 통해 Coindesk 뉴스를 가져와서 Selenium으로 크롤링하고 Kafka에 전송"""
    
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
    
    print(f"✅ {len(feed.entries)}개의 기사를 찾았습니다. 최신 {num_articles}개를 처리합니다.\n")
    
    # 브라우저 옵션 설정
    chrome_options = Options()
    chrome_options.add_argument("--disable-blink-features=AutomationControlled")
    chrome_options.add_experimental_option("excludeSwitches", ["enable-automation"])
    chrome_options.add_experimental_option('useAutomationExtension', False)
    chrome_options.add_argument("user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36")
    # chrome_options.add_argument("--headless")  # 필요시 활성화
    
    driver = webdriver.Chrome(options=chrome_options)
    wait = WebDriverWait(driver, 15)
    
    success_count = 0
    fail_count = 0
    
    try:
        # 최신 기사 처리
        for idx, entry in enumerate(feed.entries[:num_articles], 1):
            try:
                print(f"\n{'='*60}")
                print(f"[{idx}/{num_articles}] 기사 처리 중...")
                print(f"제목: {entry.title}")
                print(f"링크: {entry.link}")
                print(f"날짜: {entry.published}")
                print(f"{'='*60}")
                
                # 각 기사 페이지로 이동
                driver.get(entry.link)
                print("페이지 로딩 대기 중...")
                
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
                    
                    # Kafka에 전송할 데이터 구조화
                    article_data = {
                        "title": entry.title,
                        "link": entry.link,
                        "published": entry.published,
                        "content": final_article,
                        "content_length": len(final_article),
                        "crawled_at": datetime.now().isoformat(),
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
        print(f"📝 총 처리: {success_count + fail_count}개")
        print(f"{'='*60}")
    
    except Exception as e:
        print(f"❌ 에러 발생: {e}")
    
    finally:
        # Producer 종료 전 모든 메시지 전송 완료 대기
        producer.flush()
        producer.close()
        print("\n확인을 위해 10초 대기합니다. (직접 끄셔도 됩니다)")
        time.sleep(10)
        driver.quit()
        print("✅ 브라우저 종료 완료")


if __name__ == "__main__":
    # 설정값 (필요시 수정)
    TOPIC_NAME = "coindesk-news"
    NUM_ARTICLES = 5
    BOOTSTRAP_SERVERS = "localhost:9092"
    
    fetch_coindesk_news_with_selenium(
        topic_name=TOPIC_NAME,
        num_articles=NUM_ARTICLES,
        bootstrap_servers=BOOTSTRAP_SERVERS
    )

