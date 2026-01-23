import json
import time
from datetime import datetime
from kafka import KafkaProducer
from kafka.errors import KafkaError

import trafilatura
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from webdriver_manager.chrome import ChromeDriverManager


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


def fetch_coinness_newsroom(topic_name='coinness-newsroom', bootstrap_servers='localhost:9092', num_articles=None):
    """Coinness Newsroom 기사를 크롤링하고 Kafka에 전송"""
    
    # Kafka Producer 생성
    try:
        producer = create_kafka_producer(bootstrap_servers)
    except Exception as e:
        print(f"❌ Kafka Producer 생성 실패로 종료합니다: {e}")
        return
    
    # 브라우저 설정
    chrome_options = Options()
    chrome_options.add_argument("--disable-blink-features=AutomationControlled")
    chrome_options.add_experimental_option("excludeSwitches", ["enable-automation"])
    chrome_options.add_experimental_option('useAutomationExtension', False)
    chrome_options.add_argument("user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36")
    chrome_options.add_argument("--headless")
    
    driver = webdriver.Chrome(service=Service(ChromeDriverManager().install()), options=chrome_options)
    wait = WebDriverWait(driver, 15)
    
    success_count = 0
    fail_count = 0
    
    try:
        url = "https://coinness.com/article"
        print(f"\n📡 페이지 접속: {url}")
        driver.get(url)
        
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
        
        for idx, article in enumerate(articles, 1):
            try:
                print(f"{'='*60}")
                print(f"[{idx}/{len(articles)}] 기사 처리 중...")
                
                # --- 1) 시간 및 날짜 추출 (TimeWrap 포함 클래스 내부) ---
                time_wrap = article.find_element(By.XPATH, ".//*[contains(@class, 'TimeWrap')]")
                
                # span.time-badge 안의 시간 (예: 13:09)
                try:
                    time_val = time_wrap.find_element(By.CLASS_NAME, "time-badge").text.strip()
                except:
                    time_val = "시간 정보 없음"
                
                # TimeWrap의 전체 텍스트에서 시간 부분을 제외하여 날짜 정보 추출
                full_time_text = time_wrap.text.replace(time_val, "").strip()
                date_val = full_time_text if full_time_text else "날짜 정보 없음"
                
                # --- 2) 기사 제목 추출 (ArticleTitle 포함 클래스 내부) ---
                title_val = article.find_element(By.XPATH, ".//*[contains(@class, 'ArticleTitle')]").text.strip()
                
                # --- 3) 링크 추출 (보통 ArticleContent의 부모 <a> 태그) ---
                try:
                    link_val = article.find_element(By.XPATH, "./ancestor::a").get_attribute("href")
                except:
                    link_val = None
                
                print(f"⏰ 시간: {time_val}")
                print(f"📅 날짜: {date_val}")
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
                
                # Kafka에 전송할 데이터 구조화
                article_data = {
                    "title": content_title or title_val,
                    "content": content_text,
                    "time": time_val,
                    "date": date_val,
                    "published_at": content_date or f"{date_val} {time_val}",
                    "link": link_val,
                    "author": content_author,
                    "source": "coinness",
                    "news_type": "newsroom",
                    "crawled_at": datetime.now().isoformat(),
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
        driver.quit()
        print("✅ 브라우저 종료 완료")


if __name__ == "__main__":
    # 설정값 (필요시 수정)
    TOPIC_NAME = "coinness-newsroom"
    BOOTSTRAP_SERVERS = "localhost:9092"
    NUM_ARTICLES = None  # None이면 모든 기사 처리, 숫자를 지정하면 해당 개수만 처리
    
    fetch_coinness_newsroom(
        topic_name=TOPIC_NAME,
        bootstrap_servers=BOOTSTRAP_SERVERS,
        num_articles=NUM_ARTICLES
    )

