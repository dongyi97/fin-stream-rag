import json
import time
import signal
import sys
from datetime import datetime, timezone
from websocket import WebSocketApp
from confluent_kafka import Producer

# ----------------------------
# 설정
# ----------------------------
BOOTSTRAP_SERVERS = "localhost:9092"
TOPIC = "binance.trades.btcusdt"

# Binance trade stream (btcusdt)
BINANCE_WS_URL = "wss://stream.binance.com:9443/ws/btcusdt@trade"

# Kafka producer config
KAFKA_CONF = {
    "bootstrap.servers": BOOTSTRAP_SERVERS,
    # 안정성 옵션들(원하시면 조정)
    "acks": "all",
    "enable.idempotence": True,   # 중복 방지(Exactly-once에 가까운 동작)
    "retries": 10,
    "linger.ms": 20,              # 약간 배치해서 효율 개선
    "batch.num.messages": 1000,
    "compression.type": "snappy", # 선택: 네트워크 절약
}

producer = Producer(KAFKA_CONF)
running = True


def delivery_report(err, msg):
    """Kafka 메시지 전송 결과 콜백"""
    if err is not None:
        # 여기서 로깅/재시도 정책을 더 정교하게 둘 수 있습니다.
        print(f"[DELIVERY ERROR] {err} | topic={msg.topic()} partition={msg.partition()}", file=sys.stderr)
    else:
        # 성공 시에도 로그를 남기고 싶다면 (선택적)
        # print(f"[DELIVERY SUCCESS] topic={msg.topic()} partition={msg.partition()} offset={msg.offset()}")
        pass


def now_utc_iso():
    """현재 UTC 시간을 ISO 형식으로 반환"""
    return datetime.now(timezone.utc).isoformat()


def on_message(ws, message: str):
    """
    Binance trade event 예시(JSON):
    {
      "e":"trade","E":..., "s":"BTCUSDT","t":..., "p":"...", "q":"...",
      "b":..., "a":..., "T":..., "m":true, "M":true
    }
    """
    try:
        data = json.loads(message)
        
        # Kafka key: 심볼(파티셔닝 기준)
        key = data.get("s", "UNKNOWN").encode("utf-8")
        
        envelope = {
            "source": "binance_ws",
            "stream": "trade",
            "symbol": data.get("s", "BTCUSDT"),
            "received_at": now_utc_iso(),  # 우리 쪽 수신 시각
            "payload": data,               # 원문 payload
        }
        
        value = json.dumps(envelope, ensure_ascii=False).encode("utf-8")
        
        # 큐가 꽉 차면 BufferError가 날 수 있으니 poll/flush로 관리
        try:
            producer.produce(
                topic=TOPIC,
                key=key,
                value=value,
                on_delivery=delivery_report,
            )
        except BufferError:
            # 내부 큐가 꽉 찼을 때 잠깐 비우고 재시도
            producer.poll(0.5)
            producer.produce(
                topic=TOPIC,
                key=key,
                value=value,
                on_delivery=delivery_report,
            )
        
        # delivery callback 트리거 및 내부 큐 정리
        producer.poll(0)
        
    except Exception as e:
        print(f"[ON_MESSAGE ERROR] {e}", file=sys.stderr)


def on_error(ws, error):
    """WebSocket 에러 핸들러"""
    print(f"[WS ERROR] {error}", file=sys.stderr)


def on_close(ws, close_status_code, close_msg):
    """WebSocket 종료 핸들러"""
    print(f"[WS CLOSED] code={close_status_code}, msg={close_msg}", file=sys.stderr)


def on_open(ws):
    """WebSocket 연결 성공 핸들러"""
    print("[WS OPEN] connected to Binance stream")
    print(f"[INFO] Streaming BTC/USDT trades to Kafka topic: {TOPIC}")


def graceful_shutdown(*_):
    """Graceful shutdown 핸들러"""
    global running
    running = False
    print("\n[SHUTDOWN] closing websocket and flushing kafka...")
    try:
        producer.flush(10)
        print("[SHUTDOWN] Kafka producer flushed successfully")
    except Exception as e:
        print(f"[SHUTDOWN ERROR] {e}", file=sys.stderr)
    sys.exit(0)


def run_forever_with_reconnect():
    """WebSocket 연결을 유지하고 재연결 로직 포함"""
    backoff = 1
    max_backoff = 60
    
    while running:
        ws = WebSocketApp(
            BINANCE_WS_URL,
            on_open=on_open,
            on_message=on_message,
            on_error=on_error,
            on_close=on_close,
        )
        
        try:
            print(f"[CONNECTING] to {BINANCE_WS_URL}")
            ws.run_forever(ping_interval=20, ping_timeout=10)
        except Exception as e:
            print(f"[WS RUN ERROR] {e}", file=sys.stderr)
        
        # 끊기면 재연결
        if not running:
            break
        
        print(f"[RECONNECT] in {backoff}s...")
        time.sleep(backoff)
        backoff = min(backoff * 2, max_backoff)


if __name__ == "__main__":
    # 시그널 핸들러 등록 (Ctrl+C, 종료 신호)
    signal.signal(signal.SIGINT, graceful_shutdown)
    signal.signal(signal.SIGTERM, graceful_shutdown)
    
    print(f"[START] Kafka={BOOTSTRAP_SERVERS}, Topic={TOPIC}")
    print(f"[START] Binance WebSocket URL={BINANCE_WS_URL}")
    print("[INFO] Press Ctrl+C to stop gracefully\n")
    
    try:
        run_forever_with_reconnect()
    except KeyboardInterrupt:
        graceful_shutdown()
    except Exception as e:
        print(f"[FATAL ERROR] {e}", file=sys.stderr)
        graceful_shutdown()

