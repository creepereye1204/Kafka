# app/kafka_client.py
import json
import atexit
import threading
from typing import Optional, Dict, Any, Iterable, Tuple

from confluent_kafka import Producer
from django.conf import settings

_producer: Optional[Producer] = None
_lock = threading.Lock()


def _delivery(err, msg):
    if err:
        print(f"[KAFKA][ERROR] {err} topic={msg.topic() if msg else None}")
    else:
        print(
            f"[KAFKA][OK] {msg.topic()}[{msg.partition()}]@{msg.offset()} key={msg.key()}")


def get_producer() -> Producer:
    global _producer
    if _producer is None:
        with _lock:
            if _producer is None:
                conf = {
                    "bootstrap.servers": settings.KAFKA_BOOTSTRAP_SERVERS,
                    **getattr(settings, "KAFKA_PRODUCER_CONFIG", {}),
                }
                _producer = Producer(conf)
                atexit.register(lambda: _producer.flush(5))
    return _producer


def send(topic: str, key: Optional[str], value: Dict[str, Any], headers: Optional[Dict[str, Any]] = None):
    p = get_producer()
    hdrs: Optional[Iterable[Tuple[str, bytes]]] = None
    if headers:
        hdrs = [(k, str(v).encode("utf-8")) for k, v in headers.items()]

    p.produce(
        topic=topic,
        key=key.encode("utf-8") if isinstance(key, str) else key,
        value=json.dumps(value, ensure_ascii=False).encode("utf-8"),
        headers=hdrs,
        on_delivery=_delivery,
    )
    # 콜백을 트리거하기 위해 이벤트 루프 한 번 깨우기
    p.poll(0)
