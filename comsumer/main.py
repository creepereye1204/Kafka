import os
import json
import asyncio
import random
import datetime as dt
from typing import Optional, List

from fastapi import FastAPI
from aiokafka import AIOKafkaConsumer
from contextlib import asynccontextmanager
from pydantic import BaseModel, EmailStr, Field, ValidationError

BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
TOPIC = os.getenv("KAFKA_TOPIC", "user-profile")
GROUP_ID = os.getenv("KAFKA_GROUP_ID", "user-profile-consumer")
FAILURE_RATE = float(os.getenv("FAILURE_RATE", "0.3"))
FAIL_MODE = os.getenv("FAIL_MODE", "exception")
MAX_RETRIES = int(os.getenv("MAX_RETRIES", "5"))
BASE_BACKOFF = float(os.getenv("BASE_BACKOFF", "0.3"))


class UserProfile(BaseModel):
    user_id: str
    username: str = Field(..., min_length=2, max_length=50)
    email: EmailStr
    age: Optional[int] = Field(None, ge=0, le=150)
    interests: List[str] = []
    is_active: bool = True
    created_at: dt.datetime
    updated_at: Optional[dt.datetime] = None


consumer_task: Optional[asyncio.Task] = None


def maybe_fail():
    if random.random() < FAILURE_RATE:
        if FAIL_MODE == "crash":
            print("[FAIL-INJECT] crash: exiting process without commit")
            os._exit(2)
        else:
            print("[FAIL-INJECT] exception: raising error without commit")
            raise RuntimeError("Injected failure")


async def process(profile: UserProfile, meta: dict):
    maybe_fail()
    print(
        f"[CONSUMED] {meta['topic']}[{meta['partition']}]@{meta['offset']} "
        f"key={meta['key']} username={profile.username} email={profile.email} is_active={profile.is_active} "
        f"schema={meta.get('schema')} headers={meta.get('headers')}"
    )


async def consume_loop():
    consumer = AIOKafkaConsumer(
        TOPIC,
        bootstrap_servers=BOOTSTRAP,
        group_id=GROUP_ID,
        enable_auto_commit=False,
        auto_offset_reset="earliest",
        session_timeout_ms=45000,
        max_poll_interval_ms=300000,
    )
    await consumer.start()
    try:
        async for msg in consumer:
            raw = msg.value
            key = msg.key.decode("utf-8") if msg.key else None
            headers = {k: (v.decode("utf-8") if isinstance(v, (bytes, bytearray)) else v)
                       for k, v in (msg.headers or [])}
            schema = headers.get("schema")
            meta = {
                "topic": msg.topic,
                "partition": msg.partition,
                "offset": msg.offset,
                "key": key,
                "schema": schema,
                "headers": headers,
            }
            try:
                data = json.loads(raw.decode("utf-8"))
                profile = UserProfile(**data)
            except (json.JSONDecodeError, ValidationError) as e:
                print(f"[INVALID] {meta} error={e}")
                await consumer.commit()
                continue
            attempt = 0
            while True:
                try:
                    await process(profile, meta)
                    await consumer.commit()
                    break
                except Exception as e:
                    attempt += 1
                    if attempt <= MAX_RETRIES:
                        backoff = BASE_BACKOFF * (2 ** (attempt - 1))
                        print(
                            f"[RETRY] attempt={attempt}/{MAX_RETRIES} backoff={backoff:.2f}s error={e}")
                        await asyncio.sleep(backoff)
                        continue
                    else:
                        print(
                            f"[FAILED] not committed; will be reprocessed on restart error={e}")
                        break
    finally:
        await consumer.stop()


@asynccontextmanager
async def lifespan(app: FastAPI):
    global consumer_task
    consumer_task = asyncio.create_task(consume_loop())
    yield
    if consumer_task:
        consumer_task.cancel()
        try:
            await consumer_task
        except asyncio.CancelledError:
            pass

app = FastAPI(lifespan=lifespan)


@app.get("/health")
async def health():
    return {"status": "ok", "topic": TOPIC, "group": GROUP_ID}
