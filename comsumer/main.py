import os
import json
import uuid
import asyncio
import datetime as dt
from typing import Optional, List

from fastapi import FastAPI
from aiokafka import AIOKafkaProducer, AIOKafkaConsumer
from contextlib import asynccontextmanager

from pydantic import BaseModel, EmailStr, Field, ValidationError


BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
DEFAULT_TOPIC = "test-topic"

producer: Optional[AIOKafkaProducer] = None
consumer_task: Optional[asyncio.Task] = None


class UserProfile(BaseModel):
    user_id: str = Field(default_factory=lambda: str(uuid.uuid4()))
    username: str = Field(..., min_length=2, max_length=50)
    email: EmailStr
    age: Optional[int] = Field(None, ge=0, le=150)
    interests: List[str] = Field(default_factory=list)
    is_active: bool = True
    created_at: dt.datetime = Field(
        default_factory=lambda: dt.datetime.now(dt.timezone.utc))
    updated_at: Optional[dt.datetime] = None


def to_json_bytes(model: BaseModel) -> bytes:

    try:
        return model.model_dump_json(exclude_none=True).encode("utf-8")
    except AttributeError:
        return model.json(exclude_none=True, ensure_ascii=False).encode("utf-8")


async def start_producer():
    global producer
    producer = AIOKafkaProducer(bootstrap_servers=BOOTSTRAP)
    await producer.start()


async def stop_producer():
    global producer
    if producer:
        await producer.stop()


async def consume_loop(topic: str):
    consumer = AIOKafkaConsumer(
        topic,
        bootstrap_servers=BOOTSTRAP,
        group_id="fastapi-group",
        auto_offset_reset="earliest",
        enable_auto_commit=True,
    )
    await consumer.start()
    try:
        async for msg in consumer:
            raw = msg.value
            try:
                data = json.loads(raw.decode("utf-8"))
                profile = UserProfile(**data)
                print(
                    f"[CONSUMED] {msg.topic}:{msg.partition}@{msg.offset} "
                    f"key={msg.key.decode('utf-8') if msg.key else None} "
                    f"username={profile.username} email={profile.email} is_active={profile.is_active}"
                )
            except (json.JSONDecodeError, ValidationError) as e:
                print(
                    f"[INVALID_MESSAGE] {msg.topic}:{msg.partition}@{msg.offset} "
                    f"error={e} raw={raw}"
                )
    finally:
        await consumer.stop()


@asynccontextmanager
async def lifespan(app: FastAPI):
    await start_producer()

    await asyncio.sleep(1)
    global consumer_task
    consumer_task = asyncio.create_task(consume_loop(DEFAULT_TOPIC))
    yield
    await stop_producer()
    if consumer_task:
        consumer_task.cancel()
        try:
            await consumer_task
        except asyncio.CancelledError:
            pass


app = FastAPI(lifespan=lifespan)


@app.get("/")
async def root():
    return {"status": "ok"}


@app.post("/produce")
async def produce(profile: UserProfile, topic: str = DEFAULT_TOPIC):
    
    assert producer is not None, "Producer not ready"

    value = to_json_bytes(profile)
    key = profile.user_id.encode("utf-8")

    headers = [
        ("content-type", b"application/json"),
        ("schema", b"user-profile-v1"),
    ]

    md = await producer.send_and_wait(
        topic,
        value=value,
        key=key,
        headers=headers,
    )

    return {
        "status": "sent",
        "topic": topic,
        "partition": md.partition,
        "offset": md.offset,
        "key": profile.user_id,
    }
