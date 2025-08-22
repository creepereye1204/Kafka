import os
import json
import asyncio
import datetime as dt
from typing import Optional, List

from fastapi import FastAPI
from aiokafka import AIOKafkaConsumer
from aiokafka.structs import TopicPartition
from contextlib import asynccontextmanager
from pydantic import BaseModel, EmailStr, Field, ValidationError
import logging

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s"
)
logger = logging.getLogger("consumer")

BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
TOPIC = os.getenv("KAFKA_TOPIC", "user-profile")
GROUP_ID = os.getenv("KAFKA_GROUP_ID", "user-profile-consumer")


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


async def process(profile: UserProfile, meta: dict):
    logger.info(
        "[PROCESS] %s[%s]@%s key=%s user_id=%s username=%s email=%s is_active=%s",
        meta['topic'], meta['partition'], meta['offset'],
        meta['key'], profile.user_id, profile.username, profile.email, profile.is_active
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
    logger.info("[START] consuming topic=%s group=%s bootstrap=%s",
                TOPIC, GROUP_ID, BOOTSTRAP)
    try:
        async for msg in consumer:
            raw_text = msg.value.decode("utf-8", errors="replace")
            key = msg.key.decode(
                "utf-8", errors="replace") if msg.key else None
            headers = {
                k: (v.decode("utf-8", errors="replace")
                    if isinstance(v, (bytes, bytearray)) else v)
                for k, v in (msg.headers or [])
            }
            meta = {
                "topic": msg.topic,
                "partition": msg.partition,
                "offset": msg.offset,
                "key": key,
                "headers": headers,
                "schema": headers.get("schema"),
            }
            logger.info(
                "[RECV] %s[%s]@%s key=%s headers=%s raw=%s",
                msg.topic, msg.partition, msg.offset, key, headers, raw_text
            )

            valid_profile: Optional[UserProfile] = None
            try:
                data = json.loads(raw_text)
                valid_profile = UserProfile(**data)
                logger.info("[VALID] username=%s email=%s",
                            valid_profile.username, valid_profile.email)
            except (json.JSONDecodeError, ValidationError) as e:
                logger.warning("[INVALID] %s error=%s", meta, e)

            if valid_profile is not None:
                try:
                    await process(valid_profile, meta)
                except Exception as e:
                    logger.exception("[PROCESS-ERROR] %s", e)

            tp = TopicPartition(msg.topic, msg.partition)
            try:
                before = await consumer.committed(tp)
            except Exception:
                before = None

            try:
                await consumer.commit()
                after = await consumer.committed(tp)
                logger.info(
                    "[COMMIT] %s[%s] processed_offset=%s committed_before=%s committed_after=%s next_should_be=%s",
                    msg.topic, msg.partition, msg.offset, before, after, msg.offset + 1
                )
            except Exception as e:
                logger.exception(
                    "[COMMIT-ERROR] commit failed; continue anyway error=%s", e)
    except Exception as e:
        logger.exception("[FATAL] consume_loop crashed: %s", e)
    finally:
        await consumer.stop()
        logger.info("[STOP] consumer stopped")


@asynccontextmanager
async def lifespan(app: FastAPI):
    logger.info("[LIFE] startup begin")

    async def runner():
        try:
            await consume_loop()
        except asyncio.CancelledError:
            logger.info("[TASK] consumer cancelled")
            raise
        except Exception as e:
            logger.exception("[TASK] consumer crashed: %s", e)

    global consumer_task
    consumer_task = asyncio.create_task(runner())
    try:
        yield
    finally:
        logger.info("[LIFE] shutdown begin")
        if consumer_task:
            consumer_task.cancel()
            try:
                await consumer_task
            except asyncio.CancelledError:
                pass
        logger.info("[LIFE] shutdown done")

app = FastAPI(lifespan=lifespan)


@app.get("/health")
async def health() -> None:
    return {"status": "ok", "topic": TOPIC, "group": GROUP_ID}
