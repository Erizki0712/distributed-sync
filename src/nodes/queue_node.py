import asyncio
import json
import time
from typing import Any, Dict, Optional
from uuid import uuid4

import redis.asyncio as redis

from src.utils.metrics import (
    queue_publish_total,
    queue_consume_total,
    queue_redeliver_total,
    queue_inflight,   # ← gauge baru (tanpa _gauge)
)


class QueueNode:
    def __init__(self, app, settings):
        self.app = app
        self.r: redis.Redis = app["redis"]
        self.visibility_ms: int = settings.queue_visibility_timeout_ms
        self.num_vnodes: int = settings.queue_num_vnodes

    # redis key helpers
    @staticmethod
    def _k_queue(topic: str) -> str:
        return f"queue:{topic}"

    @staticmethod
    def _k_msg(msg_id: str) -> str:
        return f"queue:msg:{msg_id}"

    @staticmethod
    def _k_inflight(topic: str, group: str) -> str:
        return f"queue:{topic}:{group}:inflight"

    @staticmethod
    def _now_ms() -> int:
        return int(time.time() * 1000)

    # consistent hashing
    def _slot(self, routing_key: Optional[str]) -> int:
        if not routing_key:
            return 0
        return abs(hash(routing_key)) % max(1, self.num_vnodes)

    # API
    async def publish(self, topic: str, value: Any, routing_key: Optional[str] = None) -> bool:
        msg_id = str(uuid4())
        created = self._now_ms()
        slot = self._slot(routing_key)

        pipe = self.r.pipeline()
        pipe.hset(self._k_msg(msg_id), mapping={
            "value": json.dumps(value),
            "topic": topic,
            "created_ms": created,
            "slot": slot,
        })
        pipe.rpush(self._k_queue(topic), msg_id)
        await pipe.execute()

        queue_publish_total.labels(topic=topic).inc()
        return True

    async def consume(self, topic: str, group: str, timeout_ms: int = 1000) -> Dict[str, Any]:
        block_secs = max(0, int(timeout_ms // 1000))
        res = await self.r.blpop(self._k_queue(topic), timeout=block_secs)
        if not res:
            queue_consume_total.labels(topic=topic, result="timeout").inc()
            return {}

        _, msg_id = res
        if isinstance(msg_id, bytes):
            msg_id = msg_id.decode("utf-8")

        expire_ms = self._now_ms() + self.visibility_ms
        await self.r.zadd(self._k_inflight(topic, group), {msg_id: expire_ms})
        queue_inflight.labels(topic=topic).inc()

        data = await self.r.hgetall(self._k_msg(msg_id))
        raw_val = data.get("value")
        try:
            value = json.loads(raw_val) if raw_val is not None else None
        except Exception:
            value = raw_val

        queue_consume_total.labels(topic=topic, result="ok").inc()
        return {"msg_id": msg_id, "value": value}

    async def ack(self, topic: str, group: str, msg_id: str) -> bool:
        removed = await self.r.zrem(self._k_inflight(topic, group), msg_id)
        if removed:
            queue_inflight.labels(topic=topic).dec()
        await self.r.delete(self._k_msg(msg_id))
        return bool(removed)

    async def run_requeue_loop(self, stop_event: asyncio.Event):
        interval = 1.0
        scan_pattern = "queue:*:*:inflight"

        while not stop_event.is_set():
            now = self._now_ms()
            try:
                async for zk in self.r.scan_iter(match=scan_pattern, count=200):
                    key = zk.decode() if isinstance(zk, bytes) else zk
                    # key format: queue:{topic}:{group}:inflight
                    try:
                        _, topic, group, _ = key.split(":", 3)
                    except ValueError:
                        continue

                    expired_ids = await self.r.zrangebyscore(key, min=0, max=now)
                    if not expired_ids:
                        continue

                    pipe = self.r.pipeline()
                    for mid in expired_ids:
                        if isinstance(mid, bytes):
                            mid = mid.decode("utf-8")
                        pipe.zrem(key, mid)
                        pipe.rpush(self._k_queue(topic), mid)
                        queue_inflight.labels(topic=topic).dec()
                        queue_redeliver_total.labels(topic=topic).inc()
                    await pipe.execute()
            except Exception:
                pass

            try:
                await asyncio.wait_for(stop_event.wait(), timeout=interval)
            except asyncio.TimeoutError:
                pass
