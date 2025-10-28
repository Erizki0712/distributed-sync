import asyncio
from collections import OrderedDict
from typing import Any, Optional

import redis.asyncio as redis

from src.utils.metrics import (
    cache_get_total,
    cache_put_total,
    cache_inval_total,
    cache_items,
)


class CacheNode:
    def __init__(self, app, settings):
        self.app = app
        self.r: redis.Redis = app["redis"]

        # config
        self.node_id: str = settings.node_id
        self.max_items: int = settings.cache_max_items
        self.back_prefix: str = settings.cache_backing_prefix      # default kv:
        self.inval_prefix: str = settings.cache_pubsub_prefix      # default cache:inval:

        # local state
        self._store: dict[str, Any] = {}        # key > value
        self._state: dict[str, str] = {}        # key > M E S
        self._lru: OrderedDict[str, None] = OrderedDict()
        self._lock = asyncio.Lock()

    # helpers
    def _k_back(self, key: str) -> str:
        return f"{self.back_prefix}{key}"

    def _ch_inval(self, key: str) -> str:
        return f"{self.inval_prefix}{key}"

    def _lru_touch(self, key: str, is_new: bool):
        if key in self._lru:
            self._lru.move_to_end(key, last=True)
        else:
            self._lru[key] = None
            if is_new:
                cache_items.inc()

    def _lru_evict_if_needed(self):
        while len(self._lru) > self.max_items:
            k, _ = self._lru.popitem(last=False)  # LRU
            if k in self._store:
                del self._store[k]
            if k in self._state:
                del self._state[k]
            cache_items.dec()

    # API
    async def get(self, key: str) -> Optional[Any]:
        async with self._lock:
            if key in self._store and self._state.get(key) in ("S", "M", "E"):
                self._lru_touch(key, is_new=False)
                cache_get_total.labels(hit="true").inc()
                return self._store[key]

        # miss: ambil dari backing store
        raw = await self.r.get(self._k_back(key))
        if raw is None:
            cache_get_total.labels(hit="false").inc()
            return None

        # decode bytes
        if isinstance(raw, bytes):
            try:
                raw = raw.decode("utf-8")
            except Exception:
                pass

        async with self._lock:
            is_new = key not in self._store
            self._store[key] = raw
            self._state[key] = "S"
            self._lru_touch(key, is_new=is_new)
            self._lru_evict_if_needed()

        cache_get_total.labels(hit="false").inc()
        return raw

    async def put(self, key: str, value: Any) -> None:
        await self.r.set(self._k_back(key), value)

        # update local & LRU
        async with self._lock:
            is_new = key not in self._store
            self._store[key] = value
            self._state[key] = "M"
            self._lru_touch(key, is_new=is_new)
            self._lru_evict_if_needed()

        # broadcast invalidation
        try:
            await self.r.publish(self._ch_inval(key), f"writer={self.node_id}")
            cache_inval_total.inc()
        except Exception:
            pass

        cache_put_total.inc()

    # bg task
    async def run_invalidation_listener(self, stop_event: asyncio.Event):
        # subscribe ke pattern invalidation dan evict local entry kalau ada update dari node lain
        pubsub = self.r.pubsub()
        pattern = f"{self.inval_prefix}*"
        await pubsub.psubscribe(pattern)

        try:
            while not stop_event.is_set():
                msg = await pubsub.get_message(ignore_subscribe_messages=True, timeout=1.0)
                if not msg:
                    try:
                        await asyncio.wait_for(stop_event.wait(), timeout=0.05)
                    except asyncio.TimeoutError:
                        pass
                    continue

                ch = msg.get("channel")
                if isinstance(ch, bytes):
                    ch = ch.decode("utf-8")

                # channel: "<prefix><key>"
                if not ch or not ch.startswith(self.inval_prefix):
                    continue

                key = ch[len(self.inval_prefix):]

                async with self._lock:
                    removed = False
                    if key in self._store:
                        del self._store[key]
                        removed = True
                    if key in self._state:
                        del self._state[key]
                    if key in self._lru:
                        self._lru.pop(key, None)
                        if removed:
                            cache_items.dec()
                # naikkan counter di publisher; evict di subscriber
        finally:
            try:
                await pubsub.punsubscribe(pattern)
            except Exception:
                pass
            try:
                await pubsub.close()
            except Exception:
                pass
