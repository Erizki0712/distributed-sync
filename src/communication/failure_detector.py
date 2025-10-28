import asyncio, time
from typing import Dict, Optional
from aiohttp import ClientSession

class FailureDetector:
    def __init__(self, peer_url_by_id: dict[str,str], heartbeat_ms: int, suspect_after_misses: int):
        self.peer_url_by_id = peer_url_by_id
        self.heartbeat_ms = heartbeat_ms
        self.suspect_after_misses = suspect_after_misses
        self.misses: Dict[str,int] = {pid:0 for pid in peer_url_by_id}
        self.suspected: Dict[str,bool] = {pid:False for pid in peer_url_by_id}
        self.denylist: set[str] = set()  # for partition simulation

    def set_partition(self, peer_id: str, blocked: bool):
        if blocked:
            self.denylist.add(peer_id)
        else:
            self.denylist.discard(peer_id)

    async def run(self, stop_event: asyncio.Event):
        async with ClientSession() as s:
            while not stop_event.is_set():
                await asyncio.gather(*[
                    self._probe(s, pid, base)
                    for pid, base in self.peer_url_by_id.items()
                ], return_exceptions=True)
                await asyncio.sleep(self.heartbeat_ms/1000.0)

    async def _probe(self, s: ClientSession, pid: str, base: str):
        if pid in self.denylist:
            self._register_miss(pid); return
        try:
            async with s.get(f"{base}/health", timeout=self.heartbeat_ms/1000.0) as r:
                if r.status == 200:
                    self.misses[pid] = 0
                    self.suspected[pid] = False
                else:
                    self._register_miss(pid)
        except Exception:
            self._register_miss(pid)

    def _register_miss(self, pid: str):
        self.misses[pid] += 1
        if self.misses[pid] >= self.suspect_after_misses:
            self.suspected[pid] = True

    def alive_peers(self) -> list[str]:
        return [pid for pid in self.peer_url_by_id if not self.suspected.get(pid, False)]

    def all_peers(self) -> list[str]:
        return list(self.peer_url_by_id.keys())