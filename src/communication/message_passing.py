import asyncio, json, aiohttp
from aiohttp import web
from typing import Callable, Awaitable

class RPC:
    def __init__(self, app: web.Application, node_id: str, peers: list[str]):
        self.app = app
        self.node_id = node_id
        # peers as "http://host:port/node-id"
        self.peer_url_by_id = {}
        for p in peers:
            base, pid = p.rsplit("/", 1)
            self.peer_url_by_id[pid] = base
        self.session = aiohttp.ClientSession()
        app.on_shutdown.append(self._shutdown)

    async def _shutdown(self, app):
        await self.session.close()

    async def post_json(self, peer_id: str, path: str, payload: dict, timeout=2.0):
        if peer_id not in self.peer_url_by_id:
            raise RuntimeError(f"Unknown peer {peer_id}")
        url = f"{self.peer_url_by_id[peer_id]}{path}"
        async with self.session.post(url, json=payload, timeout=timeout) as resp:
            resp.raise_for_status()
            return await resp.json()

    async def broadcast(self, path: str, payload: dict, timeout=2.0, quorum: int | None=None):
        tasks = []
        for pid,_ in self.peer_url_by_id.items():
            if pid == self.node_id:
                continue
            tasks.append(asyncio.create_task(self.post_json(pid, path, payload, timeout)))
        if not tasks:
            return []
        results = []
        for coro in asyncio.as_completed(tasks, timeout=timeout+0.5):
            try:
                res = await coro
                results.append(res)
                if quorum and len([r for r in results if r.get("ok")]) >= quorum:
                    return results
            except Exception:
                results.append({"ok": False})
        return results

def route(app: web.Application, method: str, path: str):
    def decorator(fn: Callable[[web.Request], Awaitable[web.StreamResponse]]):
        app.router.add_route(method, path, fn)
        return fn
    return decorator