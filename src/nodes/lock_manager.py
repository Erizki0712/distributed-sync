import asyncio
from collections import defaultdict
from typing import Dict, Set, List, Optional
from aiohttp import web
from src.utils import metrics as M
from src.consensus.raft import LEADER

class LockState:
    def __init__(self):
        self.mode: Optional[str] = None           # "shared" | "exclusive" | None
        self.holders: Set[str] = set()            # clients holding the lock
        self.wait_q: List[tuple[str, str]] = []   # [(client, mode), ...]

class LockManager:
    def __init__(self, app, node_id: str):
        self.app = app
        self.node_id = node_id
        self.locks: Dict[str, LockState] = defaultdict(LockState)
        # graph deadlock detection: client > set(key)
        self.waitfor: Dict[str, Set[str]] = defaultdict(set)
        # reverse map: client > set(key) yang sedang hold
        self.holds: Dict[str, Set[str]] = defaultdict(set)

    # helpers
    def _conflicts(self, s: LockState, mode: str, client: str) -> bool:
        if not s.mode:
            return False
        if mode == "shared" and s.mode == "shared":
            return False
        if client in s.holders and s.mode == mode == "exclusive":
            return False
        return len(s.holders - {client}) > 0

    def _would_block(self, key: str, mode: str, client: str) -> bool:
        s = self.locks[key]
        return self._conflicts(s, mode, client)

    def _detect_deadlock(self, start_client: str) -> bool:
        visited: Set[str] = set()

        def dfs(c: str) -> bool:
            if c in visited:
                return True   # cycle
            visited.add(c)
            for k in self.waitfor.get(c, set()):
                holders = self.locks[k].holders
                for h in holders:
                    if dfs(h):
                        return True
            visited.discard(c)
            return False

        return dfs(start_client)

    # API
    async def acquire(self, key: str, mode: str, client: str):
        raft = self.app["raft"]
        # forward ke leader jika follower
        if raft.state != LEADER:
            leader_id = raft.leader_id
            if leader_id:
                try:
                    res = await self.app["rpc"].post_json(
                        leader_id, "/lock/acquire",
                        {"key": key, "mode": mode, "client": client},
                        timeout=2.0
                    )
                    M.lock_acquire_total.labels(mode=mode, result="forward").inc()
                    return res
                except Exception:
                    pass
            M.lock_acquire_total.labels(mode=mode, result="redirect").inc()
            return {"ok": False, "redirect": True}

        # deadlock pre-check
        if self._would_block(key, mode, client):
            self.waitfor[client].add(key)
            if self._detect_deadlock(client):
                self.waitfor[client].discard(key)
                M.lock_acquire_total.labels(mode=mode, result="deadlock").inc()
                return {"ok": False, "deadlock": True, "reason": "cycle detected"}
            self.waitfor[client].discard(key)

        ok = await raft.propose({"op": "acquire", "key": key, "mode": mode, "client": client})
        if ok:
            M.lock_acquire_total.labels(mode=mode, result="ok").inc()
            return {"ok": True}
        else:
            M.lock_acquire_total.labels(mode=mode, result="fail").inc()
            return {"ok": False}

    async def release(self, key: str, client: str):
        raft = self.app["raft"]
        # forward ke leader jika follower
        if raft.state != LEADER:
            leader_id = raft.leader_id
            if leader_id:
                try:
                    res = await self.app["rpc"].post_json(
                        leader_id, "/lock/release",
                        {"key": key, "client": client},
                        timeout=2.0
                    )
                    M.lock_release_total.labels(result="forward").inc()
                    return res
                except Exception:
                    pass
            M.lock_release_total.labels(result="redirect").inc()
            return {"ok": False, "redirect": True}

        ok = await raft.propose({"op": "release", "key": key, "client": client})
        M.lock_release_total.labels(result="ok" if ok else "fail").inc()
        return {"ok": ok}

    # state machine
    def apply_command(self, cmd: dict):
        op = cmd["op"]
        if op == "acquire":
            key, mode, client = cmd["key"], cmd["mode"], cmd["client"]
            self._apply_acquire(key, mode, client)
        elif op == "release":
            key, client = cmd["key"], cmd["client"]
            self._apply_release(key, client)

    def _apply_acquire(self, key: str, mode: str, client: str):
        s = self.locks[key]
        if not self._conflicts(s, mode, client):
            if s.mode is None:
                s.mode = mode
            elif s.mode == "shared" and mode == "exclusive":
                s.mode = "exclusive"
            # assign holder
            s.holders.add(client)
            self.holds[client].add(key)
            M.lock_held_gauge.labels(mode=s.mode).inc()
        else:
            # queue
            s.wait_q.append((client, mode))
            self.waitfor[client].add(key)

    def _apply_release(self, key: str, client: str):
        s = self.locks[key]
        if client not in s.holders:
            return  # release non-holder

        prev_mode = s.mode
        # lepas holder
        s.holders.discard(client)
        self.holds[client].discard(key)
        if prev_mode:
            M.lock_held_gauge.labels(mode=prev_mode).dec()

        if s.holders:
            return

        s.mode = None
        granted_shared: List[str] = []
        granted_exclusive: Optional[str] = None
        new_q: List[tuple[str, str]] = []

        for c, m in s.wait_q:
            if m == "shared" and granted_exclusive is None:
                granted_shared.append(c)
            elif m == "exclusive" and not granted_shared and granted_exclusive is None:
                granted_exclusive = c
            else:
                new_q.append((c, m))

        if granted_shared:
            s.mode = "shared"
            for c in granted_shared:
                s.holders.add(c)
                self.waitfor[c].discard(key)
                self.holds[c].add(key)
            M.lock_held_gauge.labels(mode="shared").inc(len(granted_shared))
        elif granted_exclusive:
            s.mode = "exclusive"
            s.holders.add(granted_exclusive)
            self.waitfor[granted_exclusive].discard(key)
            self.holds[granted_exclusive].add(key)
            M.lock_held_gauge.labels(mode="exclusive").inc()

        s.wait_q = new_q
