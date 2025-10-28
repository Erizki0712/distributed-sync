import asyncio, random, time
from typing import Any, Callable, Dict, List, Optional
from aiohttp import web
from src.utils.metrics import raft_role_gauge, raft_append_entries_total, raft_request_vote_total

FOLLOWER, CANDIDATE, LEADER = 0,1,2

class LogEntry(dict):
    pass

class RaftNode:
    """
    Simplified Raft for command replication (lock manager state machine).
    """
    def __init__(self, app: web.Application, node_id: str, peers: list[str],
                 election_min_ms: int, election_max_ms: int, heartbeat_ms: int,
                 apply_fn: Callable[[dict], None]):
        self.app = app
        self.node_id = node_id
        self.peer_ids = [p.rsplit("/",1)[-1] for p in peers]
        self.peers = peers
        self.term = 0
        self.voted_for: Optional[str] = None
        self.log: List[LogEntry] = []
        self.commit_index = -1
        self.last_applied = -1
        self.next_index: Dict[str,int] = {}
        self.match_index: Dict[str,int] = {}
        self.state = FOLLOWER
        self.leader_id: Optional[str] = None
        self.apply_fn = apply_fn

        self.election_min_ms = election_min_ms
        self.election_max_ms = election_max_ms
        self.heartbeat_ms = heartbeat_ms
        self.reset_election_deadline()

        self.rpc = app["rpc"]
        self.stop = asyncio.Event()
        self.apply_lock = asyncio.Lock()

        app.router.add_post("/raft/request_vote", self.handle_request_vote)
        app.router.add_post("/raft/append_entries", self.handle_append_entries)

        # set metric untuk /metrics
        raft_role_gauge.set(FOLLOWER)

    def quorum(self) -> int:
        return (len(self.peer_ids)+1)//2 + 1

    def reset_election_deadline(self):
        self.election_deadline = time.monotonic() + (random.randint(self.election_min_ms, self.election_max_ms)/1000.0)

    async def run(self):
        while not self.stop.is_set():
            now = time.monotonic()
            if self.state in (FOLLOWER, CANDIDATE) and now >= self.election_deadline:
                await self.start_election()
            if self.state == LEADER:
                await self.send_heartbeats()
                await asyncio.sleep(self.heartbeat_ms/1000.0)
            else:
                await asyncio.sleep(0.05)
            await self.apply_committed()

    async def start_election(self):
        self.state = CANDIDATE
        raft_role_gauge.set(CANDIDATE)
        self.term += 1
        self.voted_for = self.node_id
        votes = 1
        self.leader_id = None
        self.reset_election_deadline()
        req = {
            "term": self.term,
            "candidateId": self.node_id,
            "lastLogIndex": len(self.log)-1,
            "lastLogTerm": self.log[-1]["term"] if self.log else 0
        }
        results = await self.rpc.broadcast("/raft/request_vote", req, timeout=1.5)
        granted = sum(1 for r in results if r.get("voteGranted"))
        raft_request_vote_total.labels(granted="true").inc(granted)
        raft_request_vote_total.labels(granted="false").inc(len(results)-granted)
        votes += granted
        if votes >= self.quorum():
            self.become_leader()

    def become_leader(self):
        self.state = LEADER
        self.leader_id = self.node_id
        raft_role_gauge.set(LEADER)
        for pid in self.peer_ids:
            self.next_index[pid] = len(self.log)
            self.match_index[pid] = -1

    async def send_heartbeats(self):
        base = {"term": self.term, "leaderId": self.node_id, "leaderCommit": self.commit_index}
        tasks = []
        for pid in self.peer_ids:
            prev_index = self.next_index.get(pid, len(self.log))-1
            prev_term = self.log[prev_index]["term"] if prev_index >=0 and self.log else 0
            payload = {**base, "prevLogIndex": prev_index, "prevLogTerm": prev_term, "entries": []}
            tasks.append(asyncio.create_task(self.rpc.post_json(pid, "/raft/append_entries", payload, timeout=1.0)))
        for t in asyncio.as_completed(tasks, timeout=1.1):
            try:
                await t
                raft_append_entries_total.labels(result="ok").inc()
            except Exception:
                raft_append_entries_total.labels(result="err").inc()

    async def propose(self, command: dict) -> bool:
        if self.state != LEADER:
            return False
        self.log.append(LogEntry(term=self.term, command=command))
        index = len(self.log)-1
        success = 1  # leader
        tasks = [asyncio.create_task(self._replicate_to(pid)) for pid in self.peer_ids]
        for t in asyncio.as_completed(tasks, timeout=2.0):
            try:
                ok = await t
                if ok: success += 1
                if success >= self.quorum():
                    break
            except Exception:
                pass
        if success >= self.quorum():
            self.commit_index = index
            return True
        return False

    async def _replicate_to(self, pid: str) -> bool:
        next_idx = self.next_index.get(pid, len(self.log))
        prev_index = next_idx-1
        prev_term = self.log[prev_index]["term"] if prev_index >=0 and self.log else 0
        entries = self.log[next_idx:]
        try:
            res = await self.rpc.post_json(pid, "/raft/append_entries", {
                "term": self.term,
                "leaderId": self.node_id,
                "prevLogIndex": prev_index,
                "prevLogTerm": prev_term,
                "entries": entries,
                "leaderCommit": self.commit_index,
            }, timeout=2.0)
            if res.get("success"):
                self.next_index[pid] = len(self.log)
                self.match_index[pid] = len(self.log)-1
                raft_append_entries_total.labels(result="ok").inc()
                return True
            else:
                self.next_index[pid] = max(0, next_idx-1)
                raft_append_entries_total.labels(result="conflict").inc()
                return False
        except Exception:
            raft_append_entries_total.labels(result="err").inc()
            return False

    async def apply_committed(self):
        async with self.apply_lock:
            while self.last_applied < self.commit_index:
                self.last_applied += 1
                cmd = self.log[self.last_applied]["command"]
                self.apply_fn(cmd)

    async def handle_request_vote(self, req: web.Request):
        body = await req.json()
        term = body["term"]
        candidate = body["candidateId"]
        last_idx = body["lastLogIndex"]
        last_term = body["lastLogTerm"]
        vote = False
        if term > self.term:
            self.term = term
            self.voted_for = None
            self.state = FOLLOWER
            self.leader_id = None
            raft_role_gauge.set(FOLLOWER)

        if term == self.term:
            up_to_date = (last_term > (self.log[-1]["term"] if self.log else 0)) or \
                         (last_term == (self.log[-1]["term"] if self.log else 0) and last_idx >= len(self.log)-1)
            if (self.voted_for in (None, candidate)) and up_to_date:
                self.voted_for = candidate
                vote = True
                self.reset_election_deadline()

        raft_request_vote_total.labels(granted=str(vote).lower()).inc()
        return web.json_response({"term": self.term, "voteGranted": vote})

    async def handle_append_entries(self, req: web.Request):
        body = await req.json()
        leader_term = body["term"]
        leader_id = body["leaderId"]
        prev_idx = body["prevLogIndex"]
        prev_term = body["prevLogTerm"]
        entries = body.get("entries", [])
        leader_commit = body.get("leaderCommit", -1)
        if leader_term < self.term:
            raft_append_entries_total.labels(result="stale").inc()
            return web.json_response({"term": self.term, "success": False})

        # follower state
        self.state = FOLLOWER
        self.leader_id = leader_id
        raft_role_gauge.set(FOLLOWER)
        self.term = leader_term
        self.reset_election_deadline()

        if prev_idx >= 0:
            if prev_idx >= len(self.log) or (self.log[prev_idx]["term"] != prev_term):
                return web.json_response({"term": self.term, "success": False})

        i = 0
        while i < len(entries):
            local_idx = prev_idx + 1 + i
            if local_idx < len(self.log):
                if self.log[local_idx]["term"] != entries[i]["term"]:
                    self.log = self.log[:local_idx]
                    self.log.append(LogEntry(**entries[i]))
            else:
                self.log.append(LogEntry(**entries[i]))
            i += 1

        if leader_commit > self.commit_index:
            self.commit_index = min(leader_commit, len(self.log)-1)
        return web.json_response({"term": self.term, "success": True})
