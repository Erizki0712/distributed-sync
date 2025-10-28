import os
from typing import List
from pydantic import BaseModel

def _parse_peers(raw: str) -> List[str]:
    if not raw:
        return []
    out: List[str] = []
    for part in raw.replace("\n", ",").split(","):
        p = part.strip()
        if p:
            out.append(p)
    return out

class Settings(BaseModel):
    # core
    node_id: str = os.getenv("NODE_ID", "node-1")
    rpc_host: str = os.getenv("RPC_HOST", "0.0.0.0")
    rpc_port: int = int(os.getenv("RPC_PORT", "8101"))
    peers: List[str] = []
    redis_url: str = os.getenv("REDIS_URL", "redis://localhost:6379/0")

    # raft timing
    raft_election_min_ms: int = int(os.getenv("RAFT_ELECTION_MIN_MS", "800"))
    raft_election_max_ms: int = int(os.getenv("RAFT_ELECTION_MAX_MS", "1400"))
    raft_heartbeat_ms: int = int(os.getenv("RAFT_HEARTBEAT_MS", "250"))

    # queue
    queue_num_vnodes: int = int(os.getenv("QUEUE_NUM_VNODES", "64"))
    queue_visibility_timeout_ms: int = int(os.getenv("QUEUE_VISIBILITY_TIMEOUT_MS", "15000"))

    # cache
    cache_max_items: int = int(os.getenv("CACHE_MAX_ITEMS", "500"))
    cache_backing_prefix: str = os.getenv("CACHE_BACKING_PREFIX", "kv:")
    cache_dir_prefix: str = os.getenv("CACHE_DIR_PREFIX", "cache:dir:")
    cache_pubsub_prefix: str = os.getenv("CACHE_PUBSUB_PREFIX", "cache:inval:")

    # failure detector
    fd_heartbeat_ms: int = int(os.getenv("FD_HEARTBEAT_MS", "1000"))
    fd_suspect_after_misses: int = int(os.getenv("FD_SUSPECT_AFTER_MISSES", "5"))

    # metrics
    metrics_port: int = int(os.getenv("METRICS_PORT", "9100"))

SETTINGS = Settings()

_env_peers = os.getenv("PEERS", "").strip()
if _env_peers:
    SETTINGS.peers = _parse_peers(_env_peers)
elif isinstance(SETTINGS.peers, str):
    SETTINGS.peers = _parse_peers(SETTINGS.peers)
