import asyncio, json, os
from aiohttp import web
import redis.asyncio as redis
from src.utils.config import SETTINGS
from src.communication.message_passing import RPC
from src.communication.failure_detector import FailureDetector
from src.consensus.raft import RaftNode, LEADER
from src.utils import metrics as M

def _route(app: web.Application, method: str, path: str):
    def decorator(fn):
        app.router.add_route(method, path, fn)
        return fn
    return decorator

async def create_app():
    app = web.Application()

    # debug peers
    print(f"[BOOT] NODE_ID={SETTINGS.node_id} RPC_PORT={SETTINGS.rpc_port}")
    print(f"[BOOT] PEERS_RAW={os.getenv('PEERS')}")
    # fallback peers
    if not SETTINGS.peers:
        raw = os.getenv("PEERS", "")
        SETTINGS.peers = [p.strip() for p in raw.replace("\n", ",").split(",") if p.strip()]
    print(f"[BOOT] PEERS={SETTINGS.peers}")

    # redis & subsystems
    app["redis"] = redis.from_url(SETTINGS.redis_url, decode_responses=True)
    app["rpc"] = RPC(app, SETTINGS.node_id, SETTINGS.peers)
    app["fd"] = FailureDetector(app["rpc"].peer_url_by_id, SETTINGS.fd_heartbeat_ms, SETTINGS.fd_suspect_after_misses)

    from src.nodes.lock_manager import LockManager
    from src.nodes.queue_node import QueueNode
    from src.nodes.cache_node import CacheNode

    lm = LockManager(app, SETTINGS.node_id); app["lock_manager"] = lm
    raft = RaftNode(app, SETTINGS.node_id, SETTINGS.peers,
                    SETTINGS.raft_election_min_ms, SETTINGS.raft_election_max_ms,
                    SETTINGS.raft_heartbeat_ms, apply_fn=lm.apply_command)
    app["raft"] = raft
    qn = QueueNode(app, SETTINGS); app["queue"] = qn
    cn = CacheNode(app, SETTINGS); app["cache"] = cn

    @_route(app, "GET", "/health")
    async def health(req):
        return web.json_response({"ok": True, "node": SETTINGS.node_id})

    @_route(app, "GET", "/metrics")
    async def metrics(req):
        from prometheus_client import generate_latest, CONTENT_TYPE_LATEST
        data = generate_latest()
        return web.Response(body=data, headers={"Content-Type": CONTENT_TYPE_LATEST})

    # raft helpers
    @_route(app, "GET", "/raft/status")
    async def raft_status(req):
        state_map = {0:"FOLLOWER",1:"CANDIDATE",2:"LEADER"}
        r: RaftNode = app["raft"]
        return web.json_response({
            "node": SETTINGS.node_id,
            "term": r.term,
            "state": {"code": r.state, "name": state_map.get(r.state, "UNKNOWN")},
            "leader_id": r.leader_id,
            "commit_index": r.commit_index,
            "last_applied": r.last_applied,
            "log_len": len(r.log)
        })

    @_route(app, "GET", "/raft/leader")
    async def raft_leader(req):
        r: RaftNode = app["raft"]
        return web.json_response({"leader_id": r.leader_id, "node": SETTINGS.node_id})

    @_route(app, "POST", "/raft/start_election")
    async def raft_start_election(req):
        asyncio.create_task(app["raft"].start_election())
        return web.json_response({"ok": True})

    # lock endpoints
    @_route(app, "POST", "/lock/acquire")
    async def lock_acquire(req):
        body = await req.json()
        key = body["key"]; mode = body["mode"]; client = body["client"]
        res = await lm.acquire(key, mode, client)
        return web.json_response(res)

    @_route(app, "POST", "/lock/release")
    async def lock_release(req):
        body = await req.json()
        key = body["key"]; client = body["client"]
        res = await lm.release(key, client)
        return web.json_response(res)

    # queue
    @_route(app, "POST", "/queue/publish")
    async def queue_publish(req):
        b = await req.json()
        topic = b["topic"]; value = b["value"]; key = b.get("key")
        res = await qn.publish(topic, value, key)
        return web.json_response({"ok": res})

    @_route(app, "POST", "/queue/consume")
    async def queue_consume(req):
        b = await req.json()
        topic = b["topic"]; group = b["group"]; timeout_ms = b.get("timeout_ms", 1000)
        res = await qn.consume(topic, group, timeout_ms)
        return web.json_response(res)

    @_route(app, "POST", "/queue/ack")
    async def queue_ack(req):
        b = await req.json()
        topic = b["topic"]; group = b["group"]; msg_id = b["msg_id"]
        ok = await qn.ack(topic, group, msg_id)
        return web.json_response({"ok": ok})

    # cache
    @_route(app, "POST", "/cache/get")
    async def cache_get(req):
        b = await req.json()
        key = b["key"]
        v = await cn.get(key)
        return web.json_response({"value": v})

    @_route(app, "POST", "/cache/put")
    async def cache_put(req):
        b = await req.json()
        key = b["key"]; value = b["value"]
        await cn.put(key, value)
        return web.json_response({"ok": True})

    return app

async def _run_background(app: web.Application):
    raft: RaftNode = app["raft"]
    fd: FailureDetector = app["fd"]
    qn = app["queue"]; cn = app["cache"]

    stop = asyncio.Event()
    tasks = [
        asyncio.create_task(raft.run()),
        asyncio.create_task(fd.run(stop)),
        asyncio.create_task(qn.run_requeue_loop(stop)),
        asyncio.create_task(cn.run_invalidation_listener(stop)),
    ]
    return stop, tasks

def main():
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    app = loop.run_until_complete(create_app())
    loop.run_until_complete(_start(app))

async def _start(app):
    stop, tasks = await _run_background(app)
    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, SETTINGS.rpc_host, SETTINGS.rpc_port)
    await site.start()
    print(f"Node {SETTINGS.node_id} listening on {SETTINGS.rpc_port}")
    try:
        while True:
            await asyncio.sleep(3600)
    except (KeyboardInterrupt, SystemExit):
        stop.set()
        await asyncio.gather(*tasks, return_exceptions=True)

if __name__ == "__main__":
    main()
