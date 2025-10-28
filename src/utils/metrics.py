from prometheus_client import Counter, Gauge

# raft
raft_role_gauge = Gauge("raft_role", "Current Raft role: 0=follower,1=candidate,2=leader")
raft_append_entries_total = Counter("raft_append_entries_total", "AppendEntries RPCs", ["result"])
raft_request_vote_total = Counter("raft_request_vote_total", "RequestVote RPCs", ["granted"])

# lock
lock_acquire_total = Counter("lock_acquire_total", "Total lock acquire attempts", ["mode", "result"])
lock_release_total = Counter("lock_release_total", "Total lock release attempts", ["result"])
lock_held_gauge = Gauge("lock_held", "Current locks held", ["mode"])

# queue
queue_publish_total = Counter("queue_publish_total", "Messages published", ["topic"])
queue_consume_total = Counter("queue_consume_total", "Messages consumed", ["topic", "result"])
queue_redeliver_total = Counter("queue_redeliver_total", "Messages redelivered", ["topic"])
queue_inflight = Gauge("queue_inflight", "Messages inflight (pending ack)", ["topic"])

# cache
cache_get_total = Counter("cache_get_total", "Cache GETs", ["hit"])  # hit=true/false
cache_put_total = Counter("cache_put_total", "Cache PUTs")
cache_inval_total = Counter("cache_inval_total", "Invalidations sent")
cache_items = Gauge("cache_items", "Items in local cache")
