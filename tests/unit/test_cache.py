from collections import OrderedDict
from src.nodes.cache_node import CacheNode

def test_lru_evict():
    class S: cache_max_items=2; node_id="n1"; cache_dir_prefix="d:"; cache_backing_prefix="kv:"; cache_pubsub_prefix="ps:"
    class A(dict): pass
    cn = CacheNode(A(), S())
    cn._lru_put("a","1")
    cn._lru_put("b","2")
    cn._lru_put("c","3")
    assert "a" not in cn.cache and len(cn.cache)==2