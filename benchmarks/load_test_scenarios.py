from locust import HttpUser, task, between

class LockLoad(HttpUser):
    wait_time = between(0.01, 0.2)

    @task
    def lock_cycle(self):
        self.client.post("/lock/acquire", json={"key":"k1","mode":"shared","client":"c1"})
        self.client.post("/lock/release", json={"key":"k1","client":"c1"})

class QueueLoad(HttpUser):
    wait_time = between(0.01, 0.2)

    @task
    def pub_sub(self):
        self.client.post("/queue/publish", json={"topic":"t1","value":"hello"})
        r = self.client.post("/queue/consume", json={"topic":"t1","group":"g1"})
        if r.ok and r.json().get("msg_id"):
            self.client.post("/queue/ack", json={"topic":"t1","group":"g1","msg_id":r.json()["msg_id"]})

class CacheLoad(HttpUser):
    wait_time = between(0.01, 0.2)

    @task
    def rw(self):
        self.client.post("/cache/put", json={"key":"x","value":"42"})
        self.client.post("/cache/get", json={"key":"x"})