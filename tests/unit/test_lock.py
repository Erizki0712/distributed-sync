import asyncio
from src.nodes.lock_manager import LockManager

class DummyApp(dict):
    pass

def test_deadlock_detection_simple():
    app = DummyApp()
    app["raft"] = type("R", (), {"state": 2, "propose": lambda self, cmd: asyncio.Future() })()
    async def ok(_self, cmd): return True
    app["raft"].propose = ok

    lm = LockManager(app, "node-1")
    lm.apply_command({"op":"acquire","key":"K1","mode":"exclusive","client":"A"})
    lm.apply_command({"op":"acquire","key":"K2","mode":"exclusive","client":"B"})
    lm.apply_command({"op":"acquire","key":"K2","mode":"exclusive","client":"A"})
    lm.apply_command({"op":"acquire","key":"K1","mode":"exclusive","client":"B"})
    assert lm._detect_deadlock("B") is True