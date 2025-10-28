import asyncio, os
from aiohttp import web
from src.nodes.base_node import create_app

async def test_health(aiohttp_client):
    app = await create_app()
    client = await aiohttp_client(app)
    r = await client.get("/health")
    assert r.status == 200