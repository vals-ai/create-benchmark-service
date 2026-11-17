from __future__ import annotations

import asyncio
import socket
import threading

from benchmark_service.sandbox.ecs_nitro.agent import serve
from benchmark_service.sandbox.ecs_nitro.client import AgentClient
from benchmark_service.sandbox.ecs_nitro.config import agent_token

KEY = "k" * 64


def free_port() -> int:
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        return int(sock.getsockname()[1])


def start_agent(port: int, sandbox_id: str) -> None:
    thread = threading.Thread(
        target=serve, args=(agent_token(KEY, sandbox_id), port), kwargs={"host": "127.0.0.1"}, daemon=True
    )
    thread.start()


async def ready_agent(sandbox_id: str) -> AgentClient:
    port = free_port()
    start_agent(port, sandbox_id)
    client = AgentClient("127.0.0.1", port, agent_token(KEY, sandbox_id))
    for _ in range(100):
        if await client.healthy():
            return client
        await asyncio.sleep(0.05)
    raise AssertionError("agent did not start")
