#!/usr/bin/env python3
"""Carries the sandbox's proxy connections to the egress proxy over TLS.

Clients use the plain HTTP proxy this serves on loopback, so those that can't speak TLS to a proxy
(Python's urllib, wget, aiohttp) still work, and the proxy credential never leaves the sandbox
unencrypted. Runs in the sandbox with only the standard library, on any Python from 3.8. Exits once
it's listening, leaving a child in its own session to serve, so the exec that starts it returns.

Usage: egress_tunnel.py PROXY_HOSTS PROXY_PORT CA_FILE LISTEN_PORT PID_FILE, built by `arguments()`.

PROXY_HOSTS lists the proxy's addresses, comma-separated. Each new connection tries them in order,
starting with the one that last worked, so the sandbox keeps its egress if one address goes down.
"""

from __future__ import annotations

import asyncio
import os
import socket
import ssl
import sys
from collections.abc import Sequence

# How long to wait on one proxy address before trying the next.
_CONNECT_TIMEOUT_SECONDS = 10


def arguments(hosts: Sequence[str], port: int, ca_file: str, listen_port: int, pid_file: str) -> list[str]:
    """Builds this script's command-line arguments, so callers don't depend on their order or format."""
    return [",".join(hosts), str(port), ca_file, str(listen_port), pid_file]


async def _relay(reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
    while True:
        data = await reader.read(65536)
        if not data:
            return
        writer.write(data)
        await writer.drain()


async def serve(listener: socket.socket, hosts: list[str], port: int, context: ssl.SSLContext) -> None:
    preferred = 0

    async def connect() -> tuple[asyncio.StreamReader, asyncio.StreamWriter] | None:
        nonlocal preferred
        for index in [(preferred + offset) % len(hosts) for offset in range(len(hosts))]:
            try:
                connection = await asyncio.wait_for(
                    asyncio.open_connection(hosts[index], port, ssl=context), timeout=_CONNECT_TIMEOUT_SECONDS
                )
            except (OSError, asyncio.TimeoutError) as error:
                print(f"egress proxy tunnel: can't reach {hosts[index]}:{port}: {error!r}", file=sys.stderr, flush=True)
                continue
            preferred = index
            return connection
        return None

    async def handle(client_reader: asyncio.StreamReader, client_writer: asyncio.StreamWriter) -> None:
        connection = await connect()
        if connection is None:
            client_writer.close()
            return
        proxy_reader, proxy_writer = connection
        relays = [
            asyncio.ensure_future(_relay(client_reader, proxy_writer)),
            asyncio.ensure_future(_relay(proxy_reader, client_writer)),
        ]
        # TLS can't half-close, so either side ending closes both, or a gone client holds the proxy
        # connection open until its idle timeout.
        try:
            await asyncio.wait(relays, return_when=asyncio.FIRST_COMPLETED)
        finally:
            for relay in relays:
                relay.cancel()
            await asyncio.gather(*relays, return_exceptions=True)
            client_writer.close()
            proxy_writer.close()

    server = await asyncio.start_server(handle, sock=listener)
    async with server:
        await server.serve_forever()


def _running(pid_file: str) -> bool:
    try:
        with open(pid_file) as f:
            os.kill(int(f.read()), 0)
    except (OSError, ValueError):
        return False
    return True


def main() -> None:
    import resource

    hosts, port, ca_file, listen_port, pid_file = sys.argv[1:6]
    # A retried run stage in the same sandbox reuses the tunnel the first attempt started.
    if _running(pid_file):
        return
    context = ssl.create_default_context(cafile=ca_file)
    # Each connection holds two descriptors, and sandboxes often start with a soft limit of 1,024.
    _, hard = resource.getrlimit(resource.RLIMIT_NOFILE)
    limit = 65536 if hard == resource.RLIM_INFINITY else min(hard, 65536)
    resource.setrlimit(resource.RLIMIT_NOFILE, (limit, hard))
    listener = socket.socket()
    listener.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    listener.bind(("127.0.0.1", int(listen_port)))
    listener.listen(socket.SOMAXCONN)
    if os.fork():
        return
    os.setsid()
    log = os.open(f"{os.path.splitext(pid_file)[0]}.log", os.O_WRONLY | os.O_CREAT | os.O_APPEND, 0o644)
    os.dup2(os.open(os.devnull, os.O_RDONLY), 0)
    os.dup2(log, 1)
    os.dup2(log, 2)
    with open(pid_file, "w") as f:
        f.write(str(os.getpid()))
    asyncio.run(serve(listener, [host for host in hosts.split(",") if host], int(port), context))


if __name__ == "__main__":
    main()
