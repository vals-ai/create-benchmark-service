"""Vals Sandbox egress proxy: the only way out of a sandbox's network.

Sandboxes reach the internet only through this proxy (their VPC security group allows nothing
else), so egress policy is enforced outside the guest where sandboxed code cannot change it.
Each sandbox authenticates with its own token (``Proxy-Authorization: Basic id:token``) and gets
its own policy: allow all public addresses, deny all, or an allowlist of hostnames (``*.`` wildcards
match subdomains) and IPv4 CIDRs. Private, loopback, and link-local addresses (the VPC and the
instance metadata service) are refused unless a CIDR rule names them.

The control plane sets policies through the admin API (``PUT``/``DELETE /sandboxes/<id>`` with
``Authorization: Bearer <admin token>``). Policies are kept in memory.

    python -m benchmark_service.sandbox.vals_sandbox.egress_proxy --admin-token-env VALS_SANDBOX_PROXY_ADMIN_TOKEN
"""

from __future__ import annotations

import argparse
import asyncio
import base64
import hmac
import ipaddress
import json
import os
import socket
import time
from collections.abc import Awaitable, Callable
from dataclasses import dataclass, field
from typing import Literal
from urllib.parse import urlsplit

Mode = Literal["allow_all", "deny_all", "allowlist"]
_MAX_HEADER_BYTES = 64 * 1024
_CONNECT_TIMEOUT = 15.0
_IDLE_TIMEOUT = 600.0


@dataclass
class EgressPolicy:
    token: str
    mode: Mode = "allow_all"
    domains: list[str] = field(default_factory=list)
    cidrs: list[ipaddress.IPv4Network] = field(default_factory=list)
    expires_at: float = float("inf")

    def allows_host(self, host: str) -> bool:
        host = host.lower().rstrip(".")
        for domain in self.domains:
            domain = domain.lower().rstrip(".")
            if domain.startswith("*."):
                if host.endswith(domain[1:]):
                    return True
            elif host == domain:
                return True
        return False

    def allows_address(self, address: ipaddress.IPv4Address | ipaddress.IPv6Address, *, host_allowed: bool) -> bool:
        if self.mode == "deny_all":
            return False
        if isinstance(address, ipaddress.IPv4Address) and any(address in cidr for cidr in self.cidrs):
            return True
        if not address.is_global:
            return False
        return self.mode == "allow_all" or host_allowed


Resolver = Callable[[str, int], Awaitable[list[str]]]


async def _resolve(host: str, port: int) -> list[str]:
    infos = await asyncio.get_running_loop().getaddrinfo(host, port, type=socket.SOCK_STREAM)
    return list(dict.fromkeys(str(info[4][0]) for info in infos))


class EgressProxy:
    def __init__(self, admin_token: str, *, resolve: Resolver = _resolve) -> None:
        self._admin_token = admin_token
        self._resolve = resolve
        self.policies: dict[str, EgressPolicy] = {}
        self._tunnels: dict[str, set[asyncio.StreamWriter]] = {}

    def _drop(self, sandbox_id: str) -> None:
        """Close a sandbox's open connections, so a policy change also applies to traffic already flowing."""
        for writer in self._tunnels.pop(sandbox_id, set()):
            writer.close()

    def _expire(self) -> None:
        now = time.monotonic()
        for sandbox_id in [key for key, policy in self.policies.items() if policy.expires_at <= now]:
            del self.policies[sandbox_id]
            self._drop(sandbox_id)

    async def choose_address(self, policy: EgressPolicy, host: str, port: int) -> str | None:
        """The address to connect to, or None when the policy refuses every address the host has."""
        if policy.mode == "deny_all":
            return None
        host_allowed = policy.allows_host(host)
        try:
            addresses = [ipaddress.ip_address(host)]
        except ValueError:
            if policy.mode == "allowlist" and not host_allowed and not policy.cidrs:
                return None
            try:
                addresses = [ipaddress.ip_address(a) for a in await self._resolve(host, port)]
            except OSError:
                return None
        for address in addresses:
            if policy.allows_address(address, host_allowed=host_allowed):
                return str(address)
        return None

    def _authenticate(self, headers: dict[str, str]) -> tuple[str, EgressPolicy] | None:
        scheme, _, credentials = headers.get("proxy-authorization", "").partition(" ")
        if scheme.lower() != "basic":
            return None
        try:
            sandbox_id, _, token = base64.b64decode(credentials).decode().partition(":")
        except ValueError:
            return None
        self._expire()
        policy = self.policies.get(sandbox_id)
        if policy is None or not hmac.compare_digest(policy.token, token):
            return None
        return sandbox_id, policy

    async def handle_proxy(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        try:
            request_line, headers, raw = await _read_head(reader)
            method, target, version = request_line.split(" ", 2)
            authenticated = self._authenticate(headers)
            if authenticated is None:
                await _respond(writer, 407, extra='Proxy-Authenticate: Basic realm="vals-sandbox"\r\n')
                return
            sandbox_id, policy = authenticated
            if method == "CONNECT":
                host, _, port_text = target.rpartition(":")
                upstream_head = body = b""
            else:
                url = urlsplit(target)
                if url.scheme != "http" or not url.hostname:
                    await _respond(writer, 400)
                    return
                if "transfer-encoding" in headers:
                    await _respond(writer, 411)
                    return
                body = await reader.readexactly(int(headers.get("content-length") or 0))
                host, port_text = url.hostname, str(url.port or 80)
                path = url.path or "/"
                if url.query:
                    path += f"?{url.query}"
                # One request per connection: a reused connection must not reach this origin unparsed.
                dropped = (b"proxy-", b"connection:", b"keep-alive:")
                kept = [line for line in raw.split(b"\r\n")[1:-2] if not line.lower().startswith(dropped)]
                upstream_head = b"\r\n".join(
                    [f"{method} {path} {version}".encode(), *kept, b"Connection: close", b"", b""]
                )
            host = host.strip("[]")
            port = int(port_text)
            address = await self.choose_address(policy, host, port)
            if address is None:
                await _respond(writer, 403)
                return
            try:
                upstream_reader, upstream_writer = await asyncio.wait_for(
                    asyncio.open_connection(address, port), _CONNECT_TIMEOUT
                )
            except (OSError, TimeoutError):
                await _respond(writer, 502)
                return
            tunnels = self._tunnels.setdefault(sandbox_id, set())
            tunnels.add(writer)
            if method == "CONNECT":
                writer.write(b"HTTP/1.1 200 Connection established\r\n\r\n")
                await writer.drain()
            else:
                upstream_writer.write(upstream_head + body)
            if method == "CONNECT":
                await asyncio.gather(_pipe(reader, upstream_writer), _pipe(upstream_reader, writer))
            else:
                await _pipe(upstream_reader, writer)
                upstream_writer.close()
            tunnels.discard(writer)
        except (ValueError, asyncio.IncompleteReadError, ConnectionError):
            pass
        finally:
            writer.close()

    async def handle_admin(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        try:
            request_line, headers, _ = await _read_head(reader)
            method, target, _ = request_line.split(" ", 2)
            body = await reader.readexactly(int(headers.get("content-length") or 0))
            scheme, _, token = headers.get("authorization", "").partition(" ")
            if scheme.lower() != "bearer" or not hmac.compare_digest(token, self._admin_token):
                await _respond(writer, 401)
                return
            prefix = "/sandboxes/"
            if not target.startswith(prefix) or not target[len(prefix) :]:
                await _respond(writer, 404)
                return
            sandbox_id = target[len(prefix) :]
            if method == "PUT":
                spec = json.loads(body)
                self._expire()
                existing = self.policies.get(sandbox_id)
                self.policies[sandbox_id] = EgressPolicy(
                    token=str(spec["token"] if "token" in spec or existing is None else existing.token),
                    mode=spec.get("mode", "allow_all"),
                    domains=[str(d) for d in spec.get("domains", [])],
                    cidrs=[ipaddress.IPv4Network(c) for c in spec.get("cidrs", [])],
                    expires_at=_expiry(spec, existing),
                )
                self._drop(sandbox_id)
            elif method == "DELETE":
                self.policies.pop(sandbox_id, None)
                self._drop(sandbox_id)
            else:
                await _respond(writer, 405)
                return
            await _respond(writer, 204)
        except (ValueError, KeyError, asyncio.IncompleteReadError, ConnectionError):
            await _respond(writer, 400)
        finally:
            writer.close()


def _expiry(spec: dict[str, object], existing: EgressPolicy | None) -> float:
    ttl = spec.get("ttl_seconds")
    if isinstance(ttl, (int, float)):
        return time.monotonic() + float(ttl)
    return existing.expires_at if existing is not None else float("inf")


async def _read_head(reader: asyncio.StreamReader) -> tuple[str, dict[str, str], bytes]:
    raw = await reader.readuntil(b"\r\n\r\n")
    if len(raw) > _MAX_HEADER_BYTES:
        raise ValueError("header too large")
    lines = raw.decode("latin-1").split("\r\n")
    headers: dict[str, str] = {}
    for line in lines[1:]:
        name, sep, value = line.partition(":")
        if sep:
            headers[name.strip().lower()] = value.strip()
    return lines[0], headers, raw


async def _respond(writer: asyncio.StreamWriter, status: int, *, extra: str = "") -> None:
    reasons = {
        204: "No Content",
        400: "Bad Request",
        401: "Unauthorized",
        403: "Forbidden",
        404: "Not Found",
        411: "Length Required",
    }
    reasons |= {405: "Method Not Allowed", 407: "Proxy Authentication Required", 502: "Bad Gateway"}
    writer.write(
        f"HTTP/1.1 {status} {reasons[status]}\r\n{extra}Content-Length: 0\r\nConnection: close\r\n\r\n".encode()
    )
    try:
        await writer.drain()
    except ConnectionError:
        pass


async def _pipe(reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
    try:
        while data := await asyncio.wait_for(reader.read(64 * 1024), _IDLE_TIMEOUT):
            writer.write(data)
            await writer.drain()
    except (ConnectionError, TimeoutError):
        pass
    finally:
        writer.close()


async def serve(
    proxy: EgressProxy, *, host: str = "0.0.0.0", proxy_port: int = 3128, admin_port: int = 8081
) -> tuple[asyncio.Server, asyncio.Server]:
    return (
        await asyncio.start_server(proxy.handle_proxy, host, proxy_port),
        await asyncio.start_server(proxy.handle_admin, host, admin_port),
    )


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--admin-token-env", default="VALS_SANDBOX_PROXY_ADMIN_TOKEN")
    parser.add_argument("--proxy-port", type=int, default=3128)
    parser.add_argument("--admin-port", type=int, default=8081)
    args = parser.parse_args()

    async def run() -> None:
        proxy_server, admin_server = await serve(
            EgressProxy(os.environ[args.admin_token_env]), proxy_port=args.proxy_port, admin_port=args.admin_port
        )
        async with proxy_server, admin_server:
            await asyncio.gather(proxy_server.serve_forever(), admin_server.serve_forever())

    asyncio.run(run())


if __name__ == "__main__":
    main()
