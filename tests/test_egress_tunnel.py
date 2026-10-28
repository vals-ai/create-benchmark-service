import asyncio
import contextlib
import datetime
import ipaddress
import socket
import ssl
from collections.abc import AsyncIterator
from pathlib import Path

import pytest
from cryptography import x509
from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import ec
from cryptography.x509.oid import NameOID

from benchmark_service.egress_tunnel import serve


@pytest.fixture(scope="module")
def certificate(tmp_path_factory: pytest.TempPathFactory) -> tuple[Path, Path]:
    """A self-signed certificate for 127.0.0.1, standing in for the proxy's."""
    key = ec.generate_private_key(ec.SECP256R1())
    name = x509.Name([x509.NameAttribute(NameOID.COMMON_NAME, "egress proxy")])
    now = datetime.datetime.now(datetime.UTC)
    cert = (
        x509.CertificateBuilder()
        .subject_name(name)
        .issuer_name(name)
        .public_key(key.public_key())
        .serial_number(x509.random_serial_number())
        .not_valid_before(now)
        .not_valid_after(now + datetime.timedelta(days=1))
        .add_extension(x509.SubjectAlternativeName([x509.IPAddress(ipaddress.ip_address("127.0.0.1"))]), critical=False)
        .add_extension(x509.BasicConstraints(ca=True, path_length=None), critical=True)
        .add_extension(x509.SubjectKeyIdentifier.from_public_key(key.public_key()), critical=False)
        .add_extension(x509.AuthorityKeyIdentifier.from_issuer_public_key(key.public_key()), critical=False)
        .sign(key, hashes.SHA256())
    )
    directory = tmp_path_factory.mktemp("proxy-cert")
    cert_file, key_file = directory / "proxy.crt", directory / "proxy.key"
    cert_file.write_bytes(cert.public_bytes(serialization.Encoding.PEM))
    key_file.write_bytes(
        key.private_bytes(serialization.Encoding.PEM, serialization.PrivateFormat.PKCS8, serialization.NoEncryption())
    )
    return cert_file, key_file


@contextlib.asynccontextmanager
async def echo_proxy(certificate: tuple[Path, Path]) -> AsyncIterator[int]:
    """A TLS server on 127.0.0.1 that sends back the first thing it gets; yields its port."""
    context = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
    context.load_cert_chain(*certificate)

    async def echo(reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        writer.write(await reader.read(1024))
        await writer.drain()
        writer.close()

    server = await asyncio.start_server(echo, "127.0.0.1", 0, ssl=context)
    async with server:
        yield server.sockets[0].getsockname()[1]


@contextlib.asynccontextmanager
async def tunnel(hosts: list[str], port: int, certificate: tuple[Path, Path]) -> AsyncIterator[int]:
    """Serves the tunnel on a loopback port and yields it."""
    listener = socket.create_server(("127.0.0.1", 0))
    task = asyncio.create_task(serve(listener, hosts, port, ssl.create_default_context(cafile=certificate[0])))
    try:
        yield listener.getsockname()[1]
    finally:
        task.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await task


async def send(tunnel_port: int, data: bytes) -> bytes:
    reader, writer = await asyncio.open_connection("127.0.0.1", tunnel_port)
    writer.write(data)
    await writer.drain()
    reply = await reader.read(1024)
    writer.close()
    return reply


async def test_tunnel_falls_over_to_the_next_address_and_stays_on_it(
    certificate: tuple[Path, Path], capsys: pytest.CaptureFixture[str]
) -> None:
    # Nothing listens on ::1, so the first address fails and the tunnel moves on to 127.0.0.1.
    async with echo_proxy(certificate) as port, tunnel(["::1", "127.0.0.1"], port, certificate) as tunnel_port:
        assert await send(tunnel_port, b"first") == b"first"
        assert await send(tunnel_port, b"second") == b"second"

    assert capsys.readouterr().err.count("can't reach ::1") == 1


async def test_tunnel_closes_the_client_when_no_address_answers(
    certificate: tuple[Path, Path], capsys: pytest.CaptureFixture[str]
) -> None:
    async with echo_proxy(certificate) as port, tunnel(["::1"], port, certificate) as tunnel_port:
        reader, writer = await asyncio.open_connection("127.0.0.1", tunnel_port)
        assert await reader.read(1024) == b""
        writer.close()

    assert "can't reach ::1" in capsys.readouterr().err
