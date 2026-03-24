import asyncio
import os
import uuid

from modal import App, Client, Image, Sandbox
from modal.exception import NotFoundError


APP_NAME = f"valkyrie-demo-{uuid.uuid4().hex[:8]}"


async def get_client() -> Client:
    """Authenticate with Modal using env-based credentials."""
    token_id = os.environ.get("MODAL_TOKEN_ID")
    token_secret = os.environ.get("MODAL_TOKEN_SECRET")
    if token_id and token_secret:
        return await Client.from_credentials.aio(token_id, token_secret)
    return await Client.from_env.aio()


async def get_or_create_app(client: Client, app_name: str) -> App:
    """
    Equivalent of Daytona's implicit client — Modal requires an App to create
    sandboxes from outside a container.  App.lookup with create_if_missing=True
    is a lightweight, idempotent gRPC call (no containers, no cost).
    """
    app = await App.lookup.aio(app_name, client=client, create_if_missing=True)
    print(f"[app] looked up / created app: {app_name}")
    return app


# ---------------------------------------------------------------------------
# 1. Create sandbox from a Docker image
#    Daytona equivalent: daytona.create(CreateSandboxFromImageParams(...))
# ---------------------------------------------------------------------------
async def create_sandbox_from_image(
    app: App,
    client: Client,
    name: str | None = None,
    env_vars: dict[str, str] | None = None,
    cpu: float = 1.0,
    memory_mb: int = 2048,
    idle_timeout: int = 300,
    block_network: bool = False,
) -> Sandbox:
    image = Image.from_registry("python:3.11-slim")

    sb = await Sandbox.create.aio(
        app=app,
        name=name,
        image=image,
        env=env_vars or {},
        cpu=cpu,
        memory=memory_mb,
        idle_timeout=idle_timeout,
        block_network=block_network,
        client=client,
    )
    # Modal doesn't have labels at creation time — set them via tags after
    if name:
        await sb.set_tags.aio({"Name": name})

    print(f"[create_from_image] sandbox={sb.object_id}  name={name}")
    return sb


# ---------------------------------------------------------------------------
# 2. Create sandbox from a snapshot (pre-built image ID)
#    Daytona equivalent: daytona.create(CreateSandboxFromSnapshotParams(...))
# ---------------------------------------------------------------------------
async def create_sandbox_from_snapshot(
    app: App,
    client: Client,
    image_id: str,
    name: str | None = None,
) -> Sandbox:
    image = Image.from_id(image_id, client=client)

    sb = await Sandbox.create.aio(
        app=app,
        name=name,
        image=image,
        client=client,
    )
    print(f"[create_from_snapshot] sandbox={sb.object_id}  image_id={image_id}")
    return sb


# ---------------------------------------------------------------------------
# 3. Get an existing sandbox by ID
#    Daytona equivalent: daytona.get(sandbox_name)
# ---------------------------------------------------------------------------
async def get_sandbox(client: Client, sandbox_id: str) -> Sandbox:
    sb = await Sandbox.from_id.aio(sandbox_id, client=client)
    print(f"[get] sandbox={sb.object_id}")
    return sb


# ---------------------------------------------------------------------------
# 4. List sandboxes (filtered by app + tags)
#    Daytona equivalent: daytona.list(labels=..., limit=10, page=page)
# ---------------------------------------------------------------------------
async def list_sandboxes(app: App, client: Client, tags: dict[str, str] | None = None) -> list[Sandbox]:
    sandboxes = []
    async for sb in Sandbox.list.aio(app_id=app.app_id, tags=tags or {}, client=client):
        sandboxes.append(sb)
    print(f"[list] found {len(sandboxes)} sandbox(es)")
    return sandboxes


# ---------------------------------------------------------------------------
# 5. Delete (terminate) a sandbox
#    Daytona equivalent: daytona.delete(sandbox)
# ---------------------------------------------------------------------------
async def delete_sandbox(sb: Sandbox) -> None:
    try:
        await sb.terminate.aio()
        print(f"[delete] terminated sandbox={sb.object_id}")
    except NotFoundError:
        print(f"[delete] sandbox already gone")


# ---------------------------------------------------------------------------
# 6. Force-stop all sandboxes for a run
#    Daytona equivalent: force_stop_sandboxes() iterating sandbox_generator()
# ---------------------------------------------------------------------------
async def force_stop_all(app: App, client: Client, tags: dict[str, str] | None = None) -> None:
    sandboxes = await list_sandboxes(app, client, tags)
    for sb in sandboxes:
        await delete_sandbox(sb)
    print(f"[force_stop] terminated {len(sandboxes)} sandbox(es)")


# ---------------------------------------------------------------------------
# 7. Execute a one-shot command and collect output
#    Daytona equivalent: sandbox.process.exec(...)
# ---------------------------------------------------------------------------
async def exec_command(sb: Sandbox, cmd: str) -> tuple[str, str, int]:
    process = await sb.exec.aio("sh", "-c", cmd)
    stdout = await process.stdout.read.aio()
    stderr = await process.stderr.read.aio()
    exit_code = await process.wait.aio()
    print(f"[exec] cmd={cmd!r}  exit_code={exit_code}")
    return stdout, stderr, exit_code


# ---------------------------------------------------------------------------
# 8. Stream command output
#    Daytona equivalent: sandbox.process.create_session + execute_session_command
#                        + get_session_command_logs_async
# ---------------------------------------------------------------------------
async def stream_command(sb: Sandbox, cmd: str) -> int:
    process = await sb.exec.aio("sh", "-c", cmd)

    async for chunk in process.stdout:
        print(f"  [stdout] {chunk}", end="")
    async for chunk in process.stderr:
        print(f"  [stderr] {chunk}", end="")

    exit_code = await process.wait.aio()
    print(f"\n[stream] cmd={cmd!r}  exit_code={exit_code}")
    return exit_code


# ---------------------------------------------------------------------------
# 9. Upload files to the sandbox
#    Daytona equivalent: sandbox.fs.upload_files(...)
# ---------------------------------------------------------------------------
async def upload_file(sb: Sandbox, remote_path: str, content: bytes) -> None:
    f = await sb.open.aio(remote_path, "wb")
    await f.write.aio(content)
    await f.close.aio()
    print(f"[upload] wrote {len(content)} bytes to {remote_path}")


# ---------------------------------------------------------------------------
# 10. Download files from the sandbox
#     Daytona equivalent: sandbox.process.exec("base64 ...") workaround
# ---------------------------------------------------------------------------
async def download_file(sb: Sandbox, remote_path: str) -> bytes:
    f = await sb.open.aio(remote_path, "rb")
    data = await f.read.aio()
    await f.close.aio()
    print(f"[download] read {len(data)} bytes from {remote_path}")
    return data


# ---------------------------------------------------------------------------
# 11. Create a folder in the sandbox
#     Daytona equivalent: sandbox.fs.create_folder(...)
# ---------------------------------------------------------------------------
async def create_folder(sb: Sandbox, path: str) -> None:
    await sb.mkdir.aio(path, parents=True)
    print(f"[mkdir] created {path}")


# ---------------------------------------------------------------------------
# 12. Snapshot a running sandbox's filesystem
#     Daytona equivalent: daytona.snapshot.create() (but Daytona does it
#     without a running sandbox; Modal requires one)
# ---------------------------------------------------------------------------
async def snapshot_sandbox(sb: Sandbox) -> str:
    image = await sb.snapshot_filesystem.aio()
    image_id = image.object_id
    print(f"[snapshot] captured image_id={image_id}")
    return image_id


# ---------------------------------------------------------------------------
# 13. Detach from sandbox (keep it running)
#     Daytona equivalent: N/A — Daytona sandboxes persist by default
# ---------------------------------------------------------------------------
async def detach_sandbox(sb: Sandbox) -> None:
    await sb.detach.aio()
    print(f"[detach] sandbox={sb.object_id} still running")


# ---------------------------------------------------------------------------
# Full end-to-end demo matching the Valkyrie task lifecycle
# ---------------------------------------------------------------------------
async def main():
    client = await get_client()
    app = await get_or_create_app(client, APP_NAME)

    sandbox_name = f"demo-task-{uuid.uuid4().hex[:8]}"
    tags = {"Benchmark": "demo-bench", "Task": "task-001"}

    # --- Lifecycle: create ---
    print("\n=== CREATE SANDBOX FROM IMAGE ===")
    sb = await create_sandbox_from_image(
        app, client, name=sandbox_name, env_vars={"PYTHONSAFEPATH": "1"}, cpu=1.0, memory_mb=2048
    )
    await sb.set_tags.aio(tags)

    # --- File ops: upload artifacts ---
    print("\n=== FILE OPERATIONS ===")
    await create_folder(sb, "/bundle/my-agent")
    await upload_file(sb, "/bundle/my-agent/run.py", b'print("hello from agent")\n')

    # --- Exec: install dependencies ---
    print("\n=== EXEC (one-shot) ===")
    stdout, stderr, code = await exec_command(sb, "pip install requests 2>&1 | tail -1")

    # --- Stream: run agent ---
    print("\n=== STREAM EXEC ===")
    await stream_command(sb, "python /bundle/my-agent/run.py")

    # --- Archive & download output ---
    print("\n=== ARCHIVE & DOWNLOAD ===")
    await exec_command(sb, "echo 'result data' > /tmp/output.txt")
    await exec_command(sb, "tar -czf /tmp/output.tar.gz -C /tmp output.txt")
    archive = await download_file(sb, "/tmp/output.tar.gz")
    print(f"  archive size: {len(archive)} bytes")

    # --- Snapshot for reuse ---
    print("\n=== SNAPSHOT FILESYSTEM ===")
    image_id = await snapshot_sandbox(sb)

    # --- Get sandbox by ID (what benchmark-service does) ---
    print("\n=== GET SANDBOX BY ID ===")
    sb_refetched = await get_sandbox(client, sb.object_id)
    stdout, _, _ = await exec_command(sb_refetched, "echo 'still alive'")
    print(f"  output: {stdout.strip()}")

    # --- List sandboxes by tags ---
    print("\n=== LIST SANDBOXES ===")
    await list_sandboxes(app, client, tags=tags)

    # --- Cleanup ---
    print("\n=== DELETE SANDBOX ===")
    await delete_sandbox(sb)

    # --- Create from snapshot (fast path) ---
    print("\n=== CREATE FROM SNAPSHOT ===")
    sb2 = await create_sandbox_from_snapshot(app, client, image_id, name=f"from-snapshot-{uuid.uuid4().hex[:8]}")
    stdout, _, _ = await exec_command(sb2, "python -c \"import requests; print('requests available')\"")
    print(f"  output: {stdout.strip()}")
    await delete_sandbox(sb2)

    # --- Force-stop all remaining ---
    print("\n=== FORCE STOP ALL ===")
    await force_stop_all(app, client)

    print("\n=== DONE ===")


if __name__ == "__main__":
    asyncio.run(main())
