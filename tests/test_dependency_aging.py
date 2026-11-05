import functools
import hashlib
import http.server
import json
import subprocess
import threading
import zipfile
from datetime import UTC, datetime, timedelta
from pathlib import Path

import pytest

from cli.generator import generate_project


@pytest.mark.parametrize("project", ["root", "default", "vals-ai"])
def test_project_resolution_ages_external_packages_and_trusts_certifi(tmp_path: Path, project: str) -> None:
    root = Path(__file__).resolve().parents[1]
    project_dir = root
    if project != "root":
        project_dir = tmp_path / "generated"
        generate_project("aging-probe", project_dir, template=project)

    packages: dict[str, list[dict[str, object]]] = {}
    now = datetime.now(UTC)
    for package in ("aging_probe", "certifi"):
        files: list[dict[str, object]] = []
        for version, age in (("1.0.0", 8), ("2.0.0", 0)):
            filename = f"{package}-{version}-py3-none-any.whl"
            wheel = tmp_path / filename
            with zipfile.ZipFile(wheel, "w") as archive:
                metadata = f"{package}-{version}.dist-info"
                archive.writestr(
                    f"{metadata}/METADATA",
                    f"Metadata-Version: 2.1\nName: {package.replace('_', '-')}\nVersion: {version}\n",
                )
                archive.writestr(
                    f"{metadata}/WHEEL",
                    "Wheel-Version: 1.0\nRoot-Is-Purelib: true\nTag: py3-none-any\n",
                )
                archive.writestr(f"{metadata}/RECORD", "")
            files.append(
                {
                    "filename": filename,
                    "url": f"/{filename}",
                    "hashes": {"sha256": hashlib.sha256(wheel.read_bytes()).hexdigest()},
                    "upload-time": (now - timedelta(days=age)).isoformat().replace("+00:00", "Z"),
                }
            )
        packages[package.replace("_", "-")] = files

    class IndexHandler(http.server.SimpleHTTPRequestHandler):
        def do_GET(self) -> None:
            name = self.path.rstrip("/").split("/")[-1]
            if name in packages:
                body = json.dumps({"meta": {"api-version": "1.1"}, "name": name, "files": packages[name]}).encode()
                self.send_response(200)
                self.send_header("Content-Type", "application/vnd.pypi.simple.v1+json")
                self.end_headers()
                self.wfile.write(body)
            else:
                super().do_GET()

    with http.server.ThreadingHTTPServer(
        ("127.0.0.1", 0), functools.partial(IndexHandler, directory=str(tmp_path))
    ) as server:
        thread = threading.Thread(target=server.serve_forever, daemon=True)
        thread.start()
        try:
            command = [
                "uv", "pip", "compile", "--no-deps", "--no-cache", "--index-url",
                f"http://127.0.0.1:{server.server_port}/simple", "-",
            ]
            for control in (False, True):
                result = subprocess.run(
                    command + (["--no-config"] if control else []),
                    cwd=project_dir,
                    input="aging-probe\ncertifi\n",
                    text=True,
                    capture_output=True,
                    check=True,
                )
                assert f"aging-probe=={'2.0.0' if control else '1.0.0'}" in result.stdout
                assert "certifi==2.0.0" in result.stdout
        finally:
            server.shutdown()
            thread.join()
