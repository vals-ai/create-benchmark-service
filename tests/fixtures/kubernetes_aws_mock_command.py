#!/usr/bin/env python3
"""Provide deterministic command responses for the AWS wrapper safety test."""

from __future__ import annotations

import os
import sys
import time
from pathlib import Path


def record() -> str:
    """Record the invoked command and return its rendered form."""
    command = f"{Path(sys.argv[0]).name} {' '.join(sys.argv[1:])}"
    with Path(os.environ["MOCK_COMMAND_LOG"]).open("a") as log:
        log.write(f"{command}\n")

    return command


def create_plan_file() -> None:
    """Create Terraform's requested plan artifact when present."""
    for argument in sys.argv[1:]:
        if argument.startswith("-out="):
            Path(argument.removeprefix("-out=")).touch()


def main() -> int:
    """Return deterministic responses for wrapper subprocesses."""
    command = record()
    name = Path(sys.argv[0]).name
    if name == "aws" and "sts get-caller-identity" in command:
        print(
            os.environ.get("MOCK_AWS_ACCOUNT", "123456789012"),
            os.environ.get("MOCK_AWS_ARN", "arn:aws:iam::123456789012:user/tester"),
            sep="\t",
        )
    elif name == "aws" and "service-quotas get-service-quota" in command:
        print("5000")
    elif name == "aws" and "ecr get-login-password" in command:
        print("password")
    elif name == "aws" and "ecr describe-images" in command:
        if "imageDetails[0].imageDigest" not in command:
            return 1
        print(f"sha256:{'0' * 64}")
    elif name == "terraform":
        create_plan_file()
        if " output " in f" {command} ":
            if "cluster_name" in command:
                print("test-eks")
            elif "image_repository_url" in command:
                print("123456789012.dkr.ecr.us-east-2.amazonaws.com/test/sandbox-images")
        elif " destroy " in f" {command} " and "workload.tfstate" in command:
            if os.environ.get("MOCK_WORKLOAD_DESTROY_FAIL") == "1":
                print("workload destroy failed", file=sys.stderr)
                return 1
    elif name == "openssl":
        print("0" * 64)
    elif name == "git":
        print("0123456789ab")
    elif name == "kubectl" and "cilium-dbg status" in command:
        print("Encryption: Wireguard")
    elif name == "kubectl" and "port-forward" in command:
        while True:
            time.sleep(1)
    elif name == "curl":
        print('{"status":"ok"}')
    elif name == "uv" and sys.argv[1:3] == ["run", "python"]:
        os.execv(sys.executable, [sys.executable, *sys.argv[3:]])

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
