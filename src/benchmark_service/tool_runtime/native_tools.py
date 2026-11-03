"""Run the benchmark's native research tools outside the model's shell."""

from __future__ import annotations

import json
import logging
import os
import re
import stat
import sys
from collections.abc import Mapping
from importlib import import_module
from pathlib import Path
from tempfile import TemporaryDirectory
from typing import TYPE_CHECKING, Any
from urllib.parse import urlsplit

if TYPE_CHECKING:
    from .native_tool_mcp import NativeTool as Tool


class WorkbookExecute:
    name = "execute"

    def __init__(self, tool: Tool, workbook: Path, private_dir: Path):
        if not workbook.is_absolute() or workbook.parent.resolve() != workbook.parent:
            raise ValueError("The submission workbook needs an absolute, non-symlink directory.")
        self.tool = tool
        self.workbook = workbook
        self.private_dir = private_dir
        self.description = tool.description
        self.parameters = tool.parameters
        self.required = tool.required

    async def execute(self, args: dict[str, Any], state: dict[str, Any], logger: logging.Logger) -> Any:
        if args.get("file", str(self.workbook)) not in {str(self.workbook), self.workbook.name}:
            raise ValueError("Only the submission workbook can be recalculated.")
        descriptor = os.open(self.workbook, os.O_RDWR | os.O_NOFOLLOW)
        with os.fdopen(descriptor, "r+b") as source:
            original = os.fstat(source.fileno())
            if not stat.S_ISREG(original.st_mode) or original.st_nlink != 1:
                raise ValueError("The submission workbook must be one regular file.")
            # The model cannot replace a private copy while the native tool recalculates it.
            with TemporaryDirectory(dir=self.private_dir) as temporary:
                workbook = Path(temporary) / "submission.xlsx"
                workbook.write_bytes(source.read())
                result = await self.tool.execute({**args, "file": str(workbook)}, state, logger)
                current = self.workbook.stat(follow_symlinks=False)
                if (current.st_dev, current.st_ino) != (original.st_dev, original.st_ino):
                    raise ValueError("The submission workbook changed during recalculation.")
                source.seek(0)
                source.write(workbook.read_bytes())
                source.truncate()
                return result


class WorkbookSubmit:
    name = "submit"

    def __init__(self, tool: Tool, workbook: Path, private_dir: Path):
        if not workbook.is_absolute() or workbook.parent.resolve() != workbook.parent:
            raise ValueError("The submission workbook needs an absolute, non-symlink directory.")
        self.tool = tool
        self.workbook = workbook
        self.private_dir = private_dir
        self.description = tool.description
        self.parameters = tool.parameters
        self.required = tool.required

    async def execute(self, args: dict[str, Any], state: dict[str, Any], logger: logging.Logger) -> Any:
        descriptor = os.open(self.workbook, os.O_RDONLY | os.O_NOFOLLOW)
        with os.fdopen(descriptor, "rb") as source:
            original = os.fstat(source.fileno())
            if not stat.S_ISREG(original.st_mode) or original.st_nlink != 1:
                raise ValueError("The submission workbook must be one regular file.")
            with TemporaryDirectory(dir=self.private_dir) as temporary:
                snapshot = Path(temporary) / "submission.xlsx"
                snapshot.write_bytes(source.read())
                result = await self.tool.execute(args, state, logger)
                if result.error is None and result.done:
                    snapshot.chmod(0o400)
                    snapshot.replace(self.private_dir / "submission.xlsx")
                return result


def build_tools(benchmark: str, model: str, as_of_date: str, workbook: Path, private_dir: Path) -> list[Tool]:
    match benchmark:
        case "fabv2-internal":
            module_name = "finance_agent.tools"
            names = (
                "TavilyWebSearch",
                "RetrieveInformation",
                "ParseHtmlPage",
                "EDGARSearch",
                "Calculator",
                "PriceHistory",
                "SubmitFinalResult",
            )
        case "legal-research":
            module_name = "legal_agent.tools"
            names = (
                "TavilyWebSearch",
                "RetrieveInformation",
                "ParseHtmlPage",
                "CourtListenerSearch",
                "SubmitFinalResult",
            )
        case "tax-agent":
            for variable in ("TAX_AGENT_CFR_CORPUS", "TAX_AGENT_USC_CORPUS"):
                if not Path(os.environ[variable]).is_dir():
                    raise ValueError(f"{variable} must point to the pinned authority corpus.")
            module_name = "tax_agent.tools"
            names = (
                "TavilyWebSearch",
                "RetrieveInformation",
                "FetchDocument",
                "LookupAuthority",
                "Calculator",
                "SubmitFinalResult",
            )
        case "emb":
            module_name = "emb_agent.cli"
            names = ()
        case _:
            raise ValueError(f"Unsupported research benchmark: {benchmark}")

    provider, model_name = model.split("/", 1)
    llm = import_module("model_library.base.gateway").GatewayLLM(model_name, provider)
    module = import_module(module_name)
    tools: list[Tool] = []
    if benchmark == "emb":
        module._verify_graph_startup()
        for tool in module._build_tools(llm):
            if tool.name == "bash":
                continue
            if tool.name == "execute":
                tool = WorkbookExecute(tool, workbook, private_dir)
            elif tool.name == "submit":
                tool = WorkbookSubmit(tool, workbook, private_dir)
            tools.append(tool)
        return tools

    for name in names:
        constructor = getattr(module, name)
        if name == "RetrieveInformation":
            tool = constructor(llm=llm)
        elif benchmark == "tax-agent" and name in {"TavilyWebSearch", "LookupAuthority"}:
            tool = constructor(as_of_date=as_of_date)
        else:
            tool = constructor()
        tools.append(tool)
    return tools


def render_instructions(benchmark: str, task_id: str, question: str, as_of_date: str) -> tuple[str, str]:
    match benchmark:
        case "fabv2-internal" | "tax-agent":
            package = "finance_agent" if benchmark == "fabv2-internal" else "tax_agent"
            prompt = import_module(f"{package}.prompt")
            user = prompt.QUESTION_PROMPT.format(question=question)
            if benchmark == "tax-agent" and as_of_date != import_module("tax_agent.tools").DEFAULT_AS_OF_DATE:
                user = f"Answer this question as of {as_of_date}.\n\n{user}"
            return prompt.SYSTEM_PROMPT, user
        case "legal-research":
            prompt = import_module("legal_agent.prompt").instructions_prompt("tool_loop")
            return "", prompt.format(question=question)
        case "emb":
            return import_module("emb_agent.cli")._read_system_prompt(task_id), question
        case _:
            raise ValueError(f"Unsupported research benchmark: {benchmark}")


def _redaction_secrets(environment: Mapping[str, str]) -> list[str]:
    """Select secrets from the tool environment, which also includes public settings."""
    secrets = [
        value
        for name, value in environment.items()
        if value and re.search(r"KEY|TOKEN|SECRET|PASSWORD|PASSWD|CREDENTIAL|COOKIE", name, re.I)
    ]
    for name in ("HTTP_PROXY", "HTTPS_PROXY", "ALL_PROXY", "http_proxy", "https_proxy", "all_proxy"):
        value = environment.get(name, "")
        if value:
            secrets.append(value)
            if password := urlsplit(value).password:
                secrets.append(password)
    return secrets


def main() -> None:
    from aiohttp import web

    sys.path.insert(0, str(Path(__file__).resolve().parent))
    create_tool_mcp_app = import_module("native_tool_mcp").create_tool_mcp_app

    config = json.loads(Path(sys.argv[1]).read_text())
    os.environ.update(config["credentials"])
    os.environ["NO_PROXY"] = os.environ["no_proxy"] = ""
    import_module("model_library").model_library_settings.set(
        MODEL_GATEWAY_URL=os.environ["MODEL_GATEWAY_URL"],
        MODEL_GATEWAY_API_KEY=os.environ["MODEL_GATEWAY_API_KEY"],
    )
    secret_values = _redaction_secrets(os.environ)
    private_dir = Path(config["output_dir"])
    private_dir.mkdir(parents=True, exist_ok=True)
    instructions, prompt = render_instructions(
        config["benchmark"], config["task_id"], config["question"], config["as_of_date"]
    )
    (private_dir / "instructions.txt").write_text(instructions)
    (private_dir / "prompt.txt").write_text(prompt)
    tools = build_tools(
        config["benchmark"],
        config["model"],
        config["as_of_date"],
        Path(config["workbook_path"]),
        private_dir,
    )
    app = create_tool_mcp_app(
        tools,
        config["task_id"],
        config["model"],
        private_dir,
        secret_values,
    )
    web.run_app(app, host="127.0.0.1", port=config["port"], print=None)


if __name__ == "__main__":
    main()
