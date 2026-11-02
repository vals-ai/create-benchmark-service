import asyncio
import importlib.util
import json
import logging
import os
import stat
import sys
import types
from pathlib import Path
from collections.abc import Callable
from typing import Any, cast
from unittest.mock import AsyncMock

import pytest
from aiohttp import web

from benchmark_service.tool_runtime.native_tool_mcp import create_tool_mcp_app

ROOT = Path(__file__).resolve().parents[1]
NATIVE_TOOLS = ROOT / "src" / "benchmark_service" / "tool_runtime" / "native_tools.py"


@pytest.fixture
def native_tools(monkeypatch: pytest.MonkeyPatch) -> tuple[Any, list[str], object]:
    model_calls: list[str] = []
    llm = object()
    registry: Any = types.ModuleType("model_library.registry_utils")

    def get_registry_model(model: str, config: Any) -> object:
        model_calls.append(model)
        return llm

    registry.get_registry_model = get_registry_model
    base: Any = types.ModuleType("model_library.base")
    base.LLMConfig = types.SimpleNamespace
    gateway: Any = types.ModuleType("model_library.base.gateway")

    def gateway_llm(model_name: str, provider: str, *, config: Any) -> object:
        assert config.registry_key == f"{provider}/{model_name}"
        model_calls.append(config.registry_key)
        return llm

    gateway.GatewayLLM = gateway_llm
    monkeypatch.setitem(sys.modules, "model_library.registry_utils", registry)
    monkeypatch.setitem(sys.modules, "model_library.base", base)
    monkeypatch.setitem(sys.modules, "model_library.base.gateway", gateway)

    names = {
        "finance_agent.tools": [
            ("TavilyWebSearch", "web_search"),
            ("RetrieveInformation", "retrieve_information"),
            ("ParseHtmlPage", "parse_html_page"),
            ("EDGARSearch", "edgar_search"),
            ("Calculator", "calculator"),
            ("PriceHistory", "price_history"),
            ("SubmitFinalResult", "submit_final_result"),
        ],
        "legal_agent.tools": [
            ("TavilyWebSearch", "web_search"),
            ("RetrieveInformation", "retrieve_information"),
            ("ParseHtmlPage", "parse_html_page"),
            ("CourtListenerSearch", "courtlistener_search"),
            ("SubmitFinalResult", "submit_final_result"),
        ],
        "tax_agent.tools": [
            ("TavilyWebSearch", "web_search"),
            ("RetrieveInformation", "retrieve_information"),
            ("FetchDocument", "fetch_document"),
            ("LookupAuthority", "lookup_authority"),
            ("Calculator", "calculator"),
            ("SubmitFinalResult", "submit_final_result"),
        ],
    }

    def initialize(self: Any, **kwargs: Any) -> None:
        self.kwargs = kwargs

    for module_name, entries in names.items():
        module: Any = types.ModuleType(module_name)
        for class_name, name in entries:
            setattr(module, class_name, type(class_name, (), {"name": name, "__init__": initialize}))
        monkeypatch.setitem(sys.modules, module_name, module)

    emb: Any = types.ModuleType("emb_agent.cli")

    def build_emb_tools(model: Any) -> list[Any]:
        return [
            types.SimpleNamespace(name="bash"),
            FakeExecute(),
            FakeSubmit(),
            types.SimpleNamespace(name="retrieve_information", kwargs={"llm": model}),
        ]

    emb._build_tools = build_emb_tools
    emb._verify_graph_startup = lambda: None
    monkeypatch.setitem(sys.modules, "emb_agent.cli", emb)
    spec = importlib.util.spec_from_file_location("native_benchmark_tools", NATIVE_TOOLS)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module, model_calls, llm


@pytest.mark.parametrize(
    ("benchmark", "expected"),
    [
        (
            "fabv2-internal",
            [
                "web_search",
                "retrieve_information",
                "parse_html_page",
                "edgar_search",
                "calculator",
                "price_history",
                "submit_final_result",
            ],
        ),
        (
            "legal-research",
            [
                "web_search",
                "retrieve_information",
                "parse_html_page",
                "courtlistener_search",
                "submit_final_result",
            ],
        ),
        (
            "tax-agent",
            [
                "web_search",
                "retrieve_information",
                "fetch_document",
                "lookup_authority",
                "calculator",
                "submit_final_result",
            ],
        ),
    ],
)
def test_native_roster_preserves_helper_model_and_task_date(
    native_tools: tuple[Any, list[str], object],
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    benchmark: str,
    expected: list[str],
) -> None:
    module, model_calls, llm = native_tools
    monkeypatch.setenv("TAX_AGENT_CFR_CORPUS", str(tmp_path))
    monkeypatch.setenv("TAX_AGENT_USC_CORPUS", str(tmp_path))
    tools = module.build_tools(
        benchmark, "openai/unlisted-model", "2026-02-03", tmp_path / "submission.xlsx", tmp_path
    )
    assert [tool.name for tool in tools] == expected
    assert model_calls == ["openai/unlisted-model"]
    helper = next(tool for tool in tools if tool.name == "retrieve_information")
    assert helper.kwargs == {"llm": llm}
    for tool in tools:
        if benchmark == "tax-agent" and tool.name in {"web_search", "lookup_authority"}:
            assert tool.kwargs == {"as_of_date": "2026-02-03"}
        elif tool.name != "retrieve_information":
            assert tool.kwargs == {}


def test_tax_requires_pinned_authority_corpora(
    native_tools: tuple[Any, list[str], object], tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    module, _, _ = native_tools
    monkeypatch.delenv("TAX_AGENT_CFR_CORPUS", raising=False)
    monkeypatch.delenv("TAX_AGENT_USC_CORPUS", raising=False)
    with pytest.raises((KeyError, ValueError)):
        module.build_tools(
            "tax-agent",
            "openai/unlisted-model",
            "2026-04-30",
            tmp_path / "submission.xlsx",
            tmp_path,
        )


def test_unknown_static_model_uses_scoped_gateway_only(
    native_tools: tuple[Any, list[str], object], tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    module, model_calls, llm = native_tools
    registry: Any = sys.modules["model_library.registry_utils"]

    def static_lookup(model: str, config: Any) -> object:
        raise ValueError(f"{model} is missing from the static registry")

    monkeypatch.setattr(registry, "get_registry_model", static_lookup)
    tools = module.build_tools(
        "fabv2-internal", "openai/unlisted-model", "2026-03-01", tmp_path / "submission.xlsx", tmp_path
    )
    helper = next(tool for tool in tools if tool.name == "retrieve_information")
    assert helper.kwargs == {"llm": llm}
    assert model_calls == ["openai/unlisted-model"]


def test_emb_never_exposes_shell_from_trusted_tool_process(
    native_tools: tuple[Any, list[str], object], tmp_path: Path
) -> None:
    module, _, _ = native_tools
    tools = module.build_tools("emb", "openai/unlisted-model", "2026-04-30", tmp_path / "submission.xlsx", tmp_path)
    assert [tool.name for tool in tools] == ["execute", "submit", "retrieve_information"]
    assert isinstance(tools[0], module.WorkbookExecute)
    assert isinstance(tools[1], module.WorkbookSubmit)


class FakeExecute:
    name = "execute"
    description = "Recalculate the submission workbook."
    parameters: dict[str, Any] = {"file": {"type": "string"}}
    required: list[str] = []

    def __init__(self) -> None:
        self.paths: list[Path] = []
        self.on_execute: Callable[[], None] = lambda: None

    async def execute(self, args: dict[str, Any], state: dict[str, Any], logger: logging.Logger) -> Any:
        workbook = Path(args["file"])
        self.paths.append(workbook)
        assert workbook.read_bytes() == b"workbook"
        workbook.write_bytes(b"recalculated")
        self.on_execute()
        return types.SimpleNamespace(output="verified", error=None, done=False)


class FakeSubmit:
    name = "submit"
    description = "Submit the workbook."
    parameters: dict[str, Any] = {"answer": {"type": "string"}}
    required = ["answer"]

    def __init__(self) -> None:
        self.calls: list[tuple[dict[str, Any], dict[str, Any]]] = []
        self.result = types.SimpleNamespace(output="Native answer", error=None, done=True)

    async def execute(self, args: dict[str, Any], state: dict[str, Any], logger: logging.Logger) -> Any:
        self.calls.append((args, state))
        return self.result


def test_submit_freezes_workbook_before_later_agent_edits(
    native_tools: tuple[Any, list[str], object], tmp_path: Path
) -> None:
    module, _, _ = native_tools
    workbook = tmp_path / "shared" / "submission.xlsx"
    workbook.parent.mkdir()
    workbook.write_bytes(b"submitted workbook")
    private = tmp_path / "private"
    private.mkdir()
    tool = FakeSubmit()
    wrapped = module.WorkbookSubmit(tool, workbook, private)
    args = {"answer": "Finished"}
    state = {"document": "original"}
    result = asyncio.run(wrapped.execute(args, state, logging.getLogger(__name__)))
    workbook.write_bytes(b"later agent edits")
    assert (private / "submission.xlsx").read_bytes() == b"submitted workbook"
    assert stat.S_IMODE((private / "submission.xlsx").stat().st_mode) == 0o400
    assert result is tool.result
    assert tool.calls == [(args, state)]
    assert wrapped.parameters == tool.parameters
    assert wrapped.required == tool.required


@pytest.mark.parametrize(("error", "done"), [("failed", False), (None, False)])
def test_submit_does_not_freeze_when_native_tool_does_not_submit(
    native_tools: tuple[Any, list[str], object], tmp_path: Path, error: str | None, done: bool
) -> None:
    module, _, _ = native_tools
    workbook = tmp_path / "submission.xlsx"
    workbook.write_bytes(b"partial workbook")
    private = tmp_path / "private"
    private.mkdir()
    tool = FakeSubmit()
    tool.result.error = error
    tool.result.done = done
    wrapped = module.WorkbookSubmit(tool, workbook, private)
    result = asyncio.run(wrapped.execute({"answer": "Finished"}, {}, logging.getLogger(__name__)))
    assert result is tool.result
    assert not (private / "submission.xlsx").exists()
    assert list(private.iterdir()) == []


@pytest.mark.parametrize("link", ["symlink", "hardlink"])
def test_submit_rejects_links_before_calling_native_tool(
    native_tools: tuple[Any, list[str], object], tmp_path: Path, link: str
) -> None:
    module, _, _ = native_tools
    original = tmp_path / "original.xlsx"
    original.write_bytes(b"private workbook")
    workbook = tmp_path / "submission.xlsx"
    if link == "symlink":
        workbook.symlink_to(original)
    else:
        os.link(original, workbook)
    private = tmp_path / "private"
    private.mkdir()
    tool = FakeSubmit()
    wrapped = module.WorkbookSubmit(tool, workbook, private)
    with pytest.raises((ValueError, OSError)):
        asyncio.run(wrapped.execute({"answer": "Finished"}, {}, logging.getLogger(__name__)))
    assert tool.calls == []
    assert original.read_bytes() == b"private workbook"
    assert list(private.iterdir()) == []


def test_execute_recalculates_private_copy_and_keeps_native_schema(
    native_tools: tuple[Any, list[str], object], tmp_path: Path
) -> None:
    module, _, _ = native_tools
    workbook = tmp_path / "shared" / "submission.xlsx"
    workbook.parent.mkdir()
    workbook.write_bytes(b"workbook")
    private = tmp_path / "private"
    private.mkdir()
    tool = FakeExecute()
    wrapped = module.WorkbookExecute(tool, workbook, private)
    result = asyncio.run(wrapped.execute({}, {}, logging.getLogger(__name__)))
    assert result.output == "verified"
    assert workbook.read_bytes() == b"recalculated"
    assert wrapped.parameters == tool.parameters
    assert wrapped.required == tool.required
    assert tool.paths[0].is_relative_to(private)
    assert not tool.paths[0].exists()


def test_execute_rejects_arbitrary_files_and_symlinks(
    native_tools: tuple[Any, list[str], object], tmp_path: Path
) -> None:
    module, _, _ = native_tools
    secret = tmp_path / "private.txt"
    secret.write_bytes(b"keep secret")
    workbook = tmp_path / "submission.xlsx"
    workbook.symlink_to(secret)
    tool = FakeExecute()
    wrapped = module.WorkbookExecute(tool, workbook, tmp_path)
    with pytest.raises(ValueError):
        asyncio.run(wrapped.execute({"file": str(secret)}, {}, logging.getLogger(__name__)))
    with pytest.raises((OSError, ValueError)):
        asyncio.run(wrapped.execute({}, {}, logging.getLogger(__name__)))
    assert secret.read_bytes() == b"keep secret"
    assert tool.paths == []


def test_execute_rejects_workbook_replacement_during_recalculation(
    native_tools: tuple[Any, list[str], object], tmp_path: Path
) -> None:
    module, _, _ = native_tools
    workbook = tmp_path / "submission.xlsx"
    workbook.write_bytes(b"workbook")
    secret = tmp_path / "private.txt"
    secret.write_bytes(b"keep secret")
    tool = FakeExecute()

    def replace() -> None:
        workbook.unlink()
        workbook.symlink_to(secret)

    tool.on_execute = replace
    wrapped = module.WorkbookExecute(tool, workbook, tmp_path)
    with pytest.raises(ValueError):
        asyncio.run(wrapped.execute({}, {}, logging.getLogger(__name__)))
    assert secret.read_bytes() == b"keep secret"


def test_unknown_benchmark_fails(native_tools: tuple[Any, list[str], object], tmp_path: Path) -> None:
    module, _, _ = native_tools
    with pytest.raises(ValueError):
        module.build_tools("other", "openai/unlisted-model", "2026-04-30", tmp_path / "submission.xlsx", tmp_path)


@pytest.mark.parametrize("benchmark", ["fabv2-internal", "tax-agent"])
def test_native_finance_and_tax_prompts_keep_exact_text(
    native_tools: tuple[Any, list[str], object], monkeypatch: pytest.MonkeyPatch, benchmark: str
) -> None:
    module, _, _ = native_tools
    prompt = types.ModuleType("native_prompt")
    setattr(prompt, "SYSTEM_PROMPT", "Native instruction. Keep {{literal braces}} unchanged.\n")
    setattr(prompt, "QUESTION_PROMPT", "Question:\n{question}")
    monkeypatch.setitem(sys.modules, "finance_agent.prompt", prompt)
    monkeypatch.setitem(sys.modules, "tax_agent.prompt", prompt)
    monkeypatch.setitem(sys.modules, "tax_agent.tools", types.SimpleNamespace(DEFAULT_AS_OF_DATE="2026-04-30"))
    assert module.render_instructions(benchmark, "task", "Question with {braces}.", "2026-04-30") == (
        "Native instruction. Keep {{literal braces}} unchanged.\n",
        "Question:\nQuestion with {braces}.",
    )


def test_tax_prompt_uses_native_as_of_date_override(
    native_tools: tuple[Any, list[str], object], monkeypatch: pytest.MonkeyPatch
) -> None:
    module, _, _ = native_tools
    prompt = types.ModuleType("tax_agent.prompt")
    setattr(prompt, "SYSTEM_PROMPT", "Native tax instructions.")
    setattr(prompt, "QUESTION_PROMPT", "Question:\n{question}")
    monkeypatch.setitem(sys.modules, "tax_agent.prompt", prompt)
    monkeypatch.setitem(sys.modules, "tax_agent.tools", types.SimpleNamespace(DEFAULT_AS_OF_DATE="2026-04-30"))
    assert module.render_instructions("tax-agent", "task", "Tax year 2025", "2026-02-03") == (
        "Native tax instructions.",
        "Answer this question as of 2026-02-03.\n\nQuestion:\nTax year 2025",
    )


def test_legal_prompt_formats_native_tool_loop_template_once(
    native_tools: tuple[Any, list[str], object], monkeypatch: pytest.MonkeyPatch
) -> None:
    module, _, _ = native_tools
    prompt: Any = types.ModuleType("legal_agent.prompt")
    modes: list[str] = []

    def instructions_prompt(mode: str) -> str:
        modes.append(mode)
        return 'Native legal instructions.\n{{"sources": []}}\nQuestion:\n{question}\n'

    prompt.instructions_prompt = instructions_prompt
    monkeypatch.setitem(sys.modules, "legal_agent.prompt", prompt)
    assert module.render_instructions("legal-research", "task", "Question {braces}", "2026-05-10") == (
        "",
        'Native legal instructions.\n{"sources": []}\nQuestion:\nQuestion {braces}\n',
    )
    assert modes == ["tool_loop"]


@pytest.mark.parametrize("task_id", ["model-template", "model-scratch"])
def test_emb_prompt_uses_native_template_selection(
    native_tools: tuple[Any, list[str], object], monkeypatch: pytest.MonkeyPatch, task_id: str
) -> None:
    module, _, _ = native_tools
    cli: Any = sys.modules["emb_agent.cli"]
    read_ids: list[str] = []

    def read_system_prompt(question_id: str) -> str:
        read_ids.append(question_id)
        return (
            "Native template instructions.\n" if question_id.endswith("-template") else "Native scratch instructions.\n"
        )

    cli._read_system_prompt = read_system_prompt
    expected = read_system_prompt(task_id)
    read_ids.clear()
    assert module.render_instructions("emb", task_id, "Build {model}.", "2026-06-01") == (
        expected,
        "Build {model}.",
    )
    assert read_ids == [task_id]


async def test_redaction_keeps_native_settings_and_hides_real_credentials(
    native_tools: tuple[Any, list[str], object], tmp_path: Path
) -> None:
    module, _, _ = native_tools
    environment = {
        "EMB_EXCEL_RECALC_BACKEND": "graph",
        "TAX_AGENT_CFR_CORPUS": "/trusted/corpus",
        "MODEL_GATEWAY_URL": "https://gateway.test",
        "MODEL_GATEWAY_API_KEY": "task-secret",
        "GRAPH_CLIENT_SECRET": "graph-secret",
        "HTTP_PROXY": "http://tools:proxy-secret@proxy.test:8080",
    }
    native_text = (
        "A paragraph about graph uses /trusted/corpus and https://gateway.test. "
        "task-secret graph-secret proxy-secret http://tools:proxy-secret@proxy.test:8080"
    )
    tool: Any = types.SimpleNamespace(
        name="submit",
        description="Native submission",
        parameters={},
        required=[],
        execute=AsyncMock(
            return_value=types.SimpleNamespace(
                output=native_text, error=None, done=True, metadata=None, native_query_result=None
            )
        ),
    )
    app = create_tool_mcp_app(
        [tool], "task", "openai/unlisted-model", tmp_path, module._redaction_secrets(environment)
    )
    request = cast(
        web.Request,
        types.SimpleNamespace(
            headers={},
            json=AsyncMock(
                return_value={
                    "jsonrpc": "2.0",
                    "id": 1,
                    "method": "tools/call",
                    "params": {"name": "submit", "arguments": {}},
                }
            ),
        ),
    )
    route = next(route for route in app.router.routes() if route.method == "POST")
    response = cast(web.Response, await route.handler(request))
    response_text = response.text
    assert response_text is not None
    answer = json.loads(response_text)["result"]["content"][0]["text"]
    assert answer.startswith("A paragraph about graph uses /trusted/corpus and https://gateway.test.")
    submission = (tmp_path / "generation.json").read_text()
    records = (tmp_path / "tool_calls.jsonl").read_text()
    for secret in ("task-secret", "graph-secret", "proxy-secret", environment["HTTP_PROXY"]):
        assert secret not in response_text + submission + records
    assert json.loads(submission)["answer"] == answer
