import json
from typing import Any

import httpx
import pytest
from model_library import register_models

from vals_grading import Check, CheckJudge, GradingCase
from vals_grading.gateway import GatewayJudgeModel

MODEL = "anthropic/claude-opus-4-7"
REGISTRY = {
    "models": {
        MODEL: {
            "company": "anthropic",
            "label": "Test judge",
            "open_source": False,
            "properties": {"context_window": 200000, "max_tokens": 10000, "reasoning_model": False},
            "supports": {},
            "provider_properties": {},
            "costs_per_million_token": None,
            "provider_endpoint": "claude-opus-4-7",
            "provider_name": "anthropic",
            "full_key": MODEL,
            "slug": "test-judge",
        }
    }
}


async def test_real_gateway_client_with_platform_access_blocked(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("MODEL_GATEWAY_URL", "https://gateway.test")
    monkeypatch.setenv("MODEL_GATEWAY_API_KEY", "test-key")
    monkeypatch.setenv("IDENTITY", '{"service":"grading-test","agent_name":"snap-agent"}')
    requests: list[httpx.Request] = []

    def registry_send(self: httpx.Client, request: httpx.Request, **kwargs: Any) -> httpx.Response:
        requests.append(request)
        assert str(request.url.copy_with(query=None)) == "https://gateway.test/registry"
        assert request.headers["authorization"] == "Bearer test-key"
        return httpx.Response(200, request=request, json=REGISTRY)

    async def query_send(self: httpx.AsyncClient, request: httpx.Request, **kwargs: Any) -> httpx.Response:
        requests.append(request)
        assert str(request.url) == "https://gateway.test/query"
        assert request.headers["authorization"] == "Bearer test-key"
        body = json.loads(request.content)
        assert body["model"] == MODEL
        # The service identity is kept; the role is always the grader's, never the agent's.
        assert body["identity"] == {"service": "grading-test", "agent_name": "grader"}
        assert body["config"]["temperature"] == 0
        assert body["config"]["top_p"] == 1
        assert body["config"]["max_tokens"] == 10000
        assert body["inputs"][0] == {"kind": "system", "text": "<PROMPT>A\n"}
        return httpx.Response(200, request=request, json={"output_text": '{"result":1,"explanation":"ok"}'})

    monkeypatch.setattr(httpx.Client, "send", registry_send)
    monkeypatch.setattr(httpx.AsyncClient, "send", query_send)
    monkeypatch.setattr(register_models, "_registries", None)
    model = GatewayJudgeModel(MODEL)
    saved = GradingCase(
        id="test",
        input_under_test="Q",
        context={},
        checks=tuple(
            Check(operator="custom", prompt="{{output}}\n{{criteria}}", criteria=criteria, severity=1)
            for criteria in ("C", "D")
        ),
    )
    result = await CheckJudge(model).grade_test(saved, "A", {})
    assert result.pass_percentage_with_weight == 1
    assert [request.url.path for request in requests] == ["/registry", "/query", "/query"]


@pytest.mark.parametrize("identity", ["plain-service-name", '"service"', "[]"])
def test_bad_identity_fails_before_registry_access(monkeypatch: pytest.MonkeyPatch, identity: str) -> None:
    monkeypatch.setenv("MODEL_GATEWAY_URL", "https://gateway.test")
    monkeypatch.setenv("MODEL_GATEWAY_API_KEY", "test-key")
    monkeypatch.setenv("IDENTITY", identity)
    with pytest.raises(ValueError, match="IDENTITY"):
        GatewayJudgeModel(MODEL)


def test_gateway_credentials_are_required(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("MODEL_GATEWAY_API_KEY", raising=False)
    with pytest.raises(RuntimeError, match="MODEL_GATEWAY_API_KEY"):
        GatewayJudgeModel(MODEL)
