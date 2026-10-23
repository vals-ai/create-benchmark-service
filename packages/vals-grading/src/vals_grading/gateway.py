"""Model Gateway integration for custom-check grading."""

import json
import os

from model_library.base import LLMConfig
from model_library.base.gateway import GatewayLLM
from model_library.base.input import SystemInput, TextInput
from model_library.registry_utils import get_registry_model
from model_library.telemetry import IdentityValidationError, normalize_identity

JUDGE_CONFIG = LLMConfig(temperature=0, top_p=1.0, max_tokens=10_000)


class GatewayJudgeModel:
    """Send judge requests through model-library's Gateway client."""

    def __init__(self, model: str) -> None:
        missing = [
            name for name in ("MODEL_GATEWAY_URL", "MODEL_GATEWAY_API_KEY", "IDENTITY") if not os.environ.get(name)
        ]
        if missing:
            raise RuntimeError(f"Set {' and '.join(missing)} before starting the grading service.")
        try:
            identity = normalize_identity(json.loads(os.environ["IDENTITY"]))
        except (json.JSONDecodeError, IdentityValidationError) as error:
            raise ValueError("Set IDENTITY to a JSON object that identifies the grading service.") from error
        self._model = get_registry_model(model, override_config=JUDGE_CONFIG, identity=identity)
        if not isinstance(self._model, GatewayLLM):
            raise RuntimeError(
                "The judge did not select the Gateway. Set Gateway configuration before importing model-library."
            )

    async def generate(self, prompt: str, system: str = "") -> str:
        inputs = [SystemInput(text=system)] if system else []
        return (await self._model.query([*inputs, TextInput(text=prompt)])).output_text_str

    async def count_tokens(self, text: str) -> int:
        encoding = await self._model.get_encoding()
        return len(encoding.encode(text))
