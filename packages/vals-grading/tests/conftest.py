import socket

import pytest


@pytest.fixture(autouse=True)
def block_network(monkeypatch: pytest.MonkeyPatch) -> None:
    def refused(*args: object, **kwargs: object) -> None:
        raise AssertionError("Tests must not contact the platform or any external service")

    monkeypatch.setattr(socket.socket, "connect", refused)
    monkeypatch.setattr(socket, "create_connection", refused)
    monkeypatch.delenv("VALS_API_KEY", raising=False)


class RecordedModel:
    def __init__(self, replies: list[str | BaseException]) -> None:
        self.replies = iter(replies)
        self.prompts: list[str] = []

    async def generate(self, prompt: str, system: str = "") -> str:
        self.prompts.append(system + prompt)
        reply = next(self.replies)
        if isinstance(reply, BaseException):
            raise reply
        return reply

    async def count_tokens(self, text: str) -> int:
        return len(text)
