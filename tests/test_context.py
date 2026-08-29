"""Request-scoped sandbox provider access."""

import asyncio

from benchmark_service.context import current_sandbox_provider, sandbox_provider_scope


class _Provider:
    def __init__(self, name: str) -> None:
        self.name = name


def test_no_provider_outside_a_request() -> None:
    assert current_sandbox_provider() is None


def test_scope_binds_and_restores() -> None:
    provider = _Provider("a")

    with sandbox_provider_scope(provider):  # pyright: ignore[reportArgumentType]
        assert current_sandbox_provider() is provider

    assert current_sandbox_provider() is None


def test_scope_restores_after_an_error() -> None:
    try:
        with sandbox_provider_scope(_Provider("a")):  # pyright: ignore[reportArgumentType]
            raise RuntimeError("grading blew up")
    except RuntimeError:
        pass

    assert current_sandbox_provider() is None


def test_concurrent_requests_do_not_see_each_other() -> None:
    """Each request runs in its own context, so bindings must not leak across tasks."""

    async def request(name: str, hold: float) -> str | None:
        with sandbox_provider_scope(_Provider(name)):  # pyright: ignore[reportArgumentType]
            await asyncio.sleep(hold)
            provider = current_sandbox_provider()
            return getattr(provider, "name", None)

    async def main() -> list[str | None]:
        return list(await asyncio.gather(request("a", 0.02), request("b", 0.01)))

    assert asyncio.run(main()) == ["a", "b"]


def test_generator_body_sees_the_binding_of_its_consumer() -> None:
    """evaluate_instance is an async generator consumed inside the scope."""

    async def evaluate() -> list[str | None]:
        async def body():
            provider = current_sandbox_provider()
            yield getattr(provider, "name", None)

        generator = body()
        with sandbox_provider_scope(_Provider("a")):  # pyright: ignore[reportArgumentType]
            return [chunk async for chunk in generator]

    assert asyncio.run(evaluate()) == ["a"]


def test_binding_reaches_a_task_spawned_inside_the_scope() -> None:
    """The grading path drains its evaluation through asyncio.create_task.

    A task copies the context at creation, so the binding has to be active
    around the drain, not only around the generator's creation.
    """

    async def main() -> str | None:
        async def body():
            yield getattr(current_sandbox_provider(), "name", None)

        generator = body()
        with sandbox_provider_scope(_Provider("a")):  # pyright: ignore[reportArgumentType]
            return await asyncio.create_task(anext(generator))  # pyright: ignore[reportUnknownArgumentType]

    assert asyncio.run(main()) == "a"


def test_binding_survives_a_stream_closed_through_a_task() -> None:
    """Grading closes its stream with ensure_future, which copies the context.

    A scope entered inside the stream could not reset its token there, and a
    consumer's binding still has to reach the finalizer.
    """
    seen: list[str | None] = []

    async def main() -> None:
        async def body():
            try:
                yield "result"
            finally:
                seen.append(getattr(current_sandbox_provider(), "name", None))

        with sandbox_provider_scope(_Provider("a")):  # pyright: ignore[reportArgumentType]
            generator = body()
            assert await anext(generator) == "result"  # pyright: ignore[reportUnknownArgumentType]
            await asyncio.ensure_future(generator.aclose())

    asyncio.run(main())
    assert seen == ["a"]
