"""
Runtime-level behaviour: edge crash isolation, graceful shutdown.
"""
from __future__ import annotations
import asyncio
import pydantic
import pytest

from core import Topic, Edge, Runtime
from extensions.memory.runtime_context import InMemoryRuntimeContext
from extensions.memory.config_dict import InMemoryConfig


class Msg(pydantic.BaseModel):
    v: int


class TopicA(Topic[Msg]):
    config = InMemoryConfig()


class CrasherInput(Topic[Msg]):
    config = InMemoryConfig()


class Crasher(Edge[CrasherInput, CrasherInput]):
    async def process(self) -> None:
        raise RuntimeError("boom")


class Survivor(Edge[TopicA, TopicA]):
    """Stays alive forever; gets cancelled when the runtime tears down."""

    async def process(self) -> None:
        async for _ in TopicA.sub(self.ctx):
            pass


@pytest.fixture(autouse=True)
def _reset_bus():
    InMemoryRuntimeContext.reset()
    yield
    InMemoryRuntimeContext.reset()


@pytest.mark.asyncio
async def test_crashing_edge_does_not_kill_the_runtime(capsys):
    rt = Runtime()
    rt.add(Crasher, InMemoryRuntimeContext)
    rt.add(Survivor, InMemoryRuntimeContext)

    run_task = asyncio.create_task(rt.run())
    await asyncio.sleep(0.05)  # let Crasher raise; Survivor stays subscribed

    assert not run_task.done(), "runtime should still be running"

    run_task.cancel()
    try:
        await run_task
    except asyncio.CancelledError:
        pass

    err = capsys.readouterr().err
    assert "Crasher" in err
    assert "boom" in err


@pytest.mark.asyncio
async def test_runtime_with_no_edges_starts_and_stops():
    rt = Runtime()
    await rt.run()
