"""
End-to-end test of the core wiring with a tiny in-memory Backend.
Exercises: Topic.model derivation, Edge.parse_connectivity,
Runtime.register_backend / add / run, and the dispatch from
RuntimeContext to the right Backend by ConfigDict type.
"""
from __future__ import annotations
import asyncio
import typing as t
import pydantic
import pytest

from core import Topic, Edge, Runtime, Backend, ConfigDict


# ---- in-memory backend ------------------------------------------------------


class MemConfig(ConfigDict):
    def __init__(self, name: str) -> None:
        self.name = name


class MemBackend(Backend):
    config_cls = MemConfig

    def __init__(self) -> None:
        self.queues: dict[str, asyncio.Queue] = {}
        self.published: list[tuple[str, pydantic.BaseModel]] = []
        self.started = False
        self.stopped = False

    def _q(self, name: str) -> asyncio.Queue:
        return self.queues.setdefault(name, asyncio.Queue())

    def register_input(self, topic, owner):
        self._q(topic.config.name)

    def register_output(self, topic):
        self._q(topic.config.name)

    async def start(self):
        self.started = True

    async def stop(self):
        self.stopped = True

    async def publish(self, topic, data):
        self.published.append((topic.config.name, data))
        await self._q(topic.config.name).put(data)

    async def subscribe(self, topic, *, owner):
        q = self._q(topic.config.name)
        model_cls = topic.model
        while True:
            raw = await q.get()
            if raw is None:  # sentinel to end
                return
            if isinstance(raw, model_cls):
                yield raw
            else:
                yield model_cls.model_validate(raw)


# ---- domain -----------------------------------------------------------------


class Ping(pydantic.BaseModel):
    n: int


class Pong(pydantic.BaseModel):
    n: int


class PingTopic(Topic[Ping]):
    config = MemConfig("ping")


class PongTopic(Topic[Pong]):
    config = MemConfig("pong")


class Pinger(Edge[PingTopic, PongTopic]):
    async def process(self):
        async for ping in PingTopic.sub(self.ctx):
            await PongTopic.pub(self.ctx, Pong(n=ping.n + 1))


# ---- tests ------------------------------------------------------------------


def test_topic_model_auto_derived():
    assert PingTopic.model is Ping
    assert PongTopic.model is Pong


def test_connectivity():
    c = Pinger.parse_connectivity()
    assert c["inputs"] == {PingTopic}
    assert c["outputs"] == {PongTopic}


def test_runtime_rejects_topic_without_backend():
    rt = Runtime()
    with pytest.raises(RuntimeError, match="No backend registered"):
        rt.add(Pinger)


def test_double_register_backend():
    rt = Runtime()
    rt.register_backend(MemBackend())
    with pytest.raises(RuntimeError, match="already registered"):
        rt.register_backend(MemBackend())


@pytest.mark.asyncio
async def test_end_to_end_pubsub():
    rt = Runtime()
    backend = MemBackend()
    rt.register_backend(backend)
    rt.add(Pinger)

    # seed input
    await backend.publish(PingTopic, Ping(n=1))
    await backend.publish(PingTopic, Ping(n=10))

    async def run_briefly():
        try:
            await asyncio.wait_for(rt.run(), timeout=0.3)
        except asyncio.TimeoutError:
            pass

    await run_briefly()

    pongs = [d for name, d in backend.published if name == "pong"]
    assert {p.n for p in pongs} == {2, 11}
    assert backend.started and backend.stopped


@pytest.mark.asyncio
async def test_runtime_with_no_edges_just_starts_and_stops():
    rt = Runtime()
    b = MemBackend()
    rt.register_backend(b)
    await rt.run()
    assert b.started and b.stopped


@pytest.mark.asyncio
async def test_unsafe_pub_unknown_config_raises():
    """A topic with a config type no backend handles must raise."""

    class OtherConfig(ConfigDict):
        pass

    class Lonely(Topic[Ping]):
        config = OtherConfig()

    class LonelyEdge(Edge[Lonely, PongTopic]):
        async def process(self): ...

    rt = Runtime()
    rt.register_backend(MemBackend())
    with pytest.raises(RuntimeError, match="No backend registered"):
        rt.add(LonelyEdge)
