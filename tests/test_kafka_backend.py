"""
Tests for the KafkaBackend with aiokafka producer/consumer mocked.
We don't need a broker; we only verify that the backend wires up
aiokafka correctly: producer args, consumer args, group_id derivation,
serialization/deserialization, and lifecycle.
"""
from __future__ import annotations
import asyncio
import json
from unittest.mock import AsyncMock, MagicMock, patch
import pydantic
import pytest

from core import Topic, Edge, Runtime
from extensions.kafka import KafkaConfig, KafkaBackend


class Order(pydantic.BaseModel):
    data: str


class OrderCreated(Topic[Order]):
    config = KafkaConfig(topic="orders.created")


class OrderUpdated(Topic[Order]):
    config = KafkaConfig(topic="orders.updated")


class Echo(Edge[OrderCreated, OrderUpdated]):
    async def process(self):
        async for order in OrderCreated.sub(self.ctx):
            await OrderUpdated.pub(self.ctx, order)


class _FakeMsg:
    def __init__(self, value: bytes) -> None:
        self.value = value


class _FakeConsumer:
    """Minimal stand-in for AIOKafkaConsumer."""

    def __init__(self, *topics, **kwargs):
        self.topics = topics
        self.kwargs = kwargs
        self.started = False
        self.stopped = False
        self._messages: list[_FakeMsg] = []

    def feed(self, payloads: list[bytes]) -> None:
        self._messages.extend(_FakeMsg(p) for p in payloads)

    async def start(self):
        self.started = True

    async def stop(self):
        self.stopped = True

    def __aiter__(self):
        async def _gen():
            for m in self._messages:
                yield m
            # then idle forever, simulating a live consumer
            await asyncio.Event().wait()
        return _gen()


class _FakeProducer:
    def __init__(self, **kwargs):
        self.kwargs = kwargs
        self.started = False
        self.stopped = False
        self.sent: list[tuple[str, bytes, bytes | None]] = []

    async def start(self):
        self.started = True

    async def stop(self):
        self.stopped = True

    async def send_and_wait(self, topic, value, key=None):
        self.sent.append((topic, value, key))


@pytest.mark.asyncio
async def test_kafka_backend_end_to_end_with_mocks():
    fake_producer_holder: dict[str, _FakeProducer] = {}
    def producer_factory(**kwargs):
        p = _FakeProducer(**kwargs)
        fake_producer_holder["p"] = p
        return p
    created_consumers: list[_FakeConsumer] = []

    def consumer_factory(*topics, **kwargs):
        c = _FakeConsumer(*topics, **kwargs)
        # Pre-feed two messages on the first (and only) consumer
        c.feed([
            json.dumps({"data": "hello"}).encode("utf-8"),
            json.dumps({"data": "world"}).encode("utf-8"),
        ])
        created_consumers.append(c)
        return c

    with patch("extensions.kafka.backend.AIOKafkaProducer", side_effect=producer_factory), \
         patch("extensions.kafka.backend.AIOKafkaConsumer", side_effect=consumer_factory):

        rt = Runtime()
        rt.register_backend(KafkaBackend(bootstrap_servers="kafka:9092"))
        rt.add(Echo)

        try:
            await asyncio.wait_for(rt.run(), timeout=0.5)
        except asyncio.TimeoutError:
            pass  # expected: consumer iterator never finishes

    # producer started, configured, sent 2 messages on the right topic
    fake_producer = fake_producer_holder["p"]
    assert fake_producer.started and fake_producer.stopped
    assert fake_producer.kwargs["bootstrap_servers"] == "kafka:9092"
    assert [t for t, _, _ in fake_producer.sent] == ["orders.updated", "orders.updated"]
    payloads = [json.loads(v) for _, v, _ in fake_producer.sent]
    assert payloads == [{"data": "hello"}, {"data": "world"}]

    # one consumer was created with the right topic and a derived group_id
    assert len(created_consumers) == 1
    c = created_consumers[0]
    assert c.topics == ("orders.created",)
    assert c.kwargs["bootstrap_servers"] == "kafka:9092"
    assert c.kwargs["group_id"] == "edgy.Echo"
    assert c.started and c.stopped


@pytest.mark.asyncio
async def test_kafka_backend_skips_producer_if_no_outputs():
    """Pure-consumer deployments shouldn't spin up a producer."""

    class Sink(Edge[OrderCreated, OrderCreated]):  # only sub, never pub
        async def process(self):
            async for _ in OrderCreated.sub(self.ctx):
                return

    fake_producer = _FakeProducer()
    created: list[_FakeConsumer] = []

    def consumer_factory(*a, **kw):
        c = _FakeConsumer(*a, **kw)
        created.append(c)
        return c

    with patch("extensions.kafka.backend.AIOKafkaProducer", side_effect=lambda **kw: fake_producer), \
         patch("extensions.kafka.backend.AIOKafkaConsumer", side_effect=consumer_factory):
        backend = KafkaBackend(bootstrap_servers="kafka:9092")
        # register only an INPUT (no outputs)
        backend.register_input(OrderCreated, owner="Sink")
        await backend.start()
        assert not fake_producer.started, "producer should not start with no outputs"
        await backend.stop()


def test_kafka_config_explicit_group_id_wins():
    class T(Topic[Order]):
        config = KafkaConfig(topic="x", group_id="my-group")

    assert T.config.group_id == "my-group"


@pytest.mark.asyncio
async def test_publish_uses_explicit_key():
    fake_producer = _FakeProducer()

    class Keyed(Topic[Order]):
        config = KafkaConfig(topic="k", key=lambda d: d.data)

    with patch("extensions.kafka.backend.AIOKafkaProducer", side_effect=lambda **kw: fake_producer):
        backend = KafkaBackend(bootstrap_servers="x:1")
        backend.register_output(Keyed)
        await backend.start()
        await backend.publish(Keyed, Order(data="abc"))
        await backend.stop()

    assert fake_producer.sent[0][0] == "k"
    assert fake_producer.sent[0][2] == b"abc"
