"""
Verifies KafkaRuntimeContext wires aiokafka correctly without needing
a live broker, plus connectivity guards on the base RuntimeContext.
"""
from __future__ import annotations
import asyncio
import json
from unittest.mock import patch
import pydantic
import pytest

from core import Topic, Edge, Runtime
from extensions.kafka.runtime_context import KafkaRuntimeContext
from extensions.kafka.config_dict import KafkaConfig


class FakeKafka(KafkaRuntimeContext):
    bootstrap_servers = "kafka:9092"


class Order(pydantic.BaseModel):
    data: str


class OrderCreated(Topic[Order]):
    config = KafkaConfig(topic="orders.created", group_id="oc.echo")


class OrderUpdated(Topic[Order]):
    config = KafkaConfig(topic="orders.updated", group_id="ou.echo")


class Echo(Edge[OrderCreated, OrderUpdated]):
    async def process(self) -> None:
        async for order in OrderCreated.sub(self.ctx):
            await OrderUpdated.pub(self.ctx, order)


class _FakeMsg:
    def __init__(self, value: bytes) -> None:
        self.value = value


class _FakeConsumer:
    def __init__(self, *topics, **kwargs) -> None:
        self.topics = topics
        self.kwargs = kwargs
        self.started = False
        self.stopped = False
        self._messages: list[_FakeMsg] = []

    def feed(self, payloads: list[bytes]) -> None:
        self._messages.extend(_FakeMsg(p) for p in payloads)

    async def start(self) -> None:
        self.started = True

    async def stop(self) -> None:
        self.stopped = True

    def __aiter__(self):
        async def _gen():
            for m in self._messages:
                yield m
            await asyncio.Event().wait()  # idle forever
        return _gen()


class _FakeProducer:
    def __init__(self, **kwargs) -> None:
        self.kwargs = kwargs
        self.started = False
        self.stopped = False
        self.sent: list[tuple[str, bytes, bytes | None]] = []

    async def start(self) -> None:
        self.started = True

    async def stop(self) -> None:
        self.stopped = True

    async def send_and_wait(self, topic, value, key=None) -> None:
        self.sent.append((topic, value, key))


def test_topic_model_auto_derived():
    assert OrderCreated.model is Order
    assert OrderUpdated.model is Order


def test_subclass_missing_bootstrap_servers_fails_at_instantiation():
    class NoBootstrap(KafkaRuntimeContext):
        pass

    with pytest.raises(TypeError, match="bootstrap_servers"):
        NoBootstrap(allowed_input=set(), allowed_output=set(), owner="x")


@pytest.mark.asyncio
async def test_unsafe_pub_rejects_topic_not_in_allowed_output():
    ctx = FakeKafka(allowed_input=set(), allowed_output=set(), owner="X")
    with pytest.raises(PermissionError, match="output"):
        await ctx.unsafe_pub(OrderCreated, Order(data="x"))


def test_unsafe_sub_rejects_topic_not_in_allowed_input():
    ctx = FakeKafka(allowed_input=set(), allowed_output=set(), owner="X")
    with pytest.raises(PermissionError, match="input"):
        ctx.unsafe_sub(OrderCreated)


@pytest.mark.asyncio
async def test_runtime_wires_aiokafka_end_to_end():
    holder: dict[str, _FakeProducer] = {}

    def producer_factory(**kw):
        p = _FakeProducer(**kw)
        holder["p"] = p
        return p

    consumers: list[_FakeConsumer] = []

    def consumer_factory(*a, **kw):
        c = _FakeConsumer(*a, **kw)
        c.feed([
            json.dumps({"data": "hello"}).encode("utf-8"),
            json.dumps({"data": "world"}).encode("utf-8"),
        ])
        consumers.append(c)
        return c

    with patch(
        "extensions.kafka.runtime_context.AIOKafkaProducer",
        side_effect=producer_factory,
    ), patch(
        "extensions.kafka.runtime_context.AIOKafkaConsumer",
        side_effect=consumer_factory,
    ):
        rt = Runtime()
        rt.add(Echo, FakeKafka)
        try:
            await asyncio.wait_for(rt.run(), timeout=0.3)
        except asyncio.TimeoutError:
            pass

    p = holder["p"]
    assert p.started and p.stopped
    assert p.kwargs["bootstrap_servers"] == "kafka:9092"
    assert [t for t, _, _ in p.sent] == ["orders.updated", "orders.updated"]
    assert [json.loads(v) for _, v, _ in p.sent] == [
        {"data": "hello"},
        {"data": "world"},
    ]

    assert len(consumers) == 1
    c = consumers[0]
    assert c.topics == ("orders.created",)
    assert c.kwargs["bootstrap_servers"] == "kafka:9092"
    assert c.kwargs["group_id"] == "oc.echo"
    assert c.kwargs["auto_offset_reset"] == "latest"
    assert c.started and c.stopped


@pytest.mark.asyncio
async def test_no_producer_when_no_outputs():
    fake = _FakeProducer()
    with patch(
        "extensions.kafka.runtime_context.AIOKafkaProducer",
        side_effect=lambda **kw: fake,
    ):
        ctx = FakeKafka(
            allowed_input={OrderCreated},
            allowed_output=set(),
            owner="Sink",
        )
        await ctx.start()
        assert not fake.started
        await ctx.stop()


@pytest.mark.asyncio
async def test_publish_uses_explicit_key():
    fake = _FakeProducer()

    class Keyed(Topic[Order]):
        config = KafkaConfig(
            topic="k",
            group_id="k.unused",
            key=lambda d: d.data,
        )

    with patch(
        "extensions.kafka.runtime_context.AIOKafkaProducer",
        side_effect=lambda **kw: fake,
    ):
        ctx = FakeKafka(
            allowed_input=set(),
            allowed_output={Keyed},
            owner="X",
        )
        await ctx.start()
        await ctx.unsafe_pub(Keyed, Order(data="abc"))
        await ctx.stop()

    assert fake.sent[0][0] == "k"
    assert fake.sent[0][2] == b"abc"


@pytest.mark.asyncio
async def test_subscribe_uses_kafka_config_group_id():
    class Grouped(Topic[Order]):
        config = KafkaConfig(topic="g", group_id="my-group")

    consumers: list[_FakeConsumer] = []

    def consumer_factory(*a, **kw):
        c = _FakeConsumer(*a, **kw)
        consumers.append(c)
        return c

    with patch(
        "extensions.kafka.runtime_context.AIOKafkaConsumer",
        side_effect=consumer_factory,
    ):
        ctx = FakeKafka(
            allowed_input={Grouped},
            allowed_output=set(),
            owner="Whatever",
        )

        async def drain():
            async for _ in ctx.unsafe_sub(Grouped):
                pass

        task = asyncio.create_task(drain())
        await asyncio.sleep(0.05)
        task.cancel()
        try:
            await task
        except asyncio.CancelledError:
            pass

    assert consumers[0].kwargs["group_id"] == "my-group"


@pytest.mark.asyncio
async def test_poisoned_message_raises_with_context():
    class T(Topic[Order]):
        config = KafkaConfig(topic="poison", group_id="poison.g")

    bad = _FakeMsg(b'{"not_data_field": 42}')

    def consumer_factory(*a, **kw):
        c = _FakeConsumer(*a, **kw)
        c.feed([bad.value])
        return c

    with patch(
        "extensions.kafka.runtime_context.AIOKafkaConsumer",
        side_effect=consumer_factory,
    ):
        ctx = FakeKafka(
            allowed_input={T},
            allowed_output=set(),
            owner="X",
        )

        async def drain():
            async for _ in ctx.unsafe_sub(T):
                pass

        with pytest.raises(ValueError, match="Invalid message on Kafka topic 'poison'"):
            await asyncio.wait_for(drain(), timeout=0.5)
