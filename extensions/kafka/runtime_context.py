from __future__ import annotations
import typing as t
import pydantic
from aiokafka import AIOKafkaProducer, AIOKafkaConsumer

from core.runtime_context import RuntimeContext
from core.topic import Topic
from .config_dict import KafkaConfig


class KafkaRuntimeContext(RuntimeContext):
    """
    Kafka-backed `RuntimeContext` over `aiokafka`.

    Cluster-wide settings live on the class. A deployment defines a
    subclass that pins `bootstrap_servers` (and optionally the group
    prefix) and passes that subclass to `Runtime.add`, e.g.

        class MyKafka(KafkaRuntimeContext):
            bootstrap_servers = "localhost:9092"

        runtime.add(OrderProcessor, MyKafka)

    Each instance owns one shared `AIOKafkaProducer` (lazy: only
    created when the edge has outputs) and creates one
    `AIOKafkaConsumer` per `_subscribe` call, so every (edge, input
    topic) pair gets its own group and scales independently.
    """

    bootstrap_servers: t.ClassVar[str]
    # Default to "latest" so a new group does not silently replay the
    # entire retention window. Subclasses that want replay should set
    # `auto_offset_reset = "earliest"` explicitly.
    auto_offset_reset: t.ClassVar[str] = "latest"

    def __init__(
        self,
        allowed_input: set[type[Topic]],
        allowed_output: set[type[Topic]],
        owner: str = "",
    ) -> None:
        if not getattr(type(self), "bootstrap_servers", None):
            raise TypeError(
                f"{type(self).__name__} must set `bootstrap_servers` as a "
                f"class attribute (e.g. `bootstrap_servers = 'localhost:9092'`)."
            )
        super().__init__(allowed_input, allowed_output, owner=owner)
        self._producer: AIOKafkaProducer | None = None

    async def start(self) -> None:
        if self.allowed_output:
            producer = AIOKafkaProducer(
                bootstrap_servers=self.bootstrap_servers,
            )
            await producer.start()
            self._producer = producer

    async def stop(self) -> None:
        if self._producer is not None:
            await self._producer.stop()
            self._producer = None

    async def _publish[M: pydantic.BaseModel](
        self,
        topic: type[Topic[M]],
        data: M,
    ) -> None:
        assert isinstance(topic.config, KafkaConfig)
        if self._producer is None:
            raise RuntimeError(
                f"KafkaRuntimeContext for {self.owner!r} has no producer; "
                f"start() was not called or did not initialise one."
            )
        cfg = topic.config
        payload = data.model_dump_json().encode("utf-8")
        key = self._encode_key(cfg.key(data) if cfg.key else None)
        await self._producer.send_and_wait(cfg.topic, payload, key=key)

    async def _subscribe[M: pydantic.BaseModel](
        self,
        topic: type[Topic[M]],
    ) -> t.AsyncIterator[M]:
        assert isinstance(topic.config, KafkaConfig)
        cfg = topic.config
        consumer = AIOKafkaConsumer(
            cfg.topic,
            bootstrap_servers=self.bootstrap_servers,
            group_id=cfg.group_id,
            enable_auto_commit=True,
            auto_offset_reset=self.auto_offset_reset,
        )
        await consumer.start()
        model_cls = topic.model
        try:
            async for msg in consumer:
                try:
                    parsed = model_cls.model_validate_json(msg.value)
                except pydantic.ValidationError as e:
                    raise ValueError(
                        f"Invalid message on Kafka topic {cfg.topic!r} "
                        f"for model {model_cls.__name__}: {e}"
                    ) from e
                yield parsed
        finally:
            try:
                await consumer.stop()
            except Exception:
                pass

    @staticmethod
    def _encode_key(k: str | bytes | None) -> bytes | None:
        if k is None:
            return None
        if isinstance(k, (bytes, bytearray)):
            return bytes(k)
        return str(k).encode("utf-8")
