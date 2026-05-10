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

    Owns one shared `AIOKafkaProducer` for all outbound topics and one
    `AIOKafkaConsumer` per `unsafe_sub` call, so every (edge, input
    topic) pair has its own group and can scale independently.
    """

    def __init__(
        self,
        allowed_input: set[type[Topic]],
        allowed_output: set[type[Topic]],
        owner: str = "",
        *,
        bootstrap_servers: str,
        group_id_prefix: str = "edgy",
    ) -> None:
        super().__init__(allowed_input, allowed_output, owner=owner)
        self.bootstrap_servers = bootstrap_servers
        self.group_id_prefix = group_id_prefix
        self._producer: AIOKafkaProducer | None = None
        self._consumers: list[AIOKafkaConsumer] = []

    async def start(self) -> None:
        if self.allowed_output:
            self._producer = AIOKafkaProducer(
                bootstrap_servers=self.bootstrap_servers,
            )
            await self._producer.start()

    async def stop(self) -> None:
        for c in self._consumers:
            try:
                await c.stop()
            except Exception:
                pass
        self._consumers.clear()
        if self._producer is not None:
            await self._producer.stop()
            self._producer = None

    async def unsafe_pub[M: pydantic.BaseModel](
        self,
        topic: type[Topic[M]],
        data: M,
    ) -> None:
        assert isinstance(topic.config, KafkaConfig)
        if self._producer is None:
            raise RuntimeError(
                f"No Kafka producer; topic {topic.__name__} was not "
                f"declared as an output of {self.owner!r}."
            )
        cfg = topic.config
        payload = data.model_dump_json().encode("utf-8")
        key = self._encode_key(cfg.key(data) if cfg.key else None)
        await self._producer.send_and_wait(cfg.topic, payload, key=key)

    async def unsafe_sub[M: pydantic.BaseModel](
        self,
        topic: type[Topic[M]],
    ) -> t.AsyncIterator[M]:
        assert isinstance(topic.config, KafkaConfig)
        cfg = topic.config
        group_id = cfg.group_id or f"{self.group_id_prefix}.{self.owner}"
        consumer = AIOKafkaConsumer(
            cfg.topic,
            bootstrap_servers=self.bootstrap_servers,
            group_id=group_id,
            enable_auto_commit=True,
            auto_offset_reset="earliest",
        )
        await consumer.start()
        self._consumers.append(consumer)
        model_cls = topic.model
        try:
            async for msg in consumer:
                yield model_cls.model_validate_json(msg.value)
        finally:
            try:
                await consumer.stop()
            except Exception:
                pass
            if consumer in self._consumers:
                self._consumers.remove(consumer)

    @staticmethod
    def _encode_key(k: str | bytes | None) -> bytes | None:
        if k is None:
            return None
        if isinstance(k, (bytes, bytearray)):
            return bytes(k)
        return str(k).encode("utf-8")
