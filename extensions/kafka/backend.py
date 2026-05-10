from __future__ import annotations
import typing as t
import pydantic
from aiokafka import AIOKafkaProducer, AIOKafkaConsumer

from core.backend import Backend
from core.topic import Topic
from .config_dict import KafkaConfig


class KafkaBackend(Backend):
    """
    Kafka transport adapter built on aiokafka.

    One ``KafkaBackend`` owns:
      * a single shared ``AIOKafkaProducer`` for all outbound topics
      * one ``AIOKafkaConsumer`` per ``subscribe()`` call (one per
        (edge, input topic) pair), so each subscription has its own
        group and can be load-balanced/scaled independently.
    """

    config_cls = KafkaConfig

    def __init__(
        self,
        bootstrap_servers: str,
        *,
        group_id_prefix: str = "edgy",
        producer_kwargs: dict[str, t.Any] | None = None,
        consumer_kwargs: dict[str, t.Any] | None = None,
    ) -> None:
        self.bootstrap_servers = bootstrap_servers
        self.group_id_prefix = group_id_prefix
        self.producer_kwargs = producer_kwargs or {}
        self.consumer_kwargs = consumer_kwargs or {}

        self._producer: AIOKafkaProducer | None = None
        self._consumers: list[AIOKafkaConsumer] = []
        self._output_topics: set[type[Topic]] = set()
        self._input_topics: set[tuple[type[Topic], str]] = set()
        self._started = False

    # ---- registration -------------------------------------------------------

    def register_output(self, topic: type[Topic]) -> None:
        assert isinstance(topic.config, KafkaConfig), (
            f"{topic.__name__}.config must be a KafkaConfig"
        )
        self._output_topics.add(topic)

    def register_input(self, topic: type[Topic], owner: str) -> None:
        assert isinstance(topic.config, KafkaConfig), (
            f"{topic.__name__}.config must be a KafkaConfig"
        )
        self._input_topics.add((topic, owner))

    # ---- lifecycle ----------------------------------------------------------

    async def start(self) -> None:
        if self._started:
            return
        # Only spin up a producer if at least one edge publishes through
        # this backend. Pure-consumer deployments stay lean.
        if self._output_topics:
            self._producer = AIOKafkaProducer(
                bootstrap_servers=self.bootstrap_servers,
                **self.producer_kwargs,
            )
            try:
                await self._producer.start()
            except Exception:
                # ensure we don't leak an unclosed producer if bootstrap fails
                try:
                    await self._producer.stop()
                finally:
                    self._producer = None
                raise
        self._started = True

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
        self._started = False

    # ---- runtime ops --------------------------------------------------------

    async def publish(
        self, topic: type[Topic], data: pydantic.BaseModel
    ) -> None:
        if self._producer is None:
            raise RuntimeError(
                f"KafkaBackend has no producer. Topic {topic.__name__} was "
                f"not registered as an output before start()."
            )
        cfg: KafkaConfig = topic.config  # type: ignore[assignment]
        payload = data.model_dump_json().encode("utf-8")
        key: bytes | None = None
        if cfg.key is not None and callable(cfg.key):
            k = cfg.key(data)
            if isinstance(k, str):
                key = k.encode("utf-8")
            elif isinstance(k, (bytes, bytearray)):
                key = bytes(k)
            elif k is not None:
                key = str(k).encode("utf-8")
        await self._producer.send_and_wait(cfg.topic, payload, key=key)

    async def subscribe(
        self, topic: type[Topic], *, owner: str
    ) -> t.AsyncIterator[pydantic.BaseModel]:
        cfg: KafkaConfig = topic.config  # type: ignore[assignment]
        group_id = cfg.group_id or f"{self.group_id_prefix}.{owner}"
        consumer = AIOKafkaConsumer(
            cfg.topic,
            bootstrap_servers=self.bootstrap_servers,
            group_id=group_id,
            enable_auto_commit=True,
            auto_offset_reset="earliest",
            **self.consumer_kwargs,
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
