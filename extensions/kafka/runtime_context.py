from __future__ import annotations
import asyncio
import typing as t
import json
import pydantic
from aiokafka import AIOKafkaProducer, AIOKafkaConsumer
from core.runtime_context import RuntimeContext
from core.topic import Topic
from .config_dict import KafkaConfig


# Default configuration constants
DEFAULT_BOOTSTRAP_SERVERS = "localhost:9092"
DEFAULT_GROUP_ID_PREFIX = "edgy"
DEFAULT_AUTO_OFFSET_RESET = "earliest"


class KafkaRuntimeContext(RuntimeContext):
    """
    RuntimeContext implementation using Kafka (aiokafka) as the message broker.
    """

    def __init__(
        self,
        allowed_input: set[type[Topic]],
        allowed_output: set[type[Topic]],
        bootstrap_servers: str = DEFAULT_BOOTSTRAP_SERVERS,
        group_id_prefix: str = DEFAULT_GROUP_ID_PREFIX,
    ) -> None:
        super().__init__(allowed_input, allowed_output)
        self.bootstrap_servers = bootstrap_servers
        self.group_id_prefix = group_id_prefix
        self._producer: AIOKafkaProducer | None = None
        self._consumers: dict[str, AIOKafkaConsumer] = {}

    async def start(self) -> None:
        """Initialize the Kafka producer."""
        if self._producer is None:
            self._producer = AIOKafkaProducer(
                bootstrap_servers=self.bootstrap_servers,
                value_serializer=lambda v: json.dumps(v).encode("utf-8"),
            )
            await self._producer.start()

    async def stop(self) -> None:
        """Cleanup the Kafka producer and consumers."""
        if self._producer is not None:
            await self._producer.stop()
            self._producer = None

        # Stop all consumers
        for consumer in self._consumers.values():
            await consumer.stop()
        self._consumers.clear()

    async def unsafe_pub[M: pydantic.BaseModel](
        self,
        topic: type[Topic[M]],
        data: M,
    ) -> None:
        """Publish message to Kafka topic."""
        assert isinstance(topic.config, KafkaConfig), (
            f"Topic {topic.__name__} must use KafkaConfig"
        )

        # Ensure producer is started
        if self._producer is None:
            await self.start()

        # Get topic name from config
        topic_name = topic.config.get_topic_name(topic)

        # Serialize pydantic model to dict for JSON serialization
        message_dict = data.model_dump()

        # Send to Kafka
        await self._producer.send(topic_name, message_dict)

    async def unsafe_sub[M: pydantic.BaseModel](
        self,
        topic: type[Topic[M]],
    ) -> t.AsyncIterable[M]:
        """Subscribe to Kafka topic and yield deserialized messages."""
        assert isinstance(topic.config, KafkaConfig), (
            f"Topic {topic.__name__} must use KafkaConfig"
        )

        # Get topic name from config
        topic_name = topic.config.get_topic_name(topic)

        # Create or get consumer for this topic
        consumer = await self._get_or_create_consumer(topic_name, topic)

        try:
            async for msg in consumer:
                try:
                    # Deserialize JSON
                    data_dict = json.loads(msg.value.decode("utf-8"))
                    # Validate with pydantic model
                    model_instance = topic.model.model_validate(data_dict)
                    yield model_instance
                except json.JSONDecodeError as e:
                    raise ValueError(
                        f"Invalid JSON received from topic {topic_name}: {e}"
                    ) from e
                except pydantic.ValidationError as e:
                    raise ValueError(
                        f"Message validation failed for topic {topic_name}: {e}"
                    ) from e
        except asyncio.CancelledError:
            # Graceful shutdown on cancellation
            raise

    async def _get_or_create_consumer[
        M: pydantic.BaseModel
    ](
        self,
        topic_name: str,
        topic: type[Topic[M]],
    ) -> AIOKafkaConsumer:
        """Get existing consumer or create new one for the topic."""
        if topic_name in self._consumers:
            return self._consumers[topic_name]

        # Generate consumer group ID
        group_id = f"{self.group_id_prefix}-{topic_name}"

        # Create new consumer
        consumer = AIOKafkaConsumer(
            topic_name,
            bootstrap_servers=self.bootstrap_servers,
            group_id=group_id,
            value_deserializer=lambda v: v,  # We'll deserialize manually
            auto_offset_reset=DEFAULT_AUTO_OFFSET_RESET,
        )

        await consumer.start()
        self._consumers[topic_name] = consumer
        return consumer
