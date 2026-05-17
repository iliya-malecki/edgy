from __future__ import annotations
import typing as t
import pydantic
from core.config_dict import ConfigDict


class KafkaConfig(ConfigDict):
    """
    Per-topic Kafka settings. Cluster-wide settings (bootstrap_servers,
    security, ...) live on `KafkaRuntimeContext` and are shared across
    every topic that context handles.

    Parameters
    ----------
    topic:
        Kafka topic name on the wire.
    group_id:
        Consumer group id used when this topic is subscribed to.
        Required and explicit on purpose: a derived default would
        silently change (and trigger replay from the earliest offset)
        whenever the subscribing edge is renamed or moved. Ignored
        when the topic is only used as an output.
    key:
        Optional callable extracting a partition key from the message.
    """

    def __init__(
        self,
        topic: str,
        *,
        group_id: str,
        key: t.Callable[[pydantic.BaseModel], str | bytes | None] | None = None,
    ) -> None:
        self.topic = topic
        self.group_id = group_id
        self.key = key
