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
        Optional explicit consumer group id. If omitted, the context
        derives one from the subscribing edge's qualified name so
        replicas of the same edge load-balance partitions.
    key:
        Optional callable extracting a partition key from the message.
    """

    def __init__(
        self,
        topic: str,
        *,
        group_id: str | None = None,
        key: t.Callable[[pydantic.BaseModel], str | bytes | None] | None = None,
    ) -> None:
        self.topic = topic
        self.group_id = group_id
        self.key = key
