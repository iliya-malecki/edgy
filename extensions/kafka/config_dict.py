from __future__ import annotations
from core.config_dict import ConfigDict


class KafkaConfig(ConfigDict):
    """
    Per-topic Kafka configuration.

    Cluster-wide settings (bootstrap_servers, security, ...) live on
    the ``KafkaBackend`` and are shared by all topics on that backend.
    Only properties that are inherently per-topic belong here.

    Parameters
    ----------
    topic:
        The Kafka topic name on the wire.
    group_id:
        Optional explicit consumer group id. If not provided, the
        backend derives one from the subscribing edge's qualified name,
        which gives each edge its own group (so multiple replicas of
        the same edge load-balance partitions).
    key:
        Optional callable extracting a partition key (bytes) from the
        message. ``None`` means messages are distributed round-robin.
    """

    def __init__(
        self,
        topic: str,
        *,
        group_id: str | None = None,
        key: object | None = None,
    ) -> None:
        self.topic = topic
        self.group_id = group_id
        self.key = key
