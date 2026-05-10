from core.config_dict import ConfigDict


class KafkaConfig(ConfigDict):
    """
    Configuration for Kafka topics.

    Args:
        topic_name: Override the Kafka topic name. If None, uses the class name.
        serializer: Serialization format. Currently only "json" is supported.
    """

    def __init__(
        self,
        topic_name: str | None = None,
        serializer: str = "json",
    ) -> None:
        self.topic_name = topic_name
        self.serializer = serializer

    def get_topic_name(self, topic_class: type) -> str:
        """Get the Kafka topic name for a topic class."""
        if self.topic_name is not None:
            return self.topic_name
        return topic_class.__name__
