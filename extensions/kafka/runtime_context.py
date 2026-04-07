import typing as t
from core.runtime_context import RuntimeContext
from core.topic import Topic
from .config_dict import KafkaConfig


class KafkaRuntimeContext(RuntimeContext):
    def unsafe_pub(self, topic: type[Topic], data: t.Any) -> None:
        assert isinstance(topic.config, KafkaConfig)
        # TODO: impl
        return super().unsafe_pub(topic, data)

    def unsafe_sub(self, topic: type[Topic]) -> t.AsyncIterable[t.Any]:
        assert isinstance(topic.config, KafkaConfig)
        # TODO: impl
        return super().unsafe_sub(topic)
