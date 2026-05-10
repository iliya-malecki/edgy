import pytest
import pydantic
from core.topic import Topic
from extensions.kafka.config_dict import KafkaConfig


class Order(pydantic.BaseModel):
    data: str


class OrderCreated(Topic[Order]):
    pass


def test_kafka_config_with_topic_name():
    """KafkaConfig can be instantiated with topic_name"""
    config = KafkaConfig(topic_name="my-orders")
    assert config.topic_name == "my-orders"


def test_kafka_config_default_topic_name():
    """Default topic_name is None"""
    config = KafkaConfig()
    assert config.topic_name is None


def test_get_topic_name_returns_explicit_name():
    """get_topic_name returns explicit name if set"""
    config = KafkaConfig(topic_name="custom-topic")
    assert config.get_topic_name(OrderCreated) == "custom-topic"


def test_get_topic_name_returns_class_name():
    """get_topic_name returns class name if not set"""
    config = KafkaConfig()
    assert config.get_topic_name(OrderCreated) == "OrderCreated"
