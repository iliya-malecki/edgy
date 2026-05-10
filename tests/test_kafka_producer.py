import pytest
from unittest.mock import AsyncMock, MagicMock, patch
import pydantic
from core.topic import Topic
from extensions.kafka.runtime_context import KafkaRuntimeContext
from extensions.kafka.config_dict import KafkaConfig


class Order(pydantic.BaseModel):
    data: str
    count: int = 1


class OrderCreated(Topic[Order]):
    config = KafkaConfig(topic_name="orders-created")


class OrderEvent(Topic[Order]):
    config = KafkaConfig()  # Uses class name


@pytest.fixture
def mock_producer():
    """Create a mock AIOKafkaProducer."""
    with patch(
        "extensions.kafka.runtime_context.AIOKafkaProducer"
    ) as mock_class:
        mock_instance = AsyncMock()
        mock_class.return_value = mock_instance
        yield mock_instance, mock_class


@pytest.mark.asyncio
async def test_producer_initializes_on_start(mock_producer):
    """Producer is initialized when start() is called"""
    mock_instance, mock_class = mock_producer

    ctx = KafkaRuntimeContext({OrderCreated}, {OrderCreated})
    await ctx.start()

    mock_class.assert_called_once()
    mock_instance.start.assert_called_once()


@pytest.mark.asyncio
async def test_producer_stops_on_stop(mock_producer):
    """Producer is stopped when stop() is called"""
    mock_instance, _ = mock_producer

    ctx = KafkaRuntimeContext({OrderCreated}, {OrderCreated})
    await ctx.start()
    await ctx.stop()

    mock_instance.stop.assert_called_once()


@pytest.mark.asyncio
async def test_unsafe_pub_sends_to_correct_topic(mock_producer):
    """Message is sent to the correct Kafka topic"""
    mock_instance, _ = mock_producer

    ctx = KafkaRuntimeContext({OrderCreated}, {OrderCreated})
    await ctx.start()

    order = Order(data="test-order", count=42)
    await ctx.unsafe_pub(OrderCreated, order)

    mock_instance.send.assert_called_once()
    call_args = mock_instance.send.call_args
    assert call_args[0][0] == "orders-created"  # topic name


@pytest.mark.asyncio
async def test_unsafe_pub_serializes_to_json(mock_producer):
    """Message is serialized as JSON"""
    mock_instance, mock_class = mock_producer

    ctx = KafkaRuntimeContext({OrderCreated}, {OrderCreated})
    await ctx.start()

    order = Order(data="test", count=5)
    await ctx.unsafe_pub(OrderCreated, order)

    # Check the value_serializer was set up
    assert mock_class.call_args[1]["value_serializer"] is not None

    # Check the data sent is a dict (ready for JSON serialization)
    call_args = mock_instance.send.call_args
    sent_data = call_args[0][1]
    assert sent_data == {"data": "test", "count": 5}


@pytest.mark.asyncio
async def test_topic_name_defaults_to_class_name(mock_producer):
    """When topic_name not set, uses class name"""
    mock_instance, _ = mock_producer

    ctx = KafkaRuntimeContext({OrderEvent}, {OrderEvent})
    await ctx.start()

    await ctx.unsafe_pub(OrderEvent, Order(data="test"))

    call_args = mock_instance.send.call_args
    assert call_args[0][0] == "OrderEvent"  # class name


@pytest.mark.asyncio
async def test_lazy_initialization(mock_producer):
    """Producer is created lazily on first publish"""
    mock_instance, mock_class = mock_producer

    ctx = KafkaRuntimeContext({OrderCreated}, {OrderCreated})

    # Producer not created yet
    mock_class.assert_not_called()

    # Publish triggers initialization
    await ctx.unsafe_pub(OrderCreated, Order(data="lazy"))

    mock_class.assert_called_once()
    mock_instance.start.assert_called_once()
