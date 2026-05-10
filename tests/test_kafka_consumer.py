import pytest
from unittest.mock import AsyncMock, MagicMock, patch
import json
import pydantic
from core.topic import Topic
from extensions.kafka.runtime_context import KafkaRuntimeContext
from extensions.kafka.config_dict import KafkaConfig
from tests.conftest import AsyncIteratorMock, SUBSCRIBE_DELAY


class Order(pydantic.BaseModel):
    data: str
    count: int = 1


class OrderCreated(Topic[Order]):
    config = KafkaConfig(topic_name="orders-created")


@pytest.fixture
def mock_consumer():
    """Create a mock AIOKafkaConsumer."""
    with patch(
        "extensions.kafka.runtime_context.AIOKafkaConsumer"
    ) as mock_class:
        mock_instance = AsyncMock()
        mock_class.return_value = mock_instance
        yield mock_instance, mock_class


@pytest.mark.asyncio
async def test_consumer_creates_on_subscribe(mock_consumer):
    """Consumer is created when unsafe_sub is called"""
    mock_instance, mock_class = mock_consumer

    # Mock empty async iteration
    mock_instance.__aiter__ = lambda self: AsyncIteratorMock([])

    ctx = KafkaRuntimeContext({OrderCreated}, {OrderCreated})

    # Subscribe (but don't iterate since it's empty)
    async for _ in ctx.unsafe_sub(OrderCreated):
        pass

    mock_class.assert_called_once()
    assert "orders-created" in mock_class.call_args[0][0]  # topic name


@pytest.mark.asyncio
async def test_consumer_receives_and_deserializes(mock_consumer):
    """Consumer receives messages and deserializes to pydantic model"""
    mock_instance, mock_class = mock_consumer

    # Create a mock message
    mock_msg = MagicMock()
    mock_msg.value = json.dumps({"data": "test-order", "count": 42}).encode("utf-8")

    # Mock the async iteration to yield one message
    mock_instance.__aiter__ = lambda self: AsyncIteratorMock([mock_msg])

    ctx = KafkaRuntimeContext({OrderCreated}, {OrderCreated})

    messages = []
    async for msg in ctx.unsafe_sub(OrderCreated):
        messages.append(msg)
        break

    assert len(messages) == 1
    assert isinstance(messages[0], Order)
    assert messages[0].data == "test-order"
    assert messages[0].count == 42


@pytest.mark.asyncio
async def test_consumer_group_id(mock_consumer):
    """Consumer uses correct group_id"""
    mock_instance, mock_class = mock_consumer
    mock_instance.__aiter__ = lambda self: AsyncIteratorMock([])

    ctx = KafkaRuntimeContext(
        {OrderCreated}, {OrderCreated}, group_id_prefix="my-app"
    )

    async for _ in ctx.unsafe_sub(OrderCreated):
        pass

    assert mock_class.call_args[1]["group_id"] == "my-app-orders-created"


@pytest.mark.asyncio
async def test_invalid_json_raises_error(mock_consumer):
    """Invalid JSON raises clear error"""
    mock_instance, mock_class = mock_consumer

    mock_msg = MagicMock()
    mock_msg.value = b"not valid json"

    mock_instance.__aiter__ = lambda self: AsyncIteratorMock([mock_msg])

    ctx = KafkaRuntimeContext({OrderCreated}, {OrderCreated})

    with pytest.raises(ValueError, match="Invalid JSON"):
        async for _ in ctx.unsafe_sub(OrderCreated):
            pass


@pytest.mark.asyncio
async def test_validation_error_raises(mock_consumer):
    """Invalid message structure raises validation error"""
    mock_instance, mock_class = mock_consumer

    mock_msg = MagicMock()
    mock_msg.value = json.dumps({"invalid_field": "value"}).encode("utf-8")

    mock_instance.__aiter__ = lambda self: AsyncIteratorMock([mock_msg])

    ctx = KafkaRuntimeContext({OrderCreated}, {OrderCreated})

    with pytest.raises(ValueError, match="validation failed"):
        async for _ in ctx.unsafe_sub(OrderCreated):
            pass


@pytest.mark.asyncio
async def test_consumer_reused_for_same_topic(mock_consumer):
    """Same consumer is reused for multiple calls to unsafe_sub"""
    mock_instance, mock_class = mock_consumer
    mock_instance.__aiter__ = lambda self: AsyncIteratorMock([])

    ctx = KafkaRuntimeContext({OrderCreated}, {OrderCreated})

    # First call creates consumer
    async for _ in ctx.unsafe_sub(OrderCreated):
        pass

    # Second call should reuse
    async for _ in ctx.unsafe_sub(OrderCreated):
        pass

    # Consumer should only be created once
    mock_class.assert_called_once()


@pytest.mark.asyncio
async def test_stop_closes_consumers(mock_consumer):
    """stop() closes all consumers"""
    mock_instance, mock_class = mock_consumer
    mock_instance.__aiter__ = lambda self: AsyncIteratorMock([])

    ctx = KafkaRuntimeContext({OrderCreated}, {OrderCreated})

    # Create consumer by subscribing
    async for _ in ctx.unsafe_sub(OrderCreated):
        pass

    # Stop should close consumer
    await ctx.stop()

    mock_instance.stop.assert_called_once()
