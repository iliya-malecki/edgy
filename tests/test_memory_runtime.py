import pytest
import pydantic
import asyncio
from core.topic import Topic
from extensions.memory.runtime_context import InMemoryRuntimeContext
from extensions.memory.config_dict import InMemoryConfig


class Order(pydantic.BaseModel):
    data: str


class OrderTopic(Topic[Order]):
    config = InMemoryConfig()


@pytest.mark.asyncio
async def test_can_publish_and_subscribe():
    """Can publish message to in-memory topic and receive it"""
    ctx = InMemoryRuntimeContext({OrderTopic}, {OrderTopic})

    messages = []

    async def subscribe():
        async for msg in ctx.unsafe_sub(OrderTopic):
            messages.append(msg)
            if len(messages) >= 1:
                break

    # Start subscriber first
    task = asyncio.create_task(subscribe())
    await asyncio.sleep(0.01)  # Let subscriber start

    # Then publish
    await ctx.unsafe_pub(OrderTopic, Order(data="test"))

    # Wait for subscriber to receive
    await asyncio.wait_for(task, timeout=1.0)

    assert len(messages) == 1
    assert isinstance(messages[0], Order)
    assert messages[0].data == "test"


@pytest.mark.asyncio
async def test_multiple_subscribers_receive_broadcast():
    """Multiple subscribers both receive message (broadcast)"""
    ctx = InMemoryRuntimeContext({OrderTopic}, {OrderTopic})

    # Start two subscribers
    sub1_messages = []
    sub2_messages = []

    async def subscriber1():
        async for msg in ctx.unsafe_sub(OrderTopic):
            sub1_messages.append(msg)
            if len(sub1_messages) >= 1:
                break

    async def subscriber2():
        async for msg in ctx.unsafe_sub(OrderTopic):
            sub2_messages.append(msg)
            if len(sub2_messages) >= 1:
                break

    # Start subscribers
    task1 = asyncio.create_task(subscriber1())
    task2 = asyncio.create_task(subscriber2())

    # Give subscribers time to start waiting
    await asyncio.sleep(0.01)

    # Publish message
    await ctx.unsafe_pub(OrderTopic, Order(data="broadcast"))

    # Wait for both subscribers to receive
    await asyncio.wait_for(asyncio.gather(task1, task2), timeout=1.0)

    assert len(sub1_messages) == 1
    assert len(sub2_messages) == 1
    assert sub1_messages[0].data == "broadcast"
    assert sub2_messages[0].data == "broadcast"


@pytest.mark.asyncio
async def test_messages_are_pydantic_instances():
    """Messages are pydantic model instances"""
    ctx = InMemoryRuntimeContext({OrderTopic}, {OrderTopic})

    received = []

    async def subscribe():
        async for msg in ctx.unsafe_sub(OrderTopic):
            received.append(msg)
            break

    task = asyncio.create_task(subscribe())
    await asyncio.sleep(0.01)

    await ctx.unsafe_pub(OrderTopic, Order(data="validation"))

    await asyncio.wait_for(task, timeout=1.0)

    assert len(received) == 1
    assert isinstance(received[0], pydantic.BaseModel)
    assert hasattr(received[0], 'data')
