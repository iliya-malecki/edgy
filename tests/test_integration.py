import pytest
import asyncio
import pydantic
from core.runtime import Runtime
from core.topic import Topic
from core.edge import Edge
from extensions.memory.runtime_context import InMemoryRuntimeContext
from extensions.memory.config_dict import InMemoryConfig
from tests.conftest import (
    STARTUP_DELAY,
    SUBSCRIBE_DELAY,
    PUBLISH_DELAY,
    PROCESSING_DELAY,
    SHUTDOWN_TIMEOUT,
    MESSAGE_TIMEOUT,
)


class Order(pydantic.BaseModel):
    data: str


class ProcessedOrder(pydantic.BaseModel):
    data: str
    processed: bool = True


class OrderCreated(Topic[Order]):
    config = InMemoryConfig()


class OrderProcessed(Topic[ProcessedOrder]):
    config = InMemoryConfig()


class OrderProcessor(Edge[OrderCreated, OrderProcessed]):
    """Edge that transforms Order to ProcessedOrder."""

    async def process(self):
        async for order in OrderCreated.sub(self.ctx):
            processed = ProcessedOrder(data=f"processed:{order.data}")
            await OrderProcessed.pub(self.ctx, processed)


@pytest.mark.asyncio
async def test_end_to_end_message_flow():
    """Full roundtrip: publish -> edge processes -> subscribe receives"""
    runtime = Runtime()
    runtime.add(OrderProcessor, InMemoryRuntimeContext)

    # Get the context from the edge
    ctx = runtime.edges[0].ctx

    # Start runtime in background
    start_task = asyncio.create_task(runtime.start())
    await asyncio.sleep(STARTUP_DELAY)  # Let edge start subscribing

    # Set up output subscriber FIRST (before publishing)
    received = []

    async def collect_output():
        async for msg in OrderProcessed.sub(ctx):
            received.append(msg)
            break

    output_task = asyncio.create_task(collect_output())
    await asyncio.sleep(SUBSCRIBE_DELAY)  # Let subscriber start

    # Now publish a message
    await OrderCreated.pub(ctx, Order(data="test-order"))

    # Wait for output with timeout
    try:
        await asyncio.wait_for(output_task, timeout=MESSAGE_TIMEOUT)
    except asyncio.TimeoutError:
        pass

    # Stop runtime
    await runtime.stop()
    try:
        await asyncio.wait_for(start_task, timeout=SHUTDOWN_TIMEOUT)
    except asyncio.CancelledError:
        pass

    # Verify message was processed
    assert len(received) == 1
    assert received[0].data == "processed:test-order"
    assert received[0].processed is True


@pytest.mark.asyncio
async def test_multiple_messages_flow():
    """Multiple messages flow through the system"""
    runtime = Runtime()
    runtime.add(OrderProcessor, InMemoryRuntimeContext)

    ctx = runtime.edges[0].ctx

    start_task = asyncio.create_task(runtime.start())
    await asyncio.sleep(STARTUP_DELAY)

    # Set up output subscriber first
    received = []

    async def collect_output():
        async for msg in OrderProcessed.sub(ctx):
            received.append(msg)
            if len(received) >= 3:
                break

    output_task = asyncio.create_task(collect_output())
    await asyncio.sleep(SUBSCRIBE_DELAY)

    # Publish multiple messages
    for i in range(3):
        await OrderCreated.pub(ctx, Order(data=f"order-{i}"))
        await asyncio.sleep(PUBLISH_DELAY)  # Small delay between publishes

    # Wait for all messages
    try:
        await asyncio.wait_for(output_task, timeout=MESSAGE_TIMEOUT * 2)
    except asyncio.TimeoutError:
        pass

    await runtime.stop()
    try:
        await asyncio.wait_for(start_task, timeout=SHUTDOWN_TIMEOUT)
    except asyncio.CancelledError:
        pass

    assert len(received) == 3
    for i, msg in enumerate(received):
        assert msg.data == f"processed:order-{i}"


@pytest.mark.asyncio
async def test_type_safety_at_runtime():
    """Runtime guards prevent publishing to wrong topic"""
    runtime = Runtime()
    runtime.add(OrderProcessor, InMemoryRuntimeContext)

    ctx = runtime.edges[0].ctx

    # OrderProcessor only allows OrderCreated as input
    # and OrderProcessed as output

    # This should work - OrderCreated is in allowed_input
    assert ctx.guard_input_topic(OrderCreated)

    # This should work - OrderProcessed is in allowed_output
    assert ctx.guard_output_topic(OrderProcessed)

    # OrderProcessed is NOT in allowed_input
    assert not ctx.guard_input_topic(OrderProcessed)

    # OrderCreated is NOT in allowed_output
    assert not ctx.guard_output_topic(OrderCreated)
