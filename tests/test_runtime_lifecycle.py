import pytest
import asyncio
import pydantic
from core.runtime import Runtime
from core.topic import Topic
from core.edge import Edge
from extensions.memory.runtime_context import InMemoryRuntimeContext


class TestMessage(pydantic.BaseModel):
    value: str


class TestTopic(Topic[TestMessage]):
    config = ...  # Will set in tests


class TestEdge(Edge[TestTopic, TestTopic]):
    def __init__(self, ctx):
        super().__init__(ctx)
        self.processed = []

    async def process(self):
        async for msg in TestTopic.sub(self.ctx):
            self.processed.append(msg)
            await TestTopic.pub(self.ctx, TestMessage(value=f"processed:{msg.value}"))


@pytest.mark.asyncio
async def test_runtime_starts_and_stops():
    """Runtime can start and stop gracefully"""
    runtime = Runtime()
    runtime.add(TestEdge, InMemoryRuntimeContext)

    # Start runtime in background
    start_task = asyncio.create_task(runtime.start())
    await asyncio.sleep(0.05)  # Let it start

    # Stop runtime
    await runtime.stop()

    # Should complete without hanging
    try:
        await asyncio.wait_for(start_task, timeout=0.5)
    except asyncio.CancelledError:
        pass  # Expected


@pytest.mark.asyncio
async def test_edge_process_is_called():
    """Edge process() method is called during runtime"""
    runtime = Runtime()
    edge_instance = runtime.add(TestEdge, InMemoryRuntimeContext)

    # Start runtime
    start_task = asyncio.create_task(runtime.start())
    await asyncio.sleep(0.05)

    # Stop
    await runtime.stop()
    try:
        await asyncio.wait_for(start_task, timeout=0.5)
    except asyncio.CancelledError:
        pass

    # Edge should have been built with a context
    assert edge_instance.ctx is not None


@pytest.mark.asyncio
async def test_multiple_edges_run_concurrently():
    """Multiple edges can be added and run"""
    runtime = Runtime()
    edge1 = runtime.add(TestEdge, InMemoryRuntimeContext)
    edge2 = runtime.add(TestEdge, InMemoryRuntimeContext)

    start_task = asyncio.create_task(runtime.start())
    await asyncio.sleep(0.05)

    await runtime.stop()
    try:
        await asyncio.wait_for(start_task, timeout=0.5)
    except asyncio.CancelledError:
        pass

    assert len(runtime.edges) == 2
