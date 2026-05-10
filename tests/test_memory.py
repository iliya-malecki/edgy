"""
End-to-end coverage using the in-memory transport (no mocks).
"""
from __future__ import annotations
import asyncio
import pydantic
import pytest

from core import Topic, Edge, Runtime
from extensions.memory.runtime_context import InMemoryRuntimeContext
from extensions.memory.config_dict import InMemoryConfig


class Order(pydantic.BaseModel):
    data: str


class OrderCreated(Topic[Order]):
    config = InMemoryConfig()


class OrderProcessed(Topic[Order]):
    config = InMemoryConfig()


class Processor(Edge[OrderCreated, OrderProcessed]):
    async def process(self) -> None:
        async for order in OrderCreated.sub(self.ctx):
            await OrderProcessed.pub(
                self.ctx, Order(data=f"processed:{order.data}")
            )


@pytest.fixture(autouse=True)
def _reset_bus():
    InMemoryRuntimeContext.reset()
    yield
    InMemoryRuntimeContext.reset()


@pytest.mark.asyncio
async def test_in_memory_end_to_end():
    rt = Runtime()
    rt.add(Processor, InMemoryRuntimeContext)

    # Side ctx so the test can publish into the bus without being an edge.
    side = InMemoryRuntimeContext(
        allowed_input={OrderProcessed},
        allowed_output={OrderCreated},
        owner="test",
    )

    received: list[Order] = []

    async def collect():
        async for o in OrderProcessed.sub(side):
            received.append(o)
            if len(received) >= 2:
                break

    run_task = asyncio.create_task(rt.run())
    collect_task = asyncio.create_task(collect())
    await asyncio.sleep(0.01)  # let subscribers attach

    await OrderCreated.pub(side, Order(data="a"))
    await OrderCreated.pub(side, Order(data="b"))

    await asyncio.wait_for(collect_task, timeout=0.5)
    run_task.cancel()
    try:
        await run_task
    except asyncio.CancelledError:
        pass

    assert [o.data for o in received] == ["processed:a", "processed:b"]


@pytest.mark.asyncio
async def test_broadcast_to_multiple_subscribers():
    """Two independent subscriptions on the same topic both receive."""
    ctx_a = InMemoryRuntimeContext(
        allowed_input={OrderCreated},
        allowed_output=set(),
        owner="a",
    )
    ctx_b = InMemoryRuntimeContext(
        allowed_input={OrderCreated},
        allowed_output={OrderCreated},
        owner="b",
    )

    seen_a: list[Order] = []
    seen_b: list[Order] = []

    async def consume(ctx, sink):
        async for o in OrderCreated.sub(ctx):
            sink.append(o)
            break

    ta = asyncio.create_task(consume(ctx_a, seen_a))
    tb = asyncio.create_task(consume(ctx_b, seen_b))
    await asyncio.sleep(0.01)

    await OrderCreated.pub(ctx_b, Order(data="hi"))

    await asyncio.wait_for(asyncio.gather(ta, tb), timeout=0.5)
    assert seen_a[0].data == "hi"
    assert seen_b[0].data == "hi"
