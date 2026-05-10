"""
Runnable two-edge demo over Kafka.

- A topic carries one pydantic model and one Kafka topic name.
- An edge declares which topics it may consume from / publish to via
  its generic parameters. The Runtime wires each edge to its own
  `KafkaRuntimeContext`.

Run a local Kafka, e.g.

    docker run -p 9092:9092 apache/kafka:latest

then:

    poetry run python examples.py

In another shell, publish a JSON payload to `orders.created`, e.g.

    {"data": "hi"}     # -> flows to orders.updated
    {"data": "cancel"} # -> flows to orders.cancelled
"""

from __future__ import annotations
import asyncio
import pydantic

from core import Topic, Edge, Runtime
from extensions.kafka.runtime_context import KafkaRuntimeContext
from extensions.kafka.config_dict import KafkaConfig


BOOTSTRAP_SERVERS = "localhost:9092"


class Order(pydantic.BaseModel):
    data: str


class Delivery(pydantic.BaseModel):
    value: int


class OrderCreated(Topic[Order]):
    config = KafkaConfig(topic="orders.created")


class OrderUpdated(Topic[Order]):
    config = KafkaConfig(topic="orders.updated")


class OrderCancelled(Topic[Delivery]):
    config = KafkaConfig(topic="orders.cancelled")


class OrderProcessor(Edge[OrderCreated, OrderUpdated | OrderCancelled]):
    async def process(self) -> None:
        async for order in OrderCreated.sub(self.ctx):
            print(f"[OrderProcessor] {order!r}")
            if order.data == "cancel":
                await OrderCancelled.pub(self.ctx, Delivery(value=42))
            else:
                await OrderUpdated.pub(self.ctx, order)


class CancellationLogger(Edge[OrderCancelled, OrderCancelled]):
    async def process(self) -> None:
        async for d in OrderCancelled.sub(self.ctx):
            print(f"[CancellationLogger] cancelled value={d.value}")


async def main() -> None:
    rt = Runtime()
    rt.add(OrderProcessor, KafkaRuntimeContext, bootstrap_servers=BOOTSTRAP_SERVERS)
    rt.add(CancellationLogger, KafkaRuntimeContext, bootstrap_servers=BOOTSTRAP_SERVERS)
    await rt.run()


if __name__ == "__main__":
    asyncio.run(main())
