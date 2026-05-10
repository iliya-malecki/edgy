"""
Runnable example wiring two edges over Kafka.

- topic must hold the knowledge of what types you can post there
- pubsub edge must hold the knowledge of what topics it can post to

Run a local Kafka (e.g. `docker run -p 9092:9092 ...`) then:

    poetry run python examples.py

In another shell, publish a message via any kafka client to the
"orders.created" topic with body like {"data": "hello"}.
"""

from __future__ import annotations
import asyncio
import pydantic

from core import Topic, Edge, Runtime
from extensions.kafka import KafkaConfig, KafkaBackend


# ---- domain models ----------------------------------------------------------


class Order(pydantic.BaseModel):
    data: str


class Delivery(pydantic.BaseModel):
    value: int


# ---- topics -----------------------------------------------------------------


class OrderCreated(Topic[Order]):
    config = KafkaConfig(topic="orders.created")


class OrderUpdated(Topic[Order]):
    config = KafkaConfig(topic="orders.updated")


class OrderCancelled(Topic[Delivery]):
    config = KafkaConfig(topic="orders.cancelled")


class OrderRemoved(Topic[Order]):
    config = KafkaConfig(topic="orders.removed")


# ---- edges ------------------------------------------------------------------


class OrderProcessor(Edge[OrderCreated, OrderUpdated | OrderCancelled]):
    async def process(self) -> None:
        async for order in OrderCreated.sub(self.ctx):
            print(f"[OrderProcessor] got {order!r}")
            if order.data == "cancel":
                await OrderCancelled.pub(self.ctx, Delivery(value=42))
            else:
                await OrderUpdated.pub(self.ctx, order)


class CancellationLogger(Edge[OrderCancelled, OrderRemoved]):
    async def process(self) -> None:
        async for delivery in OrderCancelled.sub(self.ctx):
            print(f"[CancellationLogger] cancelled with value={delivery.value}")
            await OrderRemoved.pub(self.ctx, Order(data=f"removed:{delivery.value}"))


# ---- entrypoint -------------------------------------------------------------


async def main() -> None:
    rt = Runtime()
    rt.register_backend(KafkaBackend(bootstrap_servers="localhost:9092"))
    rt.add(OrderProcessor)
    rt.add(CancellationLogger)
    await rt.run()


if __name__ == "__main__":
    asyncio.run(main())
