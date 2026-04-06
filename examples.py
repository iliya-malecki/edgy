"""
- topic must hold the knowledge of what types you can post there
- pubsub edge must hold the knowledge of what topics it can post to

"""

import pydantic
from core import Topic, Edge


class Order(pydantic.BaseModel):
    data: str


class Delivery(pydantic.BaseModel):
    value: int


class OrderCreated(Topic[Order]): ...


class OrderUpldated(Topic[Order]): ...


class OrderCancelled(Topic[Delivery]): ...


class OrderRemoved(Topic[Order]): ...


class OrderProcessor(Edge[OrderCreated, OrderUpldated | OrderCancelled]):
    async def process(self):
        async for order in OrderCreated.sub(self.ctx):
            OrderCancelled.pub(self.ctx, Delivery(value=42))
