"""
- topic must hold the knowledge of what types you can post there
- pubsub edge must hold the knowledge of what topics it can post to

"""

from __future__ import annotations
import pydantic
import typing as t

BM_co = t.TypeVar("BM_co", covariant=True, bound=pydantic.BaseModel)
Topic_co = t.TypeVar("Topic_co", covariant=True, bound="Topic")


class Topic(t.Generic[BM_co]):
    model: type[BM_co]

    def __init_subclass__(cls, model) -> None:
        cls.model = model


@t.final
class Pub(t.Protocol, t.Generic[Topic_co]):
    def __call__[M: pydantic.BaseModel](
        self: Pub[Topic[M]],
        # type variance magic, i want unions resolved in the covariant way, not function-param-contravariant
        topic: type[Topic_co],  # type: ignore
        data: M,
    ) -> None: ...


@t.final
class Sub(t.Protocol, t.Generic[Topic_co]):
    def __call__[M: pydantic.BaseModel](
        self: Sub[Topic[M]],
        # type variance magic, i want unions resolved in the covariant way, not function-param-contravariant
        topic: type[Topic_co],  # type: ignore
    ) -> t.Iterator[M]: ...


class Edge[
    Input: Topic,
    Output: Topic,
]:
    def process(self, pub: Pub[Output], sub: Sub[Input]): ...


class Order(pydantic.BaseModel):
    data: str


class Delivery(pydantic.BaseModel):
    value: int


class OrderCreated(Topic[Order], model=Order): ...


class OrderUpldated(Topic[Order], model=Order): ...


class OrderCancelled(Topic[Delivery], model=Delivery): ...


class OrderRemoved(Topic[Order], model=Order): ...


class OrderProcessor(Edge[OrderCreated, OrderUpldated | OrderCancelled]):
    def process(self, pub, sub):
        for order in sub(OrderCreated):
            pub(OrderUpldated, order)
