"""
- topic must hold the knowledge of what types you can post there
- pubsub edge must hold the knowledge of what topics it can post to

"""

from __future__ import annotations
import sys
import pydantic
import typing as t

BM_co = t.TypeVar("BM_co", covariant=True, bound=pydantic.BaseModel)
Topic_co = t.TypeVar("Topic_co", covariant=True, bound="Topic")


class Topic(t.Generic[BM_co]):
    model: type[BM_co]

    def __init_subclass__(cls) -> None:
        super().__init_subclass__()
        for base in getattr(cls, "__orig_bases__", ()):
            if t.get_origin(base) is Topic:
                (model,) = t.get_args(base)
                if isinstance(model, t.ForwardRef):
                    type_params = getattr(cls, "__type_params__", ())
                    model = model._evaluate(
                        globalns=sys.modules[cls.__module__].__dict__,
                        localns=None,
                        type_params=type_params,
                        recursive_guard=frozenset(),
                    )
                if not isinstance(model, type) or not issubclass(
                    model, pydantic.BaseModel
                ):
                    raise TypeError(
                        f"Invalid Topic model for {cls.__name__}: {model!r}"
                    )
                cls.model = t.cast(type[BM_co], model)
                return
        raise TypeError(f"Could not infer Topic model for {cls.__name__}")


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


class OrderCreated(Topic[Order]): ...


class OrderUpldated(Topic[Order]): ...


class OrderCancelled(Topic[Delivery]): ...


class OrderRemoved(Topic[Order]): ...


class OrderProcessor(Edge[OrderCreated, OrderUpldated | OrderCancelled]):
    def process(self, pub, sub):
        for order in sub(OrderCreated):
            pub(OrderUpldated, order)
