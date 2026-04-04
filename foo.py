"""
- topic must hold the knowledge of what types you can post there
- pubsub edge must hold the knowledge of what topics it can post to

"""

from __future__ import annotations
import sys
import pydantic
import typing as t

Topic_co = t.TypeVar("Topic_co", covariant=True, bound="Topic")


class Topic[BM: pydantic.BaseModel]:
    model: type[BM]

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
                cls.model = t.cast(type[BM], model)
                return
        raise TypeError(f"Could not infer Topic model for {cls.__name__}")

    @classmethod
    def pub(cls, runtime: Runtime[t.Any, t.Self], data: BM) -> None: ...
    @classmethod
    def sub(cls, runtime: Runtime[t.Self, t.Any]) -> t.AsyncIterable[BM]: ...


Input = t.TypeVar("Input", contravariant=True)
Output = t.TypeVar("Output", contravariant=True)


class Runtime(t.Generic[Input, Output]):
    """
    Runtime context holding allowed input and output topics,
    raw client connections and other runtime dependencies.
    """

    pass


class Edge(t.Generic[Input, Output]):
    runtime: Runtime[Input, Output]

    async def process(self): ...


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
        async for order in OrderCreated.sub(self.runtime):
            OrderCancelled.pub(self.runtime, Delivery(value=42))
