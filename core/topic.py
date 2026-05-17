from __future__ import annotations
import pydantic
import typing as t

from .runtime_context import RuntimeContext
from .config_dict import ConfigDict
from . import util


class Topic[BM: pydantic.BaseModel]:
    config: ConfigDict

    def __init_subclass__(cls) -> None:
        super().__init_subclass__()
        util.validate_flat_subclassing(cls, Topic)

    @classmethod
    def get_model(cls) -> type[pydantic.BaseModel]:
        """
        Resolve the `BM` generic argument lazily, on first call. Kept off
        `__init_subclass__` on purpose: doing it eagerly forces every
        topic module to import every domain model + transport config at
        class-definition time, which means a graph-extraction tool that
        wants to enumerate topics has to import the rest of the app.
        Doing it here means the topic class can be defined with a
        forward-ref generic and still resolve when a transport actually
        needs the model.
        """
        for base in getattr(cls, "__orig_bases__", ()):
            if t.get_origin(base) is Topic:
                args = t.get_args(base)
                if not args:
                    raise TypeError(
                        f"{cls.__name__} has no Topic[...] generic argument."
                    )
                model = args[0]
                if not (
                    isinstance(model, type)
                    and issubclass(model, pydantic.BaseModel)
                ):
                    raise TypeError(
                        f"{cls.__name__}'s generic argument {model!r} did "
                        f"not resolve to a pydantic.BaseModel subclass."
                    )
                return model
        raise TypeError(f"{cls.__name__} does not extend Topic[SomeModel].")

    @classmethod
    async def pub(cls, ctx: RuntimeContext[t.Any, t.Self], data: BM) -> None:
        await ctx.unsafe_pub(cls, data)

    @classmethod
    def sub(cls, ctx: RuntimeContext[t.Self, t.Any]) -> t.AsyncIterator[BM]:
        return ctx.unsafe_sub(cls)
