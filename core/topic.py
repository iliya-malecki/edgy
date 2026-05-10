from __future__ import annotations
import pydantic
import typing as t

from .runtime_context import RuntimeContext
from .config_dict import ConfigDict
from . import util


class Topic[BM: pydantic.BaseModel]:
    model: type[BM]
    config: ConfigDict

    def __init_subclass__(cls) -> None:
        super().__init_subclass__()
        util.validate_flat_subclassing(cls, Topic)

        # Extract the BaseModel type from generic parameters
        for base in getattr(cls, "__orig_bases__", ()):
            origin = t.get_origin(base)
            if origin is Topic:
                args = t.get_args(base)
                if args:
                    cls.model = args[0]
                break

    @classmethod
    def pub(cls, ctx: RuntimeContext[t.Any, t.Self], data: BM) -> None:
        return ctx.unsafe_pub(cls, data)

    @classmethod
    def sub(cls, ctx: RuntimeContext[t.Self, t.Any]) -> t.AsyncIterable[BM]:
        return ctx.unsafe_sub(cls)
