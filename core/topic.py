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
        # Auto-derive `model` from the generic argument so users only
        # have to set `config`. Skip if explicitly provided.
        if "model" not in cls.__dict__:
            for base in getattr(cls, "__orig_bases__", ()):
                if t.get_origin(base) is Topic:
                    args = t.get_args(base)
                    if (
                        args
                        and isinstance(args[0], type)
                        and issubclass(args[0], pydantic.BaseModel)
                    ):
                        cls.model = args[0]
                        break

    @classmethod
    async def pub(cls, ctx: RuntimeContext[t.Any, t.Self], data: BM) -> None:
        await ctx.unsafe_pub(cls, data)

    @classmethod
    def sub(cls, ctx: RuntimeContext[t.Self, t.Any]) -> t.AsyncIterator[BM]:
        return ctx.unsafe_sub(cls)
