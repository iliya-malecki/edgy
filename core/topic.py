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

    @classmethod
    def pub(cls, ctx: RuntimeContext[t.Any, t.Self], data: BM) -> None:
        return ctx.unsafe_pub(cls, data)

    @classmethod
    def sub(cls, ctx: RuntimeContext[t.Self, t.Any]) -> t.AsyncIterable[BM]:
        return ctx.unsafe_sub(cls)
