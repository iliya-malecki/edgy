from __future__ import annotations
import pydantic
import typing as t

from .runtime_context import RuntimeContext


class Topic[BM: pydantic.BaseModel]:
    model: type[BM]
    name: str

    @classmethod
    def _validate_topic_subclass(cls) -> None:
        for base in getattr(cls, "__orig_bases__", ()):
            if t.get_origin(base) is Topic:
                (model,) = t.get_args(base)
                if not isinstance(model, type) or not issubclass(
                    model, pydantic.BaseModel
                ):
                    raise TypeError(
                        f"Invalid Topic model for {cls.__name__}: {model!r}"
                    )
                cls.model = t.cast(type[BM], model)
                return
        raise TypeError(f"Could not infer Topic model for {cls.__name__}")

    def __init_subclass__(cls) -> None:
        super().__init_subclass__()
        cls._validate_topic_subclass()
        cls.name = cls.__name__

    @classmethod
    def pub(cls, ctx: RuntimeContext[t.Any, t.Self], data: BM) -> None:
        return ctx.unsafe_pub(cls, data)

    @classmethod
    def sub(cls, ctx: RuntimeContext[t.Self, t.Any]) -> t.AsyncIterable[BM]:
        return ctx.unsafe_sub(cls)
