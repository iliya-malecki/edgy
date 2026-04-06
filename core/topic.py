from __future__ import annotations
import sys
import pydantic
import typing as t

from .runtime_context import RuntimeContext

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
    def pub(cls, ctx: RuntimeContext[t.Any, t.Self], data: BM) -> None: ...
    @classmethod
    def sub(cls, ctx: RuntimeContext[t.Self, t.Any]) -> t.AsyncIterable[BM]: ...
