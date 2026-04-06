from __future__ import annotations
import typing as t
from .topic import Topic


Input = t.TypeVar("Input", contravariant=True, bound=Topic)
Output = t.TypeVar("Output", contravariant=True, bound=Topic)


class RuntimeContext(t.Generic[Input, Output]):
    """
    Runtime context holding allowed input and output topics,
    raw client connections and other runtime dependencies.
    """

    pass
