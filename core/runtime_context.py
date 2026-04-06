from __future__ import annotations
import typing as t


Input = t.TypeVar("Input", contravariant=True)
Output = t.TypeVar("Output", contravariant=True)


class RuntimeContext(t.Generic[Input, Output]):
    """
    Runtime context holding allowed input and output topics,
    raw client connections and other runtime dependencies.
    """

    pass
