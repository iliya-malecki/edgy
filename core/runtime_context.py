from __future__ import annotations
import typing as t
from .topic import Topic


class RuntimeContext[Input: Topic, Output: Topic]:
    """
    Runtime context holding allowed input and output topics,
    raw client connections and other runtime dependencies.
    """

    def unsafe_pub(self, topic: type[Output], data: t.Any) -> None:
        f"""
        Unsafe at the type level. It is implementors responsibility 
        to validate that data is compatible with {Topic.__name__}.model
        """
        ...

    def unsafe_sub(self, topic: type[Input]) -> t.AsyncIterable[t.Any]:
        f"""
        Unsafe at the type level. It is implementors responsibility 
        to unsure that returned data is compatible with {Topic.__name__}.model
        """
        ...
