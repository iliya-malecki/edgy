from __future__ import annotations
import typing as t
import pydantic
from .topic import Topic


class RuntimeContext[Input: Topic, Output: Topic]:
    """
    Runtime context holding allowed input and output topics,
    raw client connections and other runtime dependencies.
    """

    def guard_input_topic(self, topic: type[Input]) -> bool: ...
    def guard_output_topic(self, topic: type[Output]) -> bool: ...

    def unsafe_pub[M: pydantic.BaseModel](self, topic: type[Topic[M]], data: M) -> None:
        f"""
        Unsafe at the type level. It is implementors responsibility
        to validate that topic is in the allowed set of topics for this runtime context.
        Use {RuntimeContext.guard_output_topic.__name__} for that.
        """
        ...

    def unsafe_sub[M: pydantic.BaseModel](self, topic: type[Topic[M]]) -> t.AsyncIterable[M]:
        f"""
        Unsafe at the type level. It is implementors responsibility
        to validate that topic is in the allowed set of topics for this runtime context
        Use {RuntimeContext.guard_input_topic.__name__} for that.
        """
        ...
