from __future__ import annotations
import typing as t
import pydantic
from .topic import Topic


# Input and Output must be kept contravariant
class RuntimeContext[Input: Topic, Output: Topic]:
    """
    Runtime context holding allowed input and output topics,
    raw client connections and other runtime dependencies.
    """

    def __init__(
        self, allowed_input: set[type[Topic]], allowed_output: set[type[Topic]]
    ) -> None:
        self.allowed_input = allowed_input
        self.allowed_output = allowed_output

    def guard_input_topic(self, topic: type[Input]) -> bool:
        return topic in self.allowed_input

    def guard_output_topic(self, topic: type[Output]) -> bool:
        return topic in self.allowed_output

    def unsafe_pub[M: pydantic.BaseModel](
        self,
        topic: type[Topic[M]],
        data: M,
    ) -> None:
        f"""
        Unsafe at the type level. It is implementors responsibility
        to validate that topic is in the allowed set of topics for this runtime context.
        Use {RuntimeContext.guard_output_topic.__name__} for that.
        """
        ...

    def unsafe_sub[M: pydantic.BaseModel](
        self,
        topic: type[Topic[M]],
    ) -> t.AsyncIterable[M]:
        f"""
        Unsafe at the type level. It is implementors responsibility
        to validate that topic is in the allowed set of topics for this runtime context
        Use {RuntimeContext.guard_input_topic.__name__} for that.
        """
        ...
