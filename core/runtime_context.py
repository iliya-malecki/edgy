from __future__ import annotations
import typing as t
import pydantic

if t.TYPE_CHECKING:
    from .topic import Topic


# Input and Output must be kept contravariant
class RuntimeContext[Input: "Topic", Output: "Topic"]:
    """
    Holds the set of topics an edge may pub/sub and owns the raw
    transport clients. Transport-specific subclasses implement
    `unsafe_pub` / `unsafe_sub` and override `start` / `stop` to
    manage connections.
    """

    def __init__(
        self,
        allowed_input: set[type["Topic"]],
        allowed_output: set[type["Topic"]],
        owner: str = "",
    ) -> None:
        self.allowed_input = allowed_input
        self.allowed_output = allowed_output
        self.owner = owner

    def guard_input_topic(self, topic: type[Input]) -> bool:
        return topic in self.allowed_input

    def guard_output_topic(self, topic: type[Output]) -> bool:
        return topic in self.allowed_output

    async def start(self) -> None:
        """Initialise transport clients. No-op by default."""

    async def stop(self) -> None:
        """Tear down transport clients. No-op by default."""

    async def unsafe_pub[M: pydantic.BaseModel](
        self,
        topic: type["Topic[M]"],
        data: M,
    ) -> None:
        """
        Unsafe at the type level. Implementor must validate the topic
        is in `allowed_output` (use `guard_output_topic`).
        """
        raise NotImplementedError

    def unsafe_sub[M: pydantic.BaseModel](
        self,
        topic: type["Topic[M]"],
    ) -> t.AsyncIterator[M]:
        """
        Unsafe at the type level. Implementor must validate the topic
        is in `allowed_input` (use `guard_input_topic`).
        """
        raise NotImplementedError
