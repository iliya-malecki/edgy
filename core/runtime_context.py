from __future__ import annotations
import typing as t
import pydantic

if t.TYPE_CHECKING:
    from .topic import Topic


# Input and Output must be kept contravariant
class RuntimeContext[Input: "Topic", Output: "Topic"]:
    """
    Holds the set of topics an edge may pub/sub and owns the raw
    transport clients.

    Transport-specific subclasses implement `_publish` / `_subscribe`
    and may override `start` / `stop` to manage connections. The
    public `unsafe_pub` / `unsafe_sub` are non-overridable wrappers
    that enforce the `allowed_input` / `allowed_output` guards before
    delegating, so connectivity violations cannot reach the wire even
    if a topic is wired to the wrong edge.
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
        if not self.guard_output_topic(topic):
            raise PermissionError(
                f"Topic {topic.__name__} is not declared as an output of "
                f"{self.owner!r}."
            )
        await self._publish(topic, data)

    def unsafe_sub[M: pydantic.BaseModel](
        self,
        topic: type["Topic[M]"],
    ) -> t.AsyncIterator[M]:
        if not self.guard_input_topic(topic):
            raise PermissionError(
                f"Topic {topic.__name__} is not declared as an input of "
                f"{self.owner!r}."
            )
        return self._subscribe(topic)

    async def _publish[M: pydantic.BaseModel](
        self,
        topic: type["Topic[M]"],
        data: M,
    ) -> None:
        raise NotImplementedError

    async def _subscribe[M: pydantic.BaseModel](
        self,
        topic: type["Topic[M]"],
    ) -> t.AsyncIterator[M]:
        raise NotImplementedError
        yield  # makes this an async-generator function so subclasses can override symmetrically
