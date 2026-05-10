from __future__ import annotations
import typing as t
import pydantic

from .config_dict import ConfigDict
from .backend import Backend

if t.TYPE_CHECKING:
    from .topic import Topic


# Input and Output must be kept contravariant
class RuntimeContext[Input: Topic, Output: Topic]:
    """
    Runtime context holding allowed input and output topics and a
    registry of Backends keyed by ``ConfigDict`` subclass.

    The context itself is transport-agnostic; per-topic operations are
    dispatched to the right Backend via ``type(topic.config)``. This
    keeps a single ``Edge`` free to mix topics from multiple transports.
    """

    def __init__(
        self,
        allowed_input: set[type[Topic]],
        allowed_output: set[type[Topic]],
        backends: dict[type[ConfigDict], Backend] | None = None,
        owner: str = "",
    ) -> None:
        self.allowed_input = allowed_input
        self.allowed_output = allowed_output
        self.backends: dict[type[ConfigDict], Backend] = backends or {}
        self.owner = owner

    def guard_input_topic(self, topic: type[Input]) -> bool:
        return topic in self.allowed_input

    def guard_output_topic(self, topic: type[Output]) -> bool:
        return topic in self.allowed_output

    def _backend_for(self, topic: type[Topic]) -> Backend:
        cfg = getattr(topic, "config", None)
        if cfg is None:
            raise RuntimeError(
                f"Topic {topic.__name__} has no `config`; "
                f"assign a ConfigDict instance (e.g. KafkaConfig(...))."
            )
        cfg_type = type(cfg)
        try:
            return self.backends[cfg_type]
        except KeyError:
            raise RuntimeError(
                f"No backend registered for {cfg_type.__name__} "
                f"(needed by topic {topic.__name__})."
            ) from None

    async def unsafe_pub[M: pydantic.BaseModel](
        self,
        topic: type[Topic[M]],
        data: M,
    ) -> None:
        """
        Unsafe at the type level. It is the implementor's responsibility
        to validate that topic is in the allowed set of topics for this
        runtime context. Use ``guard_output_topic`` for that.
        """
        await self._backend_for(topic).publish(topic, data)

    def unsafe_sub[M: pydantic.BaseModel](
        self,
        topic: type[Topic[M]],
    ) -> t.AsyncIterator[M]:
        """
        Unsafe at the type level. It is the implementor's responsibility
        to validate that topic is in the allowed set of topics for this
        runtime context. Use ``guard_input_topic`` for that.
        """
        return t.cast(
            t.AsyncIterator[M],
            self._backend_for(topic).subscribe(topic, owner=self.owner),
        )
