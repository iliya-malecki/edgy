from __future__ import annotations
import abc
import typing as t
import pydantic

from .config_dict import ConfigDict

if t.TYPE_CHECKING:
    from .topic import Topic


class Backend(abc.ABC):
    """
    Transport adapter for a specific pubsub module (Kafka, NATS, ...).

    A Backend owns the raw clients (producer/consumer) for one transport
    and knows how to publish to / subscribe from any Topic whose
    ``config`` is an instance of ``config_cls``.

    Backends are registered with the Runtime, which dispatches each
    ``Topic`` to the right Backend by ``type(topic.config)``. This is
    what allows the same process to mix transports, and what allows
    per-service deployments to only spin up the transports they need
    (a backend with no registered topics simply has nothing to do).
    """

    config_cls: t.ClassVar[type[ConfigDict]]

    # ---- lifecycle ----------------------------------------------------------

    @abc.abstractmethod
    async def start(self) -> None: ...

    @abc.abstractmethod
    async def stop(self) -> None: ...

    # ---- registration (called by Runtime at build time) ---------------------

    def register_output(self, topic: type["Topic"]) -> None:
        """Declare that some edge will publish to ``topic``."""

    def register_input(self, topic: type["Topic"], owner: str) -> None:
        """Declare that ``owner`` (an edge) will subscribe to ``topic``."""

    # ---- runtime ops --------------------------------------------------------

    @abc.abstractmethod
    async def publish(
        self, topic: type["Topic"], data: pydantic.BaseModel
    ) -> None: ...

    @abc.abstractmethod
    def subscribe(
        self, topic: type["Topic"], *, owner: str
    ) -> t.AsyncIterator[pydantic.BaseModel]: ...
