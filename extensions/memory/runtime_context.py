from __future__ import annotations
import asyncio
import typing as t
import pydantic

from core.runtime_context import RuntimeContext
from core.topic import Topic
from .config_dict import InMemoryConfig


class InMemoryRuntimeContext(RuntimeContext):
    """
    In-process pub/sub bus. Suitable for tests, examples, or
    single-process compositions of edges. Broadcast semantics: every
    active subscription on a topic receives every published message.

    The bus lives on the class so distinct `InMemoryRuntimeContext`
    instances belonging to different edges in the same Runtime can
    talk to each other.
    """

    _bus: t.ClassVar[dict[type[Topic], list[asyncio.Queue]]] = {}

    @classmethod
    def reset(cls) -> None:
        """Drop all subscribers. Useful between test cases."""
        cls._bus.clear()

    async def _publish[M: pydantic.BaseModel](
        self,
        topic: type[Topic[M]],
        data: M,
    ) -> None:
        assert isinstance(topic.config, InMemoryConfig)
        for q in self._bus.get(topic, []):
            await q.put(data)

    async def _subscribe[M: pydantic.BaseModel](
        self,
        topic: type[Topic[M]],
    ) -> t.AsyncIterator[M]:
        assert isinstance(topic.config, InMemoryConfig)
        queue: asyncio.Queue[M] = asyncio.Queue()
        self._bus.setdefault(topic, []).append(queue)
        try:
            while True:
                yield await queue.get()
        finally:
            subscribers = self._bus.get(topic, [])
            if queue in subscribers:
                subscribers.remove(queue)
            if not subscribers:
                self._bus.pop(topic, None)
