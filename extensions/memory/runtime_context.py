from __future__ import annotations
import typing as t
import asyncio
import pydantic
from core.runtime_context import RuntimeContext
from core.topic import Topic


class InMemoryRuntimeContext(RuntimeContext):
    """
    In-memory implementation of RuntimeContext for testing.
    Uses asyncio.Queue for message buffering per topic.
    Supports broadcast to multiple subscribers.
    """

    def __init__(
        self, allowed_input: set[type[Topic]], allowed_output: set[type[Topic]]
    ) -> None:
        super().__init__(allowed_input, allowed_output)
        # Store queues per topic - each subscriber gets a new queue
        # For broadcast, we maintain a list of queues per topic
        self._subscribers: dict[type[Topic], list[asyncio.Queue]] = {}

    async def unsafe_pub[M: pydantic.BaseModel](
        self,
        topic: type[Topic[M]],
        data: M,
    ) -> None:
        """Publish message to all subscribers of this topic."""
        if topic not in self._subscribers:
            # No subscribers yet, message is dropped
            return

        # Broadcast to all subscriber queues
        for queue in self._subscribers[topic]:
            await queue.put(data)

    async def unsafe_sub[M: pydantic.BaseModel](
        self,
        topic: type[Topic[M]],
    ) -> t.AsyncIterable[M]:
        """Subscribe to topic and yield messages as they arrive."""
        # Create a new queue for this subscriber
        queue: asyncio.Queue[M] = asyncio.Queue()

        if topic not in self._subscribers:
            self._subscribers[topic] = []
        self._subscribers[topic].append(queue)

        try:
            while True:
                message = await queue.get()
                yield message
        finally:
            # Cleanup when subscriber disconnects
            if topic in self._subscribers and queue in self._subscribers[topic]:
                self._subscribers[topic].remove(queue)
                if not self._subscribers[topic]:
                    del self._subscribers[topic]

    async def start(self) -> None:
        """No-op for in-memory (no external resources)."""
        pass

    async def stop(self) -> None:
        """Clear all subscribers."""
        self._subscribers.clear()
