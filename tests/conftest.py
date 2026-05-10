"""Shared test fixtures and helpers for edgy tests."""

import typing as t


class AsyncIteratorMock:
    """
    Helper to create async iterators for mocking.

    Usage:
        mock_instance.__aiter__ = lambda self: AsyncIteratorMock([item1, item2])
    """

    def __init__(self, items: list[t.Any]) -> None:
        self.items = items
        self.index = 0

    def __aiter__(self) -> "AsyncIteratorMock":
        return self

    async def __anext__(self) -> t.Any:
        if self.index >= len(self.items):
            raise StopAsyncIteration
        item = self.items[self.index]
        self.index += 1
        return item


# Timing constants for async tests
# These values balance test speed with reliability
STARTUP_DELAY = 0.05  # Time for runtime/edges to start
SUBSCRIBE_DELAY = 0.01  # Time for subscriber to be ready
PUBLISH_DELAY = 0.01  # Small delay between publishes
PROCESSING_DELAY = 0.05  # Time for message processing
SHUTDOWN_TIMEOUT = 0.5  # Max time to wait for graceful shutdown
MESSAGE_TIMEOUT = 1.0  # Max time to wait for message receipt
