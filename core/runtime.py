from __future__ import annotations
import asyncio

from .edge import Edge
from .runtime_context import RuntimeContext


class Runtime:
    def __init__(self) -> None:
        self.edges: list[Edge] = []
        self.contexts: list[RuntimeContext] = []

    def add[T: Edge](
        self,
        edge: type[T],
        context: type[RuntimeContext],
    ) -> T:
        connectivity = edge.parse_connectivity()
        ctx = context(
            connectivity["inputs"],
            connectivity["outputs"],
            owner=f"{edge.__module__}.{edge.__qualname__}",
        )
        instance = edge(ctx)
        self.edges.append(instance)
        self.contexts.append(ctx)
        return instance

    async def run(self) -> None:
        # Register every context before starting so a mid-start crash
        # still gets a `stop()` call (stop is a no-op when nothing was
        # actually initialised).
        try:
            for ctx in self.contexts:
                await ctx.start()
            if not self.edges:
                return
            await asyncio.gather(*(e.process() for e in self.edges))
        finally:
            for ctx in reversed(self.contexts):
                try:
                    await ctx.stop()
                except Exception:
                    pass
