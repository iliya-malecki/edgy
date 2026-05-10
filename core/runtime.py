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
            owner=edge.__qualname__,
        )
        instance = edge(ctx)
        self.edges.append(instance)
        self.contexts.append(ctx)
        return instance

    async def run(self) -> None:
        started: list[RuntimeContext] = []
        try:
            for ctx in self.contexts:
                await ctx.start()
                started.append(ctx)
            if not self.edges:
                return
            await asyncio.gather(*(e.process() for e in self.edges))
        finally:
            for ctx in reversed(started):
                try:
                    await ctx.stop()
                except Exception:
                    pass
