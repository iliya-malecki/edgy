from __future__ import annotations
import asyncio
import sys
import traceback

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
        # actually initialised). Per-edge crashes are isolated: the
        # crashing edge logs and exits, the rest keep running until
        # they all terminate or the runtime is cancelled.
        try:
            for ctx in self.contexts:
                await ctx.start()
            if not self.edges:
                return

            tasks: set[asyncio.Task] = {
                asyncio.create_task(e.process(), name=type(e).__qualname__)
                for e in self.edges
            }
            try:
                while tasks:
                    done, tasks = await asyncio.wait(
                        tasks, return_when=asyncio.FIRST_COMPLETED,
                    )
                    for t in done:
                        exc = t.exception()
                        if exc is None or isinstance(exc, asyncio.CancelledError):
                            continue
                        print(
                            f"[Runtime] edge {t.get_name()!r} crashed; "
                            f"other edges keep running.",
                            file=sys.stderr,
                        )
                        traceback.print_exception(
                            type(exc), exc, exc.__traceback__, file=sys.stderr,
                        )
            except (asyncio.CancelledError, KeyboardInterrupt):
                for t in tasks:
                    t.cancel()
                await asyncio.gather(*tasks, return_exceptions=True)
                raise
        finally:
            for ctx in reversed(self.contexts):
                try:
                    await ctx.stop()
                except Exception:
                    pass
