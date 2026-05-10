import asyncio
from .edge import Edge
from .runtime_context import RuntimeContext


class Runtime:
    def __init__(self) -> None:
        self.edges: list[Edge] = []
        self._contexts: list[RuntimeContext] = []
        self._running = False
        self._stop_event = asyncio.Event()

    def build[T: Edge](
        self, edge: type[T], context_class: type[RuntimeContext]
    ) -> T:
        connectivity = edge.parse_connectivity()
        ctx = context_class(
            connectivity["inputs"],
            connectivity["outputs"],
        )
        self._contexts.append(ctx)
        return edge(ctx)

    def add[T: Edge](
        self, edge: type[T], context_class: type[RuntimeContext]
    ) -> T:
        edge_instance = self.build(edge, context_class)
        self.edges.append(edge_instance)
        return edge_instance

    async def start(self) -> None:
        """Start all contexts and run all edges."""
        self._running = True
        self._stop_event.clear()

        # Start all contexts
        for ctx in self._contexts:
            await ctx.start()

        # Run all edge process methods concurrently
        tasks = [
            asyncio.create_task(edge.process()) for edge in self.edges
        ]

        # Wait for stop signal or any task to complete
        try:
            stop_task = asyncio.create_task(self._stop_event.wait())
            await asyncio.wait(
                tasks + [stop_task],
                return_when=asyncio.FIRST_COMPLETED,
            )
        finally:
            # Cancel remaining tasks
            for task in tasks:
                if not task.done():
                    task.cancel()

            # Wait for tasks to finish cancellation
            await asyncio.gather(*tasks, return_exceptions=True)

    async def stop(self) -> None:
        """Signal runtime to stop and cleanup contexts."""
        if not self._running:
            return

        self._running = False
        self._stop_event.set()

        # Stop all contexts
        for ctx in self._contexts:
            await ctx.stop()
