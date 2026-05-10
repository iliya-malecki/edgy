from __future__ import annotations
import asyncio
import typing as t

from .edge import Edge
from .runtime_context import RuntimeContext
from .backend import Backend
from .config_dict import ConfigDict
from .topic import Topic


class Runtime:
    """
    Builds and runs a set of Edges over a set of Backends.

    Deployment model
    ----------------
    The same codebase is expected to be deployed as multiple services,
    each running a different subset of edges. To support this, the
    Runtime is intentionally additive:

    * ``register_backend`` declares a transport that is available in
      this process. Several backends can coexist (Kafka + NATS + ...).
    * ``add(EdgeCls)`` instantiates a single edge and routes each of
      its input/output topics to the backend matching the topic's
      ``ConfigDict`` type. Topics that are never referenced by any
      added edge are never subscribed to.
    * ``run()`` starts every registered backend, then runs all edges
      concurrently with ``asyncio.gather``, and stops backends on exit.

    A backend with no registered topics is still started/stopped (it
    just sits idle); deployers should only register the backends their
    selected edges actually need.
    """

    def __init__(self) -> None:
        self.edges: list[Edge] = []
        self.backends: dict[type[ConfigDict], Backend] = {}

    def register_backend(self, backend: Backend) -> None:
        cfg_cls = backend.config_cls
        if cfg_cls in self.backends:
            raise RuntimeError(
                f"Backend for {cfg_cls.__name__} already registered."
            )
        self.backends[cfg_cls] = backend

    def _backend_for(self, topic: type[Topic]) -> Backend:
        cfg = getattr(topic, "config", None)
        if cfg is None:
            raise RuntimeError(
                f"Topic {topic.__name__} has no `config`; "
                f"assign a ConfigDict instance (e.g. KafkaConfig(...))."
            )
        cfg_type = type(cfg)
        if cfg_type not in self.backends:
            raise RuntimeError(
                f"No backend registered for topic {topic.__name__} "
                f"(config type {cfg_type.__name__}). "
                f"Call runtime.register_backend(...) first."
            )
        return self.backends[cfg_type]

    def build[T: Edge](self, edge: type[T]) -> T:
        connectivity = edge.parse_connectivity()
        owner = edge.__qualname__
        for topic in connectivity["inputs"]:
            self._backend_for(topic).register_input(topic, owner=owner)
        for topic in connectivity["outputs"]:
            self._backend_for(topic).register_output(topic)
        ctx: RuntimeContext = RuntimeContext(
            connectivity["inputs"],
            connectivity["outputs"],
            backends=self.backends,
            owner=owner,
        )
        return edge(ctx)

    def add[T: Edge](self, edge: type[T]) -> T:
        instance = self.build(edge)
        self.edges.append(instance)
        return instance

    async def run(self) -> None:
        started: list[Backend] = []
        try:
            for backend in self.backends.values():
                await backend.start()
                started.append(backend)
            if not self.edges:
                return
            await asyncio.gather(*(edge.process() for edge in self.edges))
        finally:
            # Stop in reverse order, swallow errors so one bad backend
            # doesn't prevent the rest from shutting down.
            for backend in reversed(started):
                try:
                    await backend.stop()
                except Exception:
                    pass
