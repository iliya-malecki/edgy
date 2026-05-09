from .edge import Edge
from .runtime_context import RuntimeContext


class Runtime:
    def __init__(self) -> None:
        self.edges: list[Edge] = []
        self.runtime_context = RuntimeContext

    def build[T: Edge](self, edge: type[T], context: type[RuntimeContext]) -> T:
        connectivity = edge.parse_connectivity()
        ctx = context(
            connectivity["inputs"],
            connectivity["outputs"],
        )
        return edge(ctx)

    def add[T: Edge](self, edge: type[T]) -> None:
        self.edges.append(self.build(edge, self.runtime_context))
