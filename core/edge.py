from __future__ import annotations

from .runtime_context import RuntimeContext
from .topic import Topic


class Edge[Input: Topic, Output: Topic]:
    ctx: RuntimeContext[Input, Output]

    async def process(self): ...
