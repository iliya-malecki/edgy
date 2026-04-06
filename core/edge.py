from __future__ import annotations
import typing as t

from .runtime_context import RuntimeContext, Input, Output


class Edge(t.Generic[Input, Output]):
    ctx: RuntimeContext[Input, Output]

    async def process(self): ...
