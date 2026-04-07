from __future__ import annotations
import typing as t
import types

from .runtime_context import RuntimeContext
from .topic import Topic
from . import util


class Connectivity(t.TypedDict):
    inputs: set[type[Topic]]
    outputs: set[type[Topic]]


class Edge[Input: Topic, Output: Topic]:
    def __init_subclass__(cls) -> None:
        super().__init_subclass__()
        util.validate_flat_subclassing(cls, Edge)

    def __init__(self, ctx: RuntimeContext[Input, Output]):
        self.ctx = ctx

    @classmethod
    def parse_connectivity(cls) -> Connectivity:
        for base in getattr(cls, "__orig_bases__", ()):
            origin = t.get_origin(base)
            if origin is Edge:
                args = t.get_args(base)
                if len(args) != 2:
                    raise ValueError(
                        f"Edge must have exactly 2 type arguments, {Input} and {Output}"
                    )
                input_type, output_type = args
                return Connectivity(
                    inputs=cls._extract_annotation_set(input_type),
                    outputs=cls._extract_annotation_set(output_type),
                )
        raise ValueError(f"Cant parse connectivity from class definition of {cls}")

    @staticmethod
    def _extract_annotation_set(annotation: t.Any) -> set[type[Topic]]:
        origin = t.get_origin(annotation)
        if origin is t.Union or (
            hasattr(types, "UnionType") and origin is types.UnionType
        ):
            args = t.get_args(annotation)
            return {
                arg for arg in args if isinstance(arg, type) and issubclass(arg, Topic)
            }
        if isinstance(annotation, type) and issubclass(annotation, Topic):
            return {annotation}
        raise ValueError(f"Invalid {Topic.__name__} type: {annotation}")

    async def process(self): ...
