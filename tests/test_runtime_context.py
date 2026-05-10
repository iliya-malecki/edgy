import pytest
from core.runtime_context import RuntimeContext
from core.topic import Topic
import pydantic


class TestModel(pydantic.BaseModel):
    value: str


class TestTopic(Topic[TestModel]):
    pass


def test_runtime_context_is_abstract():
    """Cannot instantiate RuntimeContext directly"""
    with pytest.raises(TypeError):
        RuntimeContext(set(), set())


def test_concrete_subclass_without_implementation_fails():
    """Concrete subclass without implementations raises TypeError"""
    class IncompleteContext(RuntimeContext):
        pass

    with pytest.raises(TypeError):
        IncompleteContext(set(), set())


def test_concrete_subclass_with_implementation_works():
    """Concrete subclass with implementations can be instantiated"""
    class CompleteContext(RuntimeContext):
        def unsafe_pub(self, topic, data):
            pass

        def unsafe_sub(self, topic):
            return []

        async def start(self):
            pass

        async def stop(self):
            pass

    # Should not raise
    ctx = CompleteContext({TestTopic}, {TestTopic})
    assert ctx.allowed_input == {TestTopic}
    assert ctx.allowed_output == {TestTopic}
