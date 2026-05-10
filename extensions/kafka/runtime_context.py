"""
Deprecated: Kafka no longer needs a custom RuntimeContext.

The core ``RuntimeContext`` dispatches per-topic operations to a
``Backend`` keyed by ``type(topic.config)``; the Kafka transport is
implemented as ``extensions.kafka.backend.KafkaBackend``.
"""

from .backend import KafkaBackend as KafkaBackend
