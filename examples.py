"""
Edgy Pub/Sub Framework Examples

This demonstrates how to:
1. Define typed Topics with pydantic models
2. Define Edges that subscribe to inputs and publish to outputs
3. Run the system with different backends (in-memory or Kafka)
"""

import asyncio
import pydantic
from core.runtime import Runtime
from core.topic import Topic
from core.edge import Edge

# Import backends
from extensions.memory.runtime_context import InMemoryRuntimeContext
from extensions.memory.config_dict import InMemoryConfig

# Uncomment to use Kafka backend:
# from extensions.kafka.runtime_context import KafkaRuntimeContext
# from extensions.kafka.config_dict import KafkaConfig


# ============================================================================
# Define your message types using pydantic
# ============================================================================

class Order(pydantic.BaseModel):
    """An order placed by a customer."""
    order_id: str
    customer_id: str
    amount: float


class Delivery(pydantic.BaseModel):
    """A delivery scheduled for an order."""
    order_id: str
    delivery_date: str
    address: str


# ============================================================================
# Define your topics
# Each topic is typed to a specific message type
# ============================================================================

class OrderCreated(Topic[Order]):
    """Topic for newly created orders."""
    config = InMemoryConfig()
    # For Kafka backend, use instead:
    # config = KafkaConfig(topic_name="orders.created")


class OrderUpdated(Topic[Order]):
    """Topic for updated orders."""
    config = InMemoryConfig()


class OrderCancelled(Topic[Order]):
    """Topic for cancelled orders."""
    config = InMemoryConfig()


class DeliveryScheduled(Topic[Delivery]):
    """Topic for scheduled deliveries."""
    config = InMemoryConfig()


# ============================================================================
# Define your edges (processors)
# Each edge declares its input and output topics via generics
# ============================================================================

class OrderProcessor(Edge[OrderCreated, OrderUpdated | OrderCancelled]):
    """
    Processes new orders and either updates or cancels them.

    Input: OrderCreated
    Output: OrderUpdated or OrderCancelled
    """

    async def process(self):
        async for order in OrderCreated.sub(self.ctx):
            print(f"Processing order: {order.order_id}")

            if order.amount > 0:
                # Publish updated order
                await OrderUpdated.pub(
                    self.ctx,
                    Order(
                        order_id=order.order_id,
                        customer_id=order.customer_id,
                        amount=order.amount * 0.9  # Apply discount
                    )
                )
            else:
                # Publish cancelled order
                await OrderCancelled.pub(
                    self.ctx,
                    Order(
                        order_id=order.order_id,
                        customer_id=order.customer_id,
                        amount=0.0
                    )
                )


class DeliveryScheduler(Edge[OrderUpdated, DeliveryScheduled]):
    """
    Schedules delivery for updated orders.

    Input: OrderUpdated
    Output: DeliveryScheduled
    """

    async def process(self):
        async for order in OrderUpdated.sub(self.ctx):
            print(f"Scheduling delivery for: {order.order_id}")

            await DeliveryScheduled.pub(
                self.ctx,
                Delivery(
                    order_id=order.order_id,
                    delivery_date="2024-01-15",
                    address="123 Main St"
                )
            )


# ============================================================================
# Run the system
# ============================================================================

async def main():
    """Run the edgy runtime with in-memory backend."""

    # Create runtime
    runtime = Runtime()

    # Add edges with in-memory backend
    # For Kafka backend, use KafkaRuntimeContext instead:
    # runtime.add(OrderProcessor, KafkaRuntimeContext)
    runtime.add(OrderProcessor, InMemoryRuntimeContext)
    runtime.add(DeliveryScheduler, InMemoryRuntimeContext)

    # Get context from first edge for publishing test messages
    ctx = runtime.edges[0].ctx

    # Start runtime in background
    print("Starting runtime...")
    runtime_task = asyncio.create_task(runtime.start())

    # Give edges time to start subscribing
    await asyncio.sleep(0.1)

    # Publish some test orders
    print("\nPublishing test orders...")
    await OrderCreated.pub(
        ctx,
        Order(order_id="ORD-001", customer_id="CUST-001", amount=100.0)
    )
    await OrderCreated.pub(
        ctx,
        Order(order_id="ORD-002", customer_id="CUST-002", amount=0.0)
    )

    # Let processing happen
    await asyncio.sleep(0.5)

    # Stop runtime
    print("\nStopping runtime...")
    await runtime.stop()

    try:
        await asyncio.wait_for(runtime_task, timeout=1.0)
    except asyncio.CancelledError:
        pass

    print("Done!")


if __name__ == "__main__":
    asyncio.run(main())
