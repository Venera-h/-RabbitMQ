import asyncio
import aio_pika

from app.db import init_models
from app.mq import (
    RABBITMQ_URL,
    MQ_EXCHANGE,
    MQ_QUEUE_INCOMING,
    MQ_QUEUE_REACTIONS_INCOMING,
    MQ_ROUTING_KEY_CREATED,
    MQ_ROUTING_KEY_REACTION_CREATED,
)
from workers.messages import handle_message
from workers.reactions import handle_reaction


async def consume(queue_name, routing_key, handler, channel, exchange) -> None:
    queue = await channel.declare_queue(queue_name, durable=True)
    await queue.bind(exchange, routing_key=routing_key)
    async with queue.iterator() as iterator:
        async for incoming in iterator:
            async with incoming.process(requeue=False):
                try:
                    await handler(incoming, exchange)
                except Exception as exc:
                    print(f"[worker] error in {queue_name}: {exc}")


async def run_worker() -> None:
    connection = await aio_pika.connect_robust(RABBITMQ_URL)
    channel = await connection.channel()
    exchange = await channel.declare_exchange(
        MQ_EXCHANGE, aio_pika.ExchangeType.TOPIC, durable=True
    )
    await asyncio.gather(
        consume(MQ_QUEUE_INCOMING, MQ_ROUTING_KEY_CREATED, handle_message, channel, exchange),
        consume(
            MQ_QUEUE_REACTIONS_INCOMING,
            MQ_ROUTING_KEY_REACTION_CREATED,
            handle_reaction,
            channel,
            exchange,
        ),
    )


async def main() -> None:
    await init_models()
    await run_worker()


if __name__ == "__main__":
    asyncio.run(main())
