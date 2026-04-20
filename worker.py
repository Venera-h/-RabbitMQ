import asyncio
import aio_pika

from app.db import init_models
from app.mq import (
    RABBITMQ_URL,
    MQ_EXCHANGE,
    MQ_QUEUE_INCOMING,
    MQ_ROUTING_KEY_CREATED,
)
from workers.messages import handle_message


async def consume() -> None:
    connection = await aio_pika.connect_robust(RABBITMQ_URL)
    channel = await connection.channel()
    exchange = await channel.declare_exchange(
        MQ_EXCHANGE, aio_pika.ExchangeType.TOPIC, durable=True
    )
    queue = await channel.declare_queue(MQ_QUEUE_INCOMING, durable=True)
    await queue.bind(exchange, routing_key=MQ_ROUTING_KEY_CREATED)

    async with queue.iterator() as iterator:
        async for incoming in iterator:
            async with incoming.process(requeue=True):
                await handle_message(incoming, exchange)


async def main() -> None:
    await init_models()
    await asyncio.gather(consume())


if __name__ == "__main__":
    asyncio.run(main())
