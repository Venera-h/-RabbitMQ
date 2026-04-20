"""Обработчик chat.reaction.created: upsert/delete реакции, публикация счётчиков."""

import json
from collections import Counter

import aio_pika
from sqlalchemy import delete, select
from sqlalchemy.dialects.postgresql import insert as pg_insert

from app.db import Reaction, SessionLocal
from app.mq import MQ_ROUTING_KEY_REACTION_PERSISTED


async def handle_reaction(incoming, exchange):
    """Добавить или убрать реакцию; опубликовать обновлённые счётчики."""
    payload = json.loads(incoming.body.decode("utf-8"))
    message_id = payload["message_id"]

    async with SessionLocal() as session:
        if payload.get("delete"):
            await session.execute(
                delete(Reaction).where(
                    Reaction.message_id == message_id,
                    Reaction.username == payload["username"],
                    Reaction.emoji == payload["emoji"],
                )
            )
        else:
            stmt = pg_insert(Reaction).values(
                message_id=message_id,
                username=payload["username"],
                emoji=payload["emoji"],
            ).on_conflict_do_nothing()
            await session.execute(stmt)

        rows = (
            await session.execute(
                select(Reaction.emoji).where(Reaction.message_id == message_id)
            )
        ).scalars().all()
        await session.commit()

    counts = dict(Counter(rows))
    persisted = {
        "room_id": payload["room_id"],
        "message_id": message_id,
        "counts": counts,
    }
    msg = aio_pika.Message(
        body=json.dumps(persisted).encode("utf-8"),
        delivery_mode=aio_pika.DeliveryMode.PERSISTENT,
    )
    await exchange.publish(msg, routing_key=MQ_ROUTING_KEY_REACTION_PERSISTED)
