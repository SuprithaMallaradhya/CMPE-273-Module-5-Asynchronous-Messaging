#!/usr/bin/env python3
"""Publish order events to the durable orders queue."""
from typing import Optional
import argparse
import json
import os
import sys
import time
import uuid

import pika


QUEUE = "orders"


def make_order(index: int, event_id: Optional[str] = None) -> dict:
    return {
        "event_id": event_id or str(uuid.uuid4()),
        "order_id": f"ORD-{index:04d}",
        "item_id": f"ITEM-{((index - 1) % 3) + 1:03d}",
        "quantity": (index % 4) + 1,
    }


def connect(host: str, port: int, user: str, password: str):
    credentials = pika.PlainCredentials(user, password)
    params = pika.ConnectionParameters(
        host=host,
        port=port,
        credentials=credentials,
        heartbeat=60,
        blocked_connection_timeout=30,
    )
    return pika.BlockingConnection(params)


def main() -> int:
    parser = argparse.ArgumentParser(description="Publish order messages")
    parser.add_argument("--count", type=int, default=6)
    parser.add_argument("--host", default=os.getenv("RABBITMQ_HOST", "localhost"))
    parser.add_argument("--port", type=int, default=int(os.getenv("RABBITMQ_PORT", "5672")))
    parser.add_argument("--user", default=os.getenv("RABBITMQ_USER", "guest"))
    parser.add_argument("--password", default=os.getenv("RABBITMQ_PASSWORD", "guest"))
    parser.add_argument("--delay", type=float, default=0.25)
    parser.add_argument(
        "--demo",
        action="store_true",
        help="Publish failure-demo as the first event, followed by normal events",
    )
    args = parser.parse_args()

    if args.count < 1:
        print("count must be at least 1", file=sys.stderr)
        return 2

    connection = connect(args.host, args.port, args.user, args.password)
    channel = connection.channel()
    channel.queue_declare(queue=QUEUE, durable=True)
    channel.confirm_delivery()

    messages = []
    if args.demo:
        messages.append(make_order(1, event_id="failure-demo"))
        messages.extend(make_order(i) for i in range(2, args.count + 1))
    else:
        messages.extend(make_order(i) for i in range(1, args.count + 1))

    print(f"[PRODUCER] Publishing {len(messages)} messages to queue '{QUEUE}'")
    for message in messages:
        body = json.dumps(message, separators=(",", ":"))
        channel.basic_publish(
            exchange="",
            routing_key=QUEUE,
            body=body.encode("utf-8"),
            properties=pika.BasicProperties(
                content_type="application/json",
                delivery_mode=pika.DeliveryMode.Persistent,
            ),
        )
        print(f"[PRODUCER] published event_id={message['event_id']} order_id={message['order_id']}")
        time.sleep(args.delay)

    connection.close()
    print("[PRODUCER] finished")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
