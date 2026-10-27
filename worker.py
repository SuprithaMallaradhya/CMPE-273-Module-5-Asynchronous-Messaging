#!/usr/bin/env python3
"""Consume orders with manual ACKs, redelivery, and idempotent handling."""

import argparse
import json
import os
import sys
import time
from pathlib import Path

import pika


QUEUE = "orders"


class ProcessedEventLedger:
    """Small shared file ledger; the lock makes two workers safe to use together."""

    def __init__(self, path: str):
        self.path = Path(path)
        self.path.touch(exist_ok=True)

    def contains(self, event_id: str) -> bool:
        import fcntl

        with self.path.open("r", encoding="utf-8") as file:
            fcntl.flock(file.fileno(), fcntl.LOCK_SH)
            values = {line.strip() for line in file if line.strip()}
            fcntl.flock(file.fileno(), fcntl.LOCK_UN)
        return event_id in values

    def add(self, event_id: str) -> None:
        import fcntl

        with self.path.open("a+", encoding="utf-8") as file:
            fcntl.flock(file.fileno(), fcntl.LOCK_EX)
            file.seek(0)
            values = {line.strip() for line in file if line.strip()}
            if event_id not in values:
                file.seek(0, 2)
                file.write(event_id + "\n")
                file.flush()
                os.fsync(file.fileno())
            fcntl.flock(file.fileno(), fcntl.LOCK_UN)


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


def valid_order(message: object) -> bool:
    if not isinstance(message, dict):
        return False
    required = {"event_id", "order_id", "item_id", "quantity"}
    return required.issubset(message) and isinstance(message["quantity"], int) and message["quantity"] > 0


def main() -> int:
    parser = argparse.ArgumentParser(description="Consume order messages")
    parser.add_argument("--name", required=True, help="worker name shown in logs")
    parser.add_argument("--host", default=os.getenv("RABBITMQ_HOST", "localhost"))
    parser.add_argument("--port", type=int, default=int(os.getenv("RABBITMQ_PORT", "5672")))
    parser.add_argument("--user", default=os.getenv("RABBITMQ_USER", "guest"))
    parser.add_argument("--password", default=os.getenv("RABBITMQ_PASSWORD", "guest"))
    parser.add_argument("--state-file", default="processed_events.txt")
    parser.add_argument(
        "--fail-on-event",
        help="simulate a crash after business processing but before ACK for this event_id",
    )
    parser.add_argument("--processing-seconds", type=float, default=0.7)
    args = parser.parse_args()

    ledger = ProcessedEventLedger(args.state_file)
    connection = connect(args.host, args.port, args.user, args.password)
    channel = connection.channel()
    channel.queue_declare(queue=QUEUE, durable=True)
    channel.basic_qos(prefetch_count=1)

    print(f"[{args.name}] waiting on queue '{QUEUE}' with prefetch=1", flush=True)

    def callback(ch, method, properties, body):
        try:
            message = json.loads(body.decode("utf-8"))
        except (UnicodeDecodeError, json.JSONDecodeError):
            print(f"[{args.name}] invalid JSON; rejecting message", flush=True)
            ch.basic_nack(delivery_tag=method.delivery_tag, requeue=False)
            return

        if not valid_order(message):
            print(f"[{args.name}] invalid order schema; rejecting message", flush=True)
            ch.basic_nack(delivery_tag=method.delivery_tag, requeue=False)
            return

        event_id = message["event_id"]
        redelivered = "redelivered" if method.redelivered else "first-delivery"
        print(
            f"[{args.name}] received event_id={event_id} order_id={message['order_id']} ({redelivered})",
            flush=True,
        )

        if ledger.contains(event_id):
            print(f"[{args.name}] DUPLICATE detected for event_id={event_id}; business action skipped", flush=True)
            ch.basic_ack(delivery_tag=method.delivery_tag)
            print(f"[{args.name}] ACK sent for duplicate event_id={event_id}", flush=True)
            return

        print(
            f"[{args.name}] processing order_id={message['order_id']} item_id={message['item_id']} quantity={message['quantity']}",
            flush=True,
        )
        time.sleep(args.processing_seconds)
        ledger.add(event_id)
        print(f"[{args.name}] business action completed event_id={event_id}", flush=True)

        if args.fail_on_event == event_id:
            print(
                f"[{args.name}] SIMULATED FAILURE: stopping before ACK for event_id={event_id}",
                flush=True,
            )
            ch.close()
            connection.close()
            raise SystemExit(3)

        ch.basic_ack(delivery_tag=method.delivery_tag)
        print(f"[{args.name}] ACK sent for event_id={event_id}", flush=True)

    channel.basic_consume(queue=QUEUE, on_message_callback=callback, auto_ack=False)
    try:
        channel.start_consuming()
    except KeyboardInterrupt:
        print(f"[{args.name}] stopping", flush=True)
        if connection.is_open:
            connection.close()
    except (pika.exceptions.AMQPConnectionError, pika.exceptions.ChannelClosedByBroker) as error:
        print(f"[{args.name}] connection error: {error}", file=sys.stderr, flush=True)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
