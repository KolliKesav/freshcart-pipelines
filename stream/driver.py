import json
import os
import sys

import pika

BROKER_URL = os.environ.get("AMQP_URL", "amqp://guest:guest@localhost:5672/%2F")
EXCHANGE = "rapido-dispatch"


def main():
    if len(sys.argv) != 3:
        print("usage: python driver.py NAME PATTERN")
        sys.exit(1)
    name, pattern = sys.argv[1], sys.argv[2]

    connection = pika.BlockingConnection(pika.URLParameters(BROKER_URL))
    channel = connection.channel()
    channel.exchange_declare(exchange=EXCHANGE, exchange_type="topic")

    # private queue: server-generated name, deleted when this driver disconnects
    queue_name = channel.queue_declare(queue="", exclusive=True).method.queue
    channel.queue_bind(exchange=EXCHANGE, queue=queue_name, routing_key=pattern)
    print(f"[{name}] bound pattern '{pattern}' to {queue_name}", flush=True)

    def on_message(ch, method, properties, body):
        msg = json.loads(body)
        if "zone" not in msg:
            # bad message: reject it and do NOT put it back in the queue
            print(f"[{name}] NACK key={method.routing_key} id={msg.get('id')} reason=missing zone", flush=True)
            ch.basic_nack(delivery_tag=method.delivery_tag, requeue=False)
            return
        print(
            f"[{name}] ACK key={method.routing_key} id={msg['id']} "
            f"zone={msg['zone']} type={msg.get('type')}",
            flush=True,
        )
        ch.basic_ack(delivery_tag=method.delivery_tag)

    channel.basic_consume(queue=queue_name, on_message_callback=on_message, auto_ack=False)
    try:
        channel.start_consuming()
    except KeyboardInterrupt:
        pass
    finally:
        connection.close()


if __name__ == "__main__":
    main()