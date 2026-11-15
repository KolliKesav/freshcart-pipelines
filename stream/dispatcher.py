import json
import os
import random
import time
import uuid
from datetime import datetime, timezone

import pika

BROKER_URL = os.environ.get("AMQP_URL", "amqp://guest:guest@localhost:5672/%2F")
EXCHANGE = "rapido-dispatch"

CITIES = ["bangalore", "chennai"]
ZONES = ["koramangala", "indiranagar", "whitefield", "tnagar"]
TYPES = ["ride", "food"]


def main():
    connection = pika.BlockingConnection(pika.URLParameters(BROKER_URL))
    channel = connection.channel()
    channel.exchange_declare(exchange=EXCHANGE, exchange_type="topic")

    count = 0
    try:
        while True:
            count += 1
            city = random.choice(CITIES)
            zone = random.choice(ZONES)
            req_type = random.choice(TYPES)
            routing_key = f"{city}.{zone}.{req_type}"

            message = {
                "id": uuid.uuid4().hex[:8],
                "city": city,
                "zone": zone,
                "type": req_type,
                "ts": datetime.now(timezone.utc).isoformat(),
            }
            tag = ""
            if count % 10 == 0:
                del message["zone"]          # every 10th message is bad
                tag = " BAD(no zone)"

            channel.basic_publish(
                exchange=EXCHANGE,
                routing_key=routing_key,
                body=json.dumps(message),
            )
            print(f"published key={routing_key} id={message['id']}{tag}", flush=True)
            time.sleep(2)
    except KeyboardInterrupt:
        pass
    finally:
        connection.close()


if __name__ == "__main__":
    main()