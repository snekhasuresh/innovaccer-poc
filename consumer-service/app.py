import json
from confluent_kafka import Consumer
from config import KAFKA_BOOTSTRAP_SERVERS, KAFKA_TOPIC, KAFKA_GROUP_ID

consumer_conf = {
    "bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS,
    "group.id": KAFKA_GROUP_ID,
    "auto.offset.reset": "earliest",
}

def main():
    consumer = Consumer(consumer_conf)
    consumer.subscribe([KAFKA_TOPIC])
    print(f"Consuming from {KAFKA_TOPIC}...")

    try:
        while True:
            msg = consumer.poll(1.0)
            if msg is None:
                continue
            if msg.error():
                print(f"Consumer error: {msg.error()}")
                continue

            key = msg.key().decode("utf-8") if msg.key() else None
            value = json.loads(msg.value().decode("utf-8"))
            print(f"Got event for {key}: {value}")

            # TODO: your business logic here (download file, process, etc.)
    except KeyboardInterrupt:
        pass
    finally:
        consumer.close()

if __name__ == "__main__":
    main()
