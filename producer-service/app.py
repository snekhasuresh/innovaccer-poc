import json
import base64
from concurrent.futures import TimeoutError

from google.cloud import pubsub_v1
from confluent_kafka import Producer
from config import PROJECT_ID, SUBSCRIPTION_ID, KAFKA_BOOTSTRAP_SERVERS, KAFKA_TOPIC

# Configure Kafka producer
producer_conf = {"bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS}
producer = Producer(producer_conf)

def delivery_report(err, msg):
    if err is not None:
        print(f"Delivery failed: {err}")
    else:
        print(f"Delivered to {msg.topic()} [{msg.partition()}] offset {msg.offset()}")

def handle_gcs_event(event_data: dict):
    """
    event_data is the decoded Pub/Sub message for a GCS notification.
    Typical fields: bucket, name, metageneration, timeCreated, updated, etc.
    """
    key = f"{event_data.get('bucket','')}/{event_data.get('name','')}"
    value_bytes = json.dumps(event_data).encode("utf-8")
    producer.produce(
        topic=KAFKA_TOPIC,
        key=key,
        value=value_bytes,
        callback=delivery_report,
    )
    producer.poll(0)

def callback(message: pubsub_v1.subscriber.message.Message) -> None:
    try:
        # Pub/Sub for GCS sends JSON in data field (base64-encoded)
        payload = base64.b64decode(message.data).decode("utf-8")
        event = json.loads(payload)
        print(f"Received GCS event: {event}")
        handle_gcs_event(event)
        message.ack()
    except Exception as exc:
        print(f"Failed to process message: {exc}")
        message.ack()

def main():
    subscriber = pubsub_v1.SubscriberClient()
    subscription_path = subscriber.subscription_path(PROJECT_ID, SUBSCRIPTION_ID)

    streaming_pull_future = subscriber.subscribe(subscription_path, callback=callback)
    print(f"Listening for messages on {subscription_path}...")

    try:
        streaming_pull_future.result()
    except KeyboardInterrupt:
        print("Stopping subscriber...")
        streaming_pull_future.cancel()
        streaming_pull_future.result()

if __name__ == "__main__":
    main()
