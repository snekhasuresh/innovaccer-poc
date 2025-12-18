import json
from confluent_kafka import Consumer
from config import KAFKA_BOOTSTRAP_SERVERS, KAFKA_TOPIC, KAFKA_GROUP_ID
import google.auth
from google.auth.transport.requests import Request
import time


def oauthbearer_token_refresh_cb(consumer, oauthbearer_config):
    """Refresh OAuth token for Kafka consumer."""
    credentials, project = google.auth.default(
        scopes=["https://www.googleapis.com/auth/cloud-platform"]
    )
    credentials.refresh(Request())
    access_token = credentials.token
    lifetime = int(time.time()) + 3300  # token expiry as Unix time

    consumer.set_oauthbearer_token(
        token_value=access_token,
        md_lifetime_ms=lifetime * 1000,
        md_principal_name="your-principal-or-email",
        md_extensions={},
    )


consumer_conf = {
    "bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS,
    "group.id": KAFKA_GROUP_ID,
    "auto.offset.reset": "earliest",
    "security.protocol": "SASL_SSL",
    "sasl.mechanisms": "OAUTHBEARER",
    "oauth_cb": oauthbearer_token_refresh_cb,
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
