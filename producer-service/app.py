import json
import base64
from concurrent.futures import TimeoutError
from google.cloud import pubsub_v1
from google.cloud import storage
import confluent_kafka
from config import PROJECT_ID, SUBSCRIPTION_ID, KAFKA_BOOTSTRAP_SERVERS, KAFKA_TOPIC
import time

# from tokenprovider import TokenProvider


# Configure Kafka producer
def oauthbearer_token_refresh_cb(producer, oauthbearer_config):
    # 1. Call your IdP here to get an access token (e.g. Google, Okta).
    #    This part is specific to your environment.
    access_token = "<ACCESS_TOKEN_STRING>"
    lifetime = int(time.time()) + 3300  # token expiry as Unix time

    # 2. Set token on the client
    producer.set_oauthbearer_token(
        token_value=access_token,
        md_lifetime_ms=lifetime * 1000,
        md_principal_name="your-principal-or-email",
        md_extensions={},
    )

producer_conf = {
    "bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS,
    "security.protocol": "SASL_SSL",
    "sasl.mechanisms": "OAUTHBEARER",
    "oauth_cb": oauthbearer_token_refresh_cb,
}

producer = confluent_kafka.Producer(producer_conf)


def upload_file(bucket_name: str, source_path: str, dest_blob_name: str):
    """Uploads a local file to a GCS bucket."""
    client = storage.Client()
    bucket = client.bucket(bucket_name)
    blob = bucket.blob(dest_blob_name)

    blob.upload_from_filename(source_path)
    print(f"Uploaded {source_path} to gs://{bucket_name}/{dest_blob_name}")


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
        payload = message.data.decode("utf-8")
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
    upload_file("innovacer", "sample.txt", "uploaded-file.txt")
    main()
