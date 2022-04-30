import os
import json


CONF_FILE = os.path.join(
    os.path.dirname(os.path.abspath("config.json")), "config.json"
)

with open(CONF_FILE, "r") as f:
    config = json.load(f)


class KafkaProducerConfig:
    producer = config.get("KAFKA_PRODUCER")
    KAFKA_BROKER = producer.get("KAFKA_BROKER")
    KAFKA_TOPIC = producer.get("KAFKA_TOPIC")
    KAFKA_SLICE_SIZE = int(producer.get("KAFKA_SLICE_SIZE"))
    KAFKA_IGNORE_DOMAIN = producer.get("KAFKA_IGNORE_DOMAIN")
    KAFKA_IGNORE_USERS = producer.get("KAFKA_IGNORE_USERS")
    KAFKA_PRODUCER_AGGREGATED_TOPIC = producer.get(
        "KAFKA_PRODUCER_AGGREGATED_TOPIC"
    )
    KAFKA_PRODUCER_NORMAL_TOPIC = producer.get("KAFKA_PRODUCER_NORMAL_TOPIC")


class KafkaConsumerConfig:
    consumer = config.get("KAFKA_CONSUMER")
    KAFKA_BROKER = consumer.get("KAFKA_BROKER")
    KAFKA_TOPIC = consumer.get("KAFKA_TOPIC")
    KAFKA_CONSUMER_GROUP = consumer.get("KAFKA_CONSUMER_GROUP")
    KAFKA_ENABLE_AUTO_COMMIT = consumer.get("KAFKA_ENABLE_AUTO_COMMIT")
    KAFKA_AUTO_OFFSET_RESET = consumer.get("KAFKA_AUTO_OFFSET_RESET")
    KAFKA_MAX_POLL_RECORDS = consumer.get("KAFKA_MAX_POLL_RECORDS")


class KafkaAuth:
    auth = config.get("KAFKA_AUTH")
    SASL_PLAIN_USERNAME = auth.get("SASL_PLAIN_USERNAME")
    SASL_PLAIN_PASSWORD = auth.get("SASL_PLAIN_PASSWORD")
    SECURITY_PROTOCOL = auth.get("SECURITY_PROTOCOL")
    SASL_MECHANISM = auth.get("SASL_MECHANISM")
