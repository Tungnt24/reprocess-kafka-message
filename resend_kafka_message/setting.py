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


class KafkaConsumerConfig:
    consumer = config.get("KAFKA_CONSUMER")
    KAFKA_BROKER = consumer.get("KAFKA_BROKER")
    KAFKA_TOPIC = consumer.get("KAFKA_TOPIC")
    KAFKA_CONSUMER_GROUP = consumer.get("KAFKA_CONSUMER_GROUP")
    KAFKA_ENABLE_AUTO_COMMIT = consumer.get("KAFKA_ENABLE_AUTO_COMMIT")
    KAFKA_AUTO_OFFSET_RESET = consumer.get("KAFKA_AUTO_OFFSET_RESET")
    KAFKA_MAX_POLL_RECORDS = consumer.get("KAFKA_MAX_POLL_RECORDS")


class MQTTConfig:
    mqtt = config.get("MQTT")
    CLIENT_ID = mqtt.get("CLIENT_ID")
    MQTT_BROKER = mqtt.get("MQTT_BROKER")
    MQTT_PORT = mqtt.get("MQTT_PORT")
    MQTT_USERNAME = mqtt.get("MQTT_USERNAME")
    MQTT_PASSWORD = mqtt.get("MQTT_PASSWORD")
    MQTT_TOPIC = mqtt.get("MQTT_TOPIC")
    MQTT_QoS = mqtt.get("MQTT_QoS")
    MQTT_KEEPALIVE = mqtt.get("MQTT_KEEPALIVE")