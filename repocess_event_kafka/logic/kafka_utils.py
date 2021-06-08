from kafka import KafkaProducer
from repocess_event_kafka.setting import KafkaProducerConfig
import json
from repocess_event_kafka.utils.logger import logger


def get_producer():
    logger.info("connect kafka")
    producer = KafkaProducer(
        bootstrap_servers=KafkaProducerConfig.KAFKA_BROKER,
        value_serializer=lambda x: json.dumps(x).encode("utf-8"),
    )
    logger.info("connect success")
    return producer

def send_to_kafka(user, event):
    logger.info("send event to kafka")
    producer = get_producer()
    producer.send(
        event["topic"],
        key=bytes(user, "utf-8"),
        value=event,
        partition=int(event['partition'])
    )
    producer.flush()
    logger.info("DONE")