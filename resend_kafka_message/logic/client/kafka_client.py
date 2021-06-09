from kafka import KafkaProducer
from resend_kafka_message.setting import KafkaProducerConfig
import json
from resend_kafka_message.utils.logger import logger


class KafkaClient:
    def __init__(self):
        self.producer = KafkaProducer(
        bootstrap_servers=KafkaProducerConfig.KAFKA_BROKER,
        value_serializer=lambda x: json.dumps(x).encode("utf-8"),
    )

    def send_to_kafka(self, user, event):
        logger.info("send event to kafka")
        self.producer.send(
            event["topic"],
            key=bytes(user, "utf-8"),
            value=event,
            partition=int(event['partition'])
        )
        self.producer.flush()
        logger.info("DONE")