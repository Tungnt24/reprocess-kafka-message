from kafka import KafkaProducer, KafkaConsumer
from resend_kafka_message.setting import (
    KafkaProducerConfig,
    KafkaConsumerConfig,
)
import json
from kafka.structs import TopicPartition
from resend_kafka_message.utils.logger import logger


class KafkaBackupProducer:
    def __init__(self) -> None:
        self.producer = KafkaProducer(
            bootstrap_servers=KafkaProducerConfig.KAFKA_BROKER,
            value_serializer=lambda x: json.dumps(x).encode("utf-8"),
        )
        self.topic = KafkaProducerConfig.KAFKA_TOPIC

    def send_message(self, user, event, partition):
        self.producer.send(
            topic=self.topic,
            key=bytes(user, "utf-8"),
            value=event,
            partition=partition,
        )
        self.producer.flush()


class KafkaBackupConsumer:
    def __init__(self) -> None:
        self.consumer = KafkaConsumer(
            bootstrap_servers=KafkaConsumerConfig.KAFKA_BROKER,
            auto_offset_reset=KafkaConsumerConfig.KAFKA_AUTO_OFFSET_RESET,
            value_deserializer=lambda x: json.loads(x.decode("utf-8")),
            enable_auto_commit=KafkaConsumerConfig.KAFKA_ENABLE_AUTO_COMMIT,
            max_poll_records=KafkaConsumerConfig.KAFKA_MAX_POLL_RECORDS,
        )
        self.topic = KafkaConsumerConfig.KAFKA_TOPIC

    def kafka_close(self):
        self.consumer.close(autocommit=False)

    def current_possion(self, partition):
        tp = TopicPartition(self.topic, partition)
        return self.consumer.position(tp)

    def assign_partition(self, partition):
        tp = TopicPartition(self.topic, partition)
        self.consumer.assign([tp])

    def seek_message(self, partition, offset_start):
        tp = TopicPartition(self.topic, partition)
        self.consumer.seek(tp, offset_start)
        return self.consumer

    def get_offset_and_timestamp(self, tp, timestamp_start, timestamp_end):
        offset_and_timestamp_start = self.consumer.offsets_for_times(
            {tp: int(timestamp_start)}
        )
        offset_and_timestamp_end = self.consumer.offsets_for_times(
            {tp: int(timestamp_end)}
        )
        offset_and_timestamp_start = list(offset_and_timestamp_start.values())[
            0
        ]
        offset_and_timestamp_end = list(offset_and_timestamp_end.values())[0]
        if (
            offset_and_timestamp_start is None
            or offset_and_timestamp_end is None
        ):
            return None, None
        return offset_and_timestamp_start, offset_and_timestamp_end

    def get_offset(self, partition, timestamp_start, timestamp_end):
        tp = TopicPartition(self.topic, partition)
        (
            offset_timestamp_start,
            offset_timestamp_end,
        ) = self.get_offset_and_timestamp(tp, timestamp_start, timestamp_end)
        if offset_timestamp_start is None or offset_timestamp_start is None:
            print("could not found offset and timestamp")
            offset_start, offset_end = 0, 0
        else:
            offset_start = offset_timestamp_start.offset
            offset_end = offset_timestamp_end.offset
        return offset_start, offset_end
