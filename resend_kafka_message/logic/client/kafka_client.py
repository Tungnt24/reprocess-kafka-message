import ssl

from kafka import KafkaProducer, KafkaConsumer
from resend_kafka_message.setting import (
    KafkaProducerConfig,
    KafkaConsumerConfig,
    KafkaAuth
)
import json
from kafka.structs import TopicPartition
from resend_kafka_message.utils.logger import logger


context = ssl.create_default_context()
context.options &= ssl.OP_NO_TLSv1
context.options &= ssl.OP_NO_TLSv1_1

AGGREGATE = ["MessageExpunge", "FlagsSet", "FlagsClear", "MessageTrash", "MessageAppend"]


class KafkaBackupProducer:
    def __init__(self) -> None:
        self.bootstrap_servers = KafkaProducerConfig.KAFKA_BROKER
        self.normal_topic = KafkaProducerConfig.KAFKA_PRODUCER_NORMAL_TOPIC
        self.aggregated_topic = (
            KafkaBackupProducer.KAFKA_PRODUCER_AGGREGATED_TOPIC
        )
        self.value_serializer = lambda x: json.dumps(x).encode("utf-8")
        self.kafka_msgs = []
        self.sasl_plain_username = KafkaAuth.SASL_PLAIN_USERNAME
        self.sasl_plain_password = KafkaAuth.SASL_PLAIN_PASSWORD
        self.security_protocol = KafkaAuth.SECURITY_PROTOCOL
        self.sasl_mechanism = KafkaAuth.SASL_MECHANISM
        self.ssl_context = context

    def ordered_message(self, user_messages: dict):
        for user in user_messages:
            messages = user_messages[user]
            messages.sort(key=lambda x: x[0])
            for priority, message in messages:
                self.create_kafka_message(message)

    def create_kafka_message(self, message: dict):
        uids = len(message.get("uids", []))
        user = message.get("user")
        username, _, domain = user.partition("@")
        msg_format = {"payload": message}
        if uids > 1 or message.get("event") in AGGREGATE:
            if domain in KafkaProducerConfig.KAFKA_IGNORE_DOMAIN:
                return
            topic = self.aggregated_topic
            msg_format.update({"key": user, "topic": topic})
        else:
            topic = self.normal_topic
            msg_format.update({"key": user, "topic": topic})
        self.kafka_msgs.append(msg_format)

    def send_message(self):
        producer = KafkaProducer(
            bootstrap_servers=self.bootstrap_servers,
            value_serializer=self.value_serializer,
            sasl_plain_username=self.sasl_plain_username,
            sasl_plain_password=self.sasl_plain_password,
            security_protocol=self.security_protocol,
            ssl_context=self.ssl_context,
            sasl_mechanism=self.sasl_mechanism,
            acks="all"
        )
        for msg in self.kafka_msgs:
            payload = msg.get("payload", {})
            kafka_topic = msg.get("topic")
            kafka_key = msg.get("key")
            logger.info(
                "Sending message: {} to topic: {}".format(payload, kafka_topic)
            )
            uids = payload.get("uids") or []
            slice = KafkaBackupProducer.KAFKA_SLICE_SIZE
            while len(uids) >= slice:
                p = uids[:slice]
                uids = uids[slice:]
                payload["uids"] = p
                producer.send(kafka_topic, key=bytes(kafka_key, "utf-8"), value=payload)
                producer.flush()
            else:
                if uids:
                    payload["uids"] = uids                
                    producer.send(kafka_topic, key=bytes(kafka_key, "utf-8"), value=payload)
                producer.flush()
        self.kafka_msgs.clear()


class KafkaBackupConsumer:
    def __init__(self) -> None:
        print(KafkaConsumerConfig.KAFKA_BROKER)
        self.consumer = KafkaConsumer(
            bootstrap_servers=["192.168.6.202:9093"],
            auto_offset_reset=KafkaConsumerConfig.KAFKA_AUTO_OFFSET_RESET,
            value_deserializer=lambda x: json.loads(x.decode("utf-8")),
            enable_auto_commit=KafkaConsumerConfig.KAFKA_ENABLE_AUTO_COMMIT,
            max_poll_records=KafkaConsumerConfig.KAFKA_MAX_POLL_RECORDS,
            sasl_plain_username = KafkaAuth.SASL_PLAIN_USERNAME,
            sasl_plain_password = KafkaAuth.SASL_PLAIN_PASSWORD,
            security_protocol = KafkaAuth.SECURITY_PROTOCOL,
            sasl_mechanism = KafkaAuth.SASL_MECHANISM,
            ssl_context = context
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
            raise Exception("could not found offset and timestamp")
        offset_start = offset_timestamp_start.offset
        offset_end = offset_timestamp_end.offset
        return offset_start, offset_end
