import datetime
from re import L
from typing import Dict, List
import json
from resend_kafka_message.logic.client.kafka_client import (
    KafkaBackupConsumer,
    KafkaBackupProducer,
)
from resend_kafka_message.utils.logger import logger
from resend_kafka_message.utils.decorator import retry


def convert_to_timestamp(datetime_str: str):
    time_format = None
    if "AM" in datetime_str or "PM" in datetime_str:
        time_format = datetime_str[-2:]
        local_time = datetime.datetime.strptime(datetime_str[:-2], "%m/%d/%Y %H:%M:%S")
    else:
        local_time = datetime.datetime.strptime(datetime_str, "%m/%d/%Y %H:%M:%S")
    if time_format == "PM":
        new_datetime = datetime_str.replace(time_format, "")
        local_time = datetime.datetime.strptime(new_datetime, "%m/%d/%Y %H:%M:%S")
        hours_added = datetime.timedelta(hours=12)
        if local_time.hour != 12:
            local_time = local_time + hours_added
    elif time_format == 'AM' and local_time.hour == 12:
        new_datetime = datetime_str.replace(time_format, "")
        local_time = datetime.datetime.strptime(new_datetime, "%m/%d/%Y %H:%M:%S")
        hours_added = datetime.timedelta(hours=12)
        local_time = local_time - hours_added
    logger.info(f"LOCAL TIME: {local_time}")
    timestamp = int(datetime.datetime.timestamp(local_time))
    return timestamp


def handle_timestamp(start: str, end: str, ):
    start = start.replace(",", " ")
    end = end.replace(",", " ")
    timestamp_start, timestamp_end = [
        convert_to_timestamp(time) for time in [start, end]
    ]
    return timestamp_start * 1000, timestamp_end * 1000


def get_offset_with_timestamp(
    consumer: KafkaBackupConsumer,
    partition: int,
    start: str,
    end: str,
):
    time_start, time_end = handle_timestamp(start, end)
    offset_start, offset_end = consumer.get_offset(
        partition, time_start, time_end
    )
    return offset_start, offset_end


def send_kafka_message(user: str, event: Dict, partition: int):
    logger.info(f"SENDING MESSAGE | USER: {user} | MESSAGE: {event}")
    producer = KafkaBackupProducer()
    producer.create_kafka_message(event)
    producer.send_message()
    logger.info("DONE")


@retry(times=3, delay=2, logger=logger)
def poll_message(
    consumer: KafkaBackupConsumer,
    user: str,
    partition: int,
    offset_start: int,
    offset_end: int,
    list_event_type: List=None,
):
    logger.info(f"OFFSET START: {offset_start}")
    logger.info(f"OFFSET END: {offset_end}")
    try:
        position = consumer.current_position(partition)
        if position > 0:
            offset_start = position
    except AssertionError:
        consumer.assign_partition(partition)
    offset = consumer.seek_message(partition, offset_start)
    for _ in range(offset_start, offset_end):
        msg = next(offset)
        event = msg.value
        if event["user"] != user:
            continue
        if list_event_type and event["event"] not in list_event_type:
                continue
        send_kafka_message(user, event, partition)


def resend_with_timestamp(
    user: str,
    partition: int,
    time_start: str,
    time_end: str,
    list_event_type: List=None,
):
    consumer = KafkaBackupConsumer()
    offset_start, offset_end = get_offset_with_timestamp(
        consumer, partition, time_start, time_end
    )
    if offset_end is None or offset_start is None:
        logger.info("could not found")
        consumer.kafka_close()
        return
    poll_message(
        consumer, user, partition, offset_start, offset_end, list_event_type
    )
    consumer.kafka_close()


def resend_with_offset(
    user: str,
    partition: int,
    offset_start: int,
    offset_end: int,
    list_event_type: List=None,
):
    consumer = KafkaBackupConsumer()
    poll_message(
        consumer,
        user,
        partition,
        int(offset_start),
        int(offset_end),
        list_event_type,
    )
    consumer.kafka_close()


def resend_with_file(file_name: str):
    logger.info('File name: {}'.format(file_name))
    with open(file_name, "r") as f:
        data = f.readlines()
        for i in data:
            logger.info(f"Event: {i}")
            user, partition, offset_start, offset_end = eval(i)
            resend_with_offset(
                user, int(partition), int(offset_start), int(offset_end)
            )


def main(arg):
    with_timestamp = arg.get("resend_with_timestamp")
    with_offset = arg.get("resend_with_offset")
    with_file = arg.get("resend_with_file")
    if with_file:
        file_name = with_file[0]
        resend_with_file(file_name)

    if with_timestamp:
        user = with_timestamp[0]
        partition = int(with_timestamp[1])
        time_start = with_timestamp[2]
        time_end = with_timestamp[3]
        list_event_type = None
        if len(with_timestamp) == 5:
            list_event_type = with_timestamp[4].split(",")
        resend_with_timestamp(
            user, partition, time_start, time_end, list_event_type
        )
    elif with_offset:
        user = with_offset[0]
        partition = int(with_offset[1])
        offset_start = with_offset[2]
        offset_end = with_offset[3]
        list_event_type = None
        if len(with_offset) == 5:
            list_event_type = with_offset[4].split(",")
        resend_with_offset(user, partition, offset_start, offset_end, list_event_type)
    else:
        logger.info("python3 resend_kafka_message/run.py -h for help")
