import datetime


def convert_to_timestamp(date_time_str):
    element = datetime.datetime.strptime(date_time_str, "%m/%d/%Y %H:%M:%S")
    timestamp = int(datetime.datetime.timestamp(element))
    return timestamp


def handle_timestamp(start, end):
    start = start.replace(",", " ")
    end = end.replace(",", " ")
    timestamp_start = convert_to_timestamp(start)
    timestamp_end = convert_to_timestamp(end)
    return timestamp_start * 1000, timestamp_end * 1000