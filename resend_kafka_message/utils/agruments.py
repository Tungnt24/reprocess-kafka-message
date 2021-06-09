import argparse

parser = argparse.ArgumentParser()
arg = parser.add_mutually_exclusive_group()
arg.add_argument(
    "-rwt",
    "--resend_with_timestamp",
    type=str,
    nargs="+",
    default=[],
    help="python3 resend_kafka_message/run.py -rwt example@domain.con partition time_start time_end",
)
arg.add_argument(
    "-rwo",
    "--resend_with_offset",
    type=str,
    nargs="+",
    default=[],
    help="python3 resend_kafka_message/run.py -rwo example@domain.con partition offset_start offset_end",
)
args = parser.parse_args()
