import argparse

parser = argparse.ArgumentParser()
arg = parser.add_mutually_exclusive_group()
arg.add_argument(
    "-rwt",
    "--resend_with_timestamp",
    type=str,
    nargs="+",
    help="python3 src/app -rwt example@domain.con topic partition time_start time_end",
)
arg.add_argument(
    "-rwo",
    "--resend_with_offset",
    type=str,
    nargs="+",
    help="python3 src/app -rwo example@domain.con topic partition offset_start offset_end",
)
args = parser.parse_args()
