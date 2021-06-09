from resend_kafka_message.utils.agruments import args
from resend_kafka_message.logic.tasks import main


if __name__ == "__main__":
    arg = args.__dict__
    main(arg)
