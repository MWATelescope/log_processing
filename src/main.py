import logging

from processor import LogProcessor
from cli import parse_arguments


def main() -> None:
    logging.basicConfig(format='[%(asctime)s %(levelname)s] %(message)s')

    args = parse_arguments()

    log_processor = LogProcessor(args)
    log_processor.run()


if __name__ == "__main__":
    main()