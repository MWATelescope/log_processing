import sys
import logging
import argparse

from configparser import ConfigParser

from processor import LogProcessor
from handler import LogHandler
from rules import rules

logging.basicConfig(format='[%(asctime)s %(levelname)s] %(message)s', stream=sys.stdout)
logger = logging.getLogger()


def get_dsn(config: ConfigParser) -> str:
    """
    From a provided ConfigParser object, return the postgres DSN that can be used to connect to the database.

    Parameters
    ----------
    config: ConfigParser
        ConfigParser object

    Returns
    -------
    str
        dsn string
    """

    db_config = {
        'host': config.get("database", "host"),
        'port': config.get("database", "port"),
        'name': config.get("database", "db"),
        'user': config.get("database", "user"),
        'pass': config.get("database", "pass"),
    }

    return f"postgresql://{db_config['user']}:{db_config['pass']}@{db_config['host']}:{db_config['port']}/{db_config['name']}"
    

def read_config(file_name: str) -> ConfigParser:
    """
    Parameters
    ----------
    file_name: str
        Path to a config file.

    Returns
    -------
    ConfigParser
        ConfigParser object with parsed information from file.
    """

    config = ConfigParser()

    try:
        with open(file_name) as f:
            config.read_file(f)

        return config
    except (IOError, FileNotFoundError):
        logger.warn("Could not parse config file.")
        sys.exit(1)


def parse_arguments(args: list = sys.argv[1:]) -> argparse.Namespace:
    """
    Function to Namespace object from a given list of arguments
    (defaults to sys.argv)
    Parameters
    ----------
    args: list
        List of arguments to parse.
    Returns
    -------
    list:
        Namespace object with parsed arguments.
    """
    parser = argparse.ArgumentParser()

    parser.add_argument("--log_path", default="../logs")
    parser.add_argument("--cfg", default="../cfg/config.cfg")
    parser.add_argument("--dry_run", action="store_true")
    parser.add_argument("--verbose", "-v", action="store_true", default=True)
    parser.add_argument("--db_setup", default="../db/setup.sql")

    return parser.parse_args(args)


def main() -> None:
    """
    Entrypoint of the application
    """

    args = parse_arguments()
    config = read_config(args.cfg)
    dsn = get_dsn(config)

    if args.verbose:
        logger.setLevel(logging.INFO)
    else:
        logger.setLevel(logging.WARN)

    handler = LogHandler(dsn=dsn, setup_script=args.db_setup)

    log_processor = LogProcessor(
        dry_run=args.dry_run,
        rules=rules,
        handler=handler
    )

    log_processor.run(args.log_path)


if __name__ == "__main__":
    main()