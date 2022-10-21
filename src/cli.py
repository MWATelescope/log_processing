import argparse
import sys


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

    return parser.parse_args(args)