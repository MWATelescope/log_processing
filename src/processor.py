import os
import sys
import logging
import signal
import re

from handlers import Handler
from repository import Repository

logger = logging.getLogger()


class LogProcessor():
    def __init__(
        self,
        log_path: str,
        dry_run: bool,
        repository: Repository,
        handler: Handler,
    ):
        self.log_path = log_path
        self.dry_run = dry_run
        self.repository = repository
        self.handler = handler

        for sig in [signal.SIGINT]:
            signal.signal(sig, self._signal_handler)

        if self.dry_run:
            logger.info("Dry run enabled.")


    def _signal_handler(self, sig, frame):
        """
        Function to handle interrupts.
        """

        logger.info(f"Interrupted! Received signal {sig}.")
        sys.exit(0)


    def _process_line(self, file_path: str, line: str, ruleset: str) -> None:
        """
        Method to process a single line of a log file.

        Parameters
        ----------
        line: str
            A line from a log file
        ruleset: dict
            Dictionary which defines all of the rules needed to process the file where the key is regex to match a line, and the value is the name of the function used to process the line.
        """
        if self.dry_run:
            logger.info(f"Processing line: {line}.")
            return

        try:
            # For every rule in our dictionary
            for rule in self.handler.rules[ruleset]:
                # Try and apply the rule to the current line
                match = re.match(rule, line)

                if match is not None:
                    handler = self.handler.rules[ruleset][rule]
                    handler(self.repository, file_path, line, match)

                    break
            else:
                raise ValueError

        except ValueError:
            logger.warn("No matching rule was found for the line below. Please create one.")
            logger.warn(line)
            raise
        except re.error:
            logger.error(f"Invalid regex: {rule}")
            raise
        except Exception:
            logger.error("There was an error with the line below.")
            logger.warn(line)
            raise


    def _process_file(self, file_path: str, ruleset: str) -> None:
        """
        Method to process a single log file

        Parameters
        ----------
        file_path: str
            Path to a log file to be processed
        ruleset: dict
            Dictionary which defines all of the rules needed to process the file where the key is regex to match a line, and the value is the name of the function used to process the line.
        """
        try:
            with open(file_path, encoding='utf8', errors="ignore") as file:
                for line in file:
                    self._process_line(file_path, line, ruleset)
        except IOError:
            logger.info(f"Could not open file {file_path}")
            raise


    def process_folder(self, folder_name):
        for inode in os.listdir(folder_name):
            path = os.path.join(folder_name, inode)

            if os.path.isfile(path):
                # For each set of rules that we've defined
                for ruleset in self.handler.rules.keys():
                    # If the key of the ruleset is a regex match for the filename, use that ruleset to process the file.
                    if re.search(ruleset, path):
                        self._process_file(path, ruleset)
            else:
                self.process_folder(path)


    def run(self) -> None:
        """
        Method to go and process logs!
        """
        try:
            self.handler.startup(self.repository)

            self.process_folder(self.log_path)

            self.handler.shutdown(self.repository)
        except Exception as e:
            logger.error(e)