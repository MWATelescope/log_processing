import os
import sys
import logging
import re

from configparser import ConfigParser

from rules import rules
import handlers


class LogProcessor():
    def __init__(self, args):
        self.verbose = args.verbose
        self.dry_run = args.dry_run
        self.log_path = args.log_path
        self.logger = logging.getLogger()
        self.config = self._read_config(args.cfg)

        if self.dry_run:
            self.logger.info("Dry run enabled.")

        if self.verbose:
            self.logger.setLevel(logging.INFO)
        else:
            self.logger.setLevel(logging.WARN)

    def _read_config(self, file_name: str) -> ConfigParser:
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

        self.logger.info("Parsing config file.")

        config = ConfigParser()

        try:
            with open(file_name) as f:
                config.read_file(f)

            return config
        except (IOError, FileNotFoundError):
            self.logger.error("Could not parse config file.")
            sys.exit(1)


    def _process_line(self, line):
        try:
            no_handler = True

            # For every rule in our dictionary
            for rule in rules.keys():
                # Try and apply the rule to the current line
                match = re.search(rule, line)

                if match is not None:
                    no_handler = False
                    
                    handler = getattr(handlers, rules[rule])
                    handler(line, match)

            if no_handler:
                raise ValueError

        except ValueError:
            self.logger.warn("No matching rule was found for the line below. Please create one.")
            self.logger.warn(line)
            sys.exit(0)


    def _process_file(self, file_path):
        try:
            with open(file_path) as file:
                for line in file:
                    self._process_line(line)
        except IOError:
            self.logger.info(f"Could not open file {file_path}")


    def run(self):
        for file_name in os.listdir(self.log_path):
            file_path = os.path.join(self.log_path, file_name)

            if os.path.isfile(file_path):
                self._process_file(file_path)