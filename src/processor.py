import os
import sys
import logging
import signal
import re

from psycopg import Connection, connect

logger = logging.getLogger()


class LogProcessor():
    def __init__(self, log_path: str, rules: dict, handlers, verbose: bool, dry_run: bool, dsn: str = None, connection: Connection | None = None):
        self.log_path = log_path
        self.rules = rules
        self.handlers = handlers
        self.verbose = verbose
        self.dry_run = dry_run
        self.conn = self._setup_connection(dsn, connection)
        self.ops = []

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


    def _setup_connection(self, dsn: str, connection: Connection) -> Connection:
        """
        Will return the provided connection if one exists (useful for testing). Otherwise will attempt to connect to the provided DSN.
        """
        try:
            logger.info("Connecting to database.")

            if connection is not None:
                return connection

            return connect(dsn, autocommit=False)
        except Exception as e:
            logger.error("Could not connect to database.")
            logger.error(e)
            sys.exit(1)

    
    def run_current_ops(self):
        try:
            with self.conn.transaction():
                with self.conn.cursor() as cur:
                    for (sql, args) in self.ops:
                        if args is not None:
                            cur.execute(sql, args)
                        else:
                            cur.execute(sql)
            self.ops = []
        except Exception as e:
            raise e


    def batch_run_sql(self, sql: str, args: tuple = None) -> None:
        """
        Adds the provided sql and args as a tuple to an internal ops list. When the length of this list is 1000, execute them all 
        """
        self.ops.append((sql, args))

        if len(self.ops) == 1000:
            logger.info("Running batch of 1000.")
            self.run_current_ops()


    def _process_line(self, line: str, ruleset: dict) -> None:
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
            no_handler = True

            # For every rule in our dictionary
            for rule in ruleset:
                # Try and apply the rule to the current line
                match = re.match(rule, line)

                if match is not None:
                    no_handler = False
                    
                    handler = getattr(self.handlers, ruleset[rule])
                    handler(self, line, match)

                    break

            if no_handler:
                raise ValueError

        except ValueError:
            logger.warn("No matching rule was found for the line below. Please create one.")
            logger.warn(line)
            raise
        except re.error:
            logger.error(f"Invalid regex: {rule}")
            raise


    def _process_file(self, file_path: str, ruleset: dict) -> None:
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
            with open(file_path) as file:
                for line in file:
                    self._process_line(line, ruleset)
        except IOError:
            logger.info(f"Could not open file {file_path}")


    def run(self) -> None:
        """
        Method to go and process logs!
        """
        try:
            self.handlers.on_start(self)

            # For each file in our log directory
            for file_name in os.listdir(self.log_path):
                file_path = os.path.join(self.log_path, file_name)

                # Only attempt to process files, will go one level deep.
                if os.path.isfile(file_path):
                    # For each set of rules that we've defined
                    for ruleset in self.rules.keys():
                        # If the key of the ruleset is a regex match for the filename, use that ruleset to process the file.
                        if re.search(ruleset, file_name):
                            self._process_file(file_path, self.rules[ruleset])

            # After we've finished processing, run the on_finish handler and execute any remaining operations.
            self.handlers.on_finish(self)
            self.run_current_ops()
        except Exception as e:
            logger.error(e)