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

            return connect(dsn, autocommit=True)
        except Exception as e:
            logger.error("Could not connect to database.")
            logger.error(e)
            sys.exit(1)
        

    def run_sql(self, sql: str, args: tuple = None):
        try:
            with self.conn.cursor() as cur:
                if args is not None:
                    cur.execute(sql, args)
                else:
                    cur.execute(sql)
            self.conn.commit()
        except Exception as e:
            raise e


    def _process_line(self, line: str) -> None:
        """
        Method to process a single line of a log file.

        Parameters
        ----------
        line: str
            A line from a log file
        """
        if self.dry_run:
            return

        try:
            no_handler = True

            # For every rule in our dictionary
            for rule in self.rules.keys():
                # Try and apply the rule to the current line
                match = re.search(rule, line)

                if match is not None:
                    no_handler = False
                    
                    handler = getattr(self.handlers, self.rules[rule])
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


    def _process_file(self, file_path: str) -> None:
        """
        Method to process a single log file

        Parameters
        ----------
        file_path: str
            Path to a log file to be processed
        """
        try:
            with open(file_path) as file:
                for line in file:
                    self._process_line(line)
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
                    self._process_file(file_path)

            self.handlers.on_finish(self)
        except Exception as e:
            logger.error(e)