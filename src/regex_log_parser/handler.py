import sys
import atexit
import logging

from typing import Optional
from abc import ABC, abstractmethod

from psycopg import connect, Connection

logger = logging.getLogger()


class HandlerBase(ABC):
    """
    Base handler class, provides the skip method and forces any subclassed objects to implement startup and shutdown methods.
    """
    @abstractmethod
    def startup(self):
        """
        Abstract method which should be overridden. Will be ran at the start of the process, can be used for setup.
        """
        raise NotImplementedError

    @abstractmethod
    def shutdown(self):
        """
        Abstract method which should be overridden. Will be ran at the end of the process, can be used for cleanup.
        """
        raise NotImplementedError

    def skip(self, *args):
        """
        Method used to skip a line.
        """
        return


class PostgresHandler(HandlerBase):
    def __init__(self, dsn: Optional[str] = None, connection: Optional[Connection] = None, setup_script: Optional[str] = None, BATCH_SIZE: int = 1000):
        super().__init__()
        self.conn: Connection = self._setup_connection(dsn=dsn, connection=connection)
        self.ops: list[tuple] = []
        self.BATCH_SIZE: int = BATCH_SIZE
        self._setup_db(setup_script=setup_script)

        if connection is None:
            # Test database will close cleanly causing this to fail, so only register the exit handler if we're not using a test database.
            atexit.register(self._close)

    def _close(self):
        """
        Run any outstanding operations in our queue and then close the connection to the database.
        """
        self._run_ops(self.ops)
        self.conn.close()

    def _setup_connection(self, dsn: Optional[str] = None, connection: Optional[Connection] = None) -> Connection:
        """
        Will return the provided connection if one exists (useful for testing). Otherwise will attempt to connect to the provided DSN.

        Parameters
        ----------
        dsn: str
            Postgresql connection string.
        connection: Connection
            Existing psycopg3 database connection.

        Returns
        -------
        Connection
            psycopg3 database connection.
        """
        try:
            logger.info("Connecting to database.")

            if connection is not None:
                if not isinstance(connection, Connection):
                    raise Exception("Provided connection is not a psycopg3 connection.")
                    
                return connection

            return connect(dsn, autocommit=False)
        except Exception as e:
            logger.error("Could not connect to database.")
            logger.error(e)
            sys.exit(1)

    def _setup_db(self, setup_script: Optional[str] = None):
        """
        Run a database setup script.

        Parameters
        ----------
        setup_script: str
            Path to a SQL file which will be executed by the database.
        """
        if setup_script is not None:
            try:
                logger.info("Running db setup script.")

                with open(setup_script, 'r') as f:
                    self.queue_op(f.read(), run_now=True)
            except (IOError, FileNotFoundError) as e:
                logger.error("Could not open the database setup script.")

    def queue_op(self, sql: str, args: Optional[tuple] = None, run_now: bool = False) -> None:
        """
        Adds the provided sql and args as a tuple to an internal ops list. When the length of this list is BATCH_SIZE, execute them all.

        Parameters
        ----------
        sql: str
            SQL to be executed
        args: tuple
            Args tuple which will be injected into sql
        run_now: bool
            Whether to run the current operation now or queue it up for later.
        """
        self.ops.append((sql, args))

        # By default, execute commands in batches of BATCH_SIZE, or override with run_now
        if len(self.ops) == self.BATCH_SIZE or run_now:
            self._run_ops(self.ops)
            self.ops = []

    def _run_ops(self, ops: list[tuple]):
        """
        Opens a transaction, executes all of the (sql, params) tuples in ops inside of the transaction, and commits it.

        ops: list[tuple]
            A list of (sql, args) pairs to be executed.
        """
        try:
            with self.conn.transaction():
                with self.conn.cursor() as cur:
                    logger.info(f"Running batch of {len(ops)}.")

                    for (sql, args) in ops:
                        if args is not None:
                            cur.execute(sql, args)
                        else:
                            cur.execute(sql)
        except Exception as e:
            logger.error("Could not execute database operation.")
            raise e