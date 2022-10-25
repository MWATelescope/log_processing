import sys
import logging
from abc import abstractmethod, ABC

from psycopg import Connection, connect

logger = logging.getLogger()


class Repository(ABC):
    def __init__(self, dsn: str | None = None, setup_script: str = None, connection=None, BATCH_SIZE: int=1000):
        self.conn = self._setup_connection(dsn, connection)
        self.ops: list[tuple] = []
        self.BATCH_SIZE = BATCH_SIZE
        self.setup_db(setup_script)

    @abstractmethod
    def _setup_connection(self, dsn: str | None = None, connection=None):
        """
        Abstract method to be implemented in a concrete class. Should return a connection to some data store.
        """
        raise NotImplementedError

    @abstractmethod
    def run_ops(self, ops: list[tuple]):
        """
        The programmer should implement a concrete method which accepts a list of tuples (containing some SQL and paramters respectively) and run them inside of a transaction.
        """
        raise NotImplementedError

    def setup_db(self, setup_script: str):
        """
        Run a database setup script.
        """
        if setup_script is not None:
            try:
                logger.info("Running db setup script.")

                with open(setup_script, 'r') as f:
                    self.queue_op(f.read(), run_now=True)
            except (IOError, FileNotFoundError) as e:
                logger.error("Could not open the database setup script.")

    def queue_op(self, sql: str, args: tuple | None = None, run_now: bool = False) -> None:
        """
        Adds the provided sql and args as a tuple to an internal ops list. When the length of this list is 1000, execute them all 
        """
        self.ops.append((sql, args))

        # By default, execute commands in batches of BATCH_SIZE, or override with run_now
        if len(self.ops) == self.BATCH_SIZE or run_now:
            logger.info(f"Running batch of {len(self.ops)}.")
            self.run_ops(self.ops)
            self.ops = []


class PostgresRepository(Repository):
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

    def run_ops(self, ops: list[tuple]):
        """
        Opens a transaction, executes all of the (sql, params) tuples in ops inside of the transaction, and commits it.
        """
        try:
            with self.conn.transaction():
                with self.conn.cursor() as cur:
                    for (sql, args) in ops:
                        if args is not None:
                            cur.execute(sql, args)
                        else:
                            cur.execute(sql)
        except Exception as e:
            raise e
