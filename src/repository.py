import sys
import logging
from abc import abstractmethod, ABC

from psycopg import Connection, connect

logger = logging.getLogger()


class Repository(ABC):
    def __init__(self, dsn: str | None = None, connection=None, BATCH_SIZE=1000):
        self.conn = self._setup_connection(dsn, connection)
        self.BATCH_SIZE = BATCH_SIZE
        self.ops = []

    @abstractmethod
    def _setup_connection(self, dsn: str | None = None, connection=None):
        """
        Abstract method to be implemented in a concrete class. Should return a connection to some data store.
        """
        raise NotImplementedError

    @abstractmethod
    def run_current_ops(self):
        """
        The programmer should implement a concrete method which gets all of the (sql, params) tuples from self.ops and runs them inside of a transaction.
        """
        raise 

    def batch_run_sql(self, sql: str, args: tuple = None) -> None:
        """
        Adds the provided sql and args as a tuple to an internal ops list. When the length of this list is 1000, execute them all 
        """
        self.ops.append((sql, args))

        if len(self.ops) == self.BATCH_SIZE:
            logger.info(f"Running batch of {self.BATCH_SIZE}.")
            self.run_current_ops()


class PostgresRepository(Repository):
    def __init__(self, dsn: str | None, connection=None):
        super().__init__(dsn, connection)

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
        """
        Opens a transaction, executes all of the (sql, params) tuples in self.ops inside of the transaction, and commits it.
        """
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
