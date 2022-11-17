import sys
import json
import time
import atexit
import logging

from enum import Enum
from datetime import datetime
from collections import defaultdict
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
    def __init__(self, dsn: str | None = None, connection: Connection | None = None, setup_script: str | None = None, BATCH_SIZE: int = 1000):
        super().__init__()
        self.conn: Connection = self._setup_connection(dsn=dsn, connection=connection)
        self.ops: list[tuple] = []
        self.BATCH_SIZE: int = BATCH_SIZE
        self._setup_db(setup_script=setup_script)
        atexit.register(self._close)

    def _close(self):
        """
        Run any outstanding operations in our queue and then close the connection to the database.
        """
        self._run_ops(self.ops)
        self.conn.close()

    def _setup_connection(self, dsn: str | None = None, connection: Connection | None = None) -> Connection:
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

    def _setup_db(self, setup_script: str | None = None):
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

    def queue_op(self, sql: str, args: tuple | None = None, run_now: bool = False) -> None:
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


class JobState(Enum):
    QUEUED = 0
    PROCESSING = 1
    COMPLETE = 2
    ERROR = 3
    EXPIRED = 4
    CANCELLED = 5


THRESHOLD = 60 * 60 * 24 * 6 # 1 day


class LogHandler(PostgresHandler):
    def __init__(self, dsn: str | None = None, connection: Connection | None = None, setup_script: str | None = None):
        super().__init__(dsn=dsn, connection=connection, setup_script=setup_script)
        self.obs_downloads = defaultdict(list)

    def startup(self):
        """
        Function to run at the start of processing.
        """
        self.start_processing()

    def shutdown(self):
        """
        Cleanup function to run at the end of processing.
        """
        #self.asvo_cleanup()
        self.ngas_cleanup()

    def start_processing(self):
        logger.info("Starting processing.")

    def asvo_cleanup(self):
        """
        Runs at the end of the processing routine. We're making the assumption that anything 
        that wasn't completed or cancelled, failed for some reason.
        So go and update the database to set the error fields and completed time properly.
        """
        sql = """
            UPDATE jobs_history
            SET error_code = 1, error_text = 'Job Failed', completed = started, job_state = %s
            WHERE job_state = 1
        """
        params = (JobState.ERROR.value,)
        self.queue_op(sql, params, run_now=True)

        logger.info("Finishing..")

    def asvo_consumed_message(self, file_path, line, match):
        """
        Handler for "consumed message" log entries, which denote the creation of new jobs.
        """
        timestamp = match.group(1)
        job_id = match.group(2)
        job_type = match.group(3)
        user_id = match.group(4)
        job_params = json.loads(match.group(5).replace("'", '"'))
        timestamp = datetime.strptime(timestamp, "%Y-%m-%d %H:%M:%S")

        logger.info(f"Job created: {job_id}")

        sql = """
            INSERT INTO jobs_history (id, job_type, user_id, job_params, job_state, created, started)
            VALUES
                (%s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT do nothing;
        """

        params = (job_id, job_type, user_id, json.dumps(job_params), JobState.PROCESSING.value, timestamp, timestamp)
        self.queue_op(sql, params)

    def asvo_cancel(self, file_path, line, match):
        """
        Handler for "job cancelled" log entries, which denote that a job has been cancelled.
        """
        timestamp = match.group(1)
        job_id = match.group(2)
        timestamp = datetime.strptime(timestamp, "%Y-%m-%d %H:%M:%S")

        logger.info(f"Job cancelled: {job_id}")

        sql = """
            UPDATE jobs_history
            SET job_state = %s, completed = %s
            WHERE id = %s AND job_state = %s
        """

        params = (JobState.CANCELLED.value, timestamp, job_id,JobState.PROCESSING.value)
        self.queue_op(sql, params)

    def asvo_complete(self, file_path, line, match):
        """
        Handler for "visibility download completed" log entries, which denote that a job was processed successfully.
        """
        timestamp = match.group(1)
        job_id = match.group(2)
        product = json.loads(match.group(3).replace("'", '"').replace("None", "null"))
        timestamp = datetime.strptime(timestamp, "%Y-%m-%d %H:%M:%S")

        logger.info(f"Job completed: {job_id}")

        sql = """
            UPDATE jobs_history
            SET job_state = %s, product = %s, completed = %s
            WHERE id = %s 
        """

        params = (JobState.COMPLETE.value, json.dumps(product), timestamp, job_id)                                                                                             
        self.queue_op(sql, params)

    def obsdownload_query(self, file_path, line, match):
        """
        Handler for "obsdownload" log entries, which denote the creation of new jobs.
        """
        created = match.group(1)
        ip_address = match.group(2)
        obs_ids = match.group(3)

        obs_ids = obs_ids.split(',')

        for obs_id in obs_ids:
            if len(obs_id) != 10 or not obs_id.isnumeric():
                logger.error(f"Invalid obs_id {obs_id}")
                return

            logger.info(f"Job created: {obs_id}")

            sql = """
                INSERT INTO obsdownload_history (created, ip_address, obs_id)
                VALUES
                    (%s, %s, %s)
                ON CONFLICT do nothing;   
            """

            params = (created, ip_address, obs_id)
            self.queue_op(sql, params)

    def ngas_retrieve(self, file_path, line, match):
        """
        Function to parse all ngas RETRIEVE lines into a dictionary where:
        - The key is an obs_id
        - The value is an array of "downloads" for that obs_id.
        - Each download contains:
            - earliest: the oldest timestamp for that download (in unix time)
            - latest: the newest timestamp for that download (in unix time)
            - files: a list of files for that download

        Of the format:

        obs_downloads = {
            obs_id_1: [
                {
                    'earliest': 1668647943,
                    'latest': 1668649943,
                    'files': [
                        'obs_id_1_1.fits',
                        'obs_id_1_2.fits'
                    ]
                }
            ]
        }
        """
        log_datetime = match.group(1)
        filename = match.group(3)
        timestamp = time.mktime(datetime.strptime(log_datetime, "%Y-%m-%d %H:%M:%S").timetuple())
        obs_id = int(filename.split('_')[0])

        logger.info(f"Processing file {filename}")

        for download in self.obs_downloads[obs_id]:
            # For each download for our obs_id
            if download['earliest'] - THRESHOLD < timestamp < download['latest'] + THRESHOLD and \
            filename not in download['files']:
                # If the timestamp for our entry is within some threshold of this download and we haven't seen the file before.
                # Add it to our list and update our timestamps if needed.
                download['files'].append(filename)
                download['latest'] = timestamp if timestamp > download['latest'] else download['latest']
                download['earliest'] = timestamp if timestamp < download['earliest'] else download['earliest']

                break
        else:
            # Either we haven't seen this obs_id before, or its part of a new download. Go and create an entry for it.
            self.obs_downloads[obs_id].append({
                'latest': timestamp,
                'earliest': timestamp,
                'files': [filename]
            })

    def ngas_cleanup(self):
        """
        Parse our obs_downloads structure and create corresponding rows in our database.
        """
        #print(json.dumps(obs_downloads, sort_keys=True, indent=4))
        for obs_id in self.obs_downloads:
            for download in self.obs_downloads[obs_id]:
                sql = """
                    INSERT INTO ngas_history (completed, obs_id, num_files)
                    VALUES
                        (%s, %s, %s)
                    ON CONFLICT do nothing;   
                """
                params = (datetime.fromtimestamp(download['latest']), obs_id, len(download['files']))

                self.queue_op(sql, params)