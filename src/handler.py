import sys
import json
import logging

from enum import Enum
from datetime import datetime

from psycopg import connect, Connection

logger = logging.getLogger()


def on_startup(func):
    def wrapped(self):
        self.startup_functions.append(func)
    return wrapped

def on_shutdown(func):
    def wrapped(self):
        self.shutdown_functions.append(func)
    return wrapped



class HandlerBase:
    def __init__(self):
        self.startup_functions = []
        self.shutdown_functions = []

    def startup(self):
        for func in self.startup_functions:
            func()

    def shutdown(self):
        for func in self.shutdown_functions:
            func()

    def skip(self):
        return
    
class PostgresHandler(HandlerBase):
    def __init__(self, dsn: str | None = None, connection: Connection | None = None, setup_script: str | None = None, BATCH_SIZE: int = 1000):
        self.conn: Connection = self._setup_connection(dsn=dsn, connection=connection)
        self.ops: list[tuple] = []
        self.BATCH_SIZE: int = BATCH_SIZE
        self._setup_db(setup_script=setup_script)

    @on_shutdown
    def _close(self):
        self.conn.close()

    def _setup_connection(self, dsn: str | None = None, connection: Connection | None = None) -> Connection:
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

    def _setup_db(self, setup_script: str | None = None):
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
            self._run_ops(self.ops)
            self.ops = []

    def _run_ops(self, ops: list[tuple]):
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
            logger.error("Could not execute database operation.")
            raise e


class JobState(Enum):
    QUEUED = 0
    PROCESSING = 1
    COMPLETE = 2
    ERROR = 3
    EXPIRED = 4
    CANCELLED = 5


class LogHandler(PostgresHandler):
    def __init__(self, dsn: str | None = None, connection: Connection | None = None, setup_script: str | None = None):
        super().__init__(dsn=dsn, connection=connection, setup_script=setup_script)

        self.THRESHOLD = 60 * 60 * 24
        self.obs_downloads = {}

    @on_startup
    def start_processing(self):
        logger.info("Starting processing.")

    @on_shutdown
    def cleanup_asvo(self):
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

    def consumed_message(self, file_path, line, match):
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

    def cancel(self, file_path, line, match):
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

    def complete(self, file_path, line, match):
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

    def query(self, file_path, line, match):
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
        log_datetime = match.group(1)
        filename = match.group(3)
        timestamp = time.mktime(datetime.strptime(log_datetime, "%Y-%m-%d %H:%M:%S").timetuple())
        obs_id = int(filename.split('_')[0])

        logger.info(f"Processing file {filename}")

        if obs_id in self.obs_downloads:
            # If we've seen it before
            for download in self.obs_downloads[obs_id]:
                # For each download for our obs_id
                if download['earliest'] - self.THRESHOLD < timestamp < download['latest'] + self.THRESHOLD:
                    # If the timestamp for our entry is within some threshold of this download
                    if filename not in download['files']:
                        # We haven't see the file in this download, add it and update the most_recent timestamp if it's newer than what we have already. Then stop looking.
                        download['files'].append(filename)
                        download['latest'] = timestamp if timestamp > download['latest'] else download['latest']
                        download['earliest'] = timestamp if timestamp < download['earliest'] else download['earliest']

                        break
                    else:
                        # We found the file in the current download, check the next one.
                        continue
                else:
                    # This timestamp is outside of the threshold for the current download, try the next one.
                    continue
            else:
                # This file is part of a new download, initialise it and add it to the downloads for this obs id.
                download = {
                    'latest': timestamp,
                    'earliest': timestamp,
                    'files': [filename]
                }

                self.obs_downloads[obs_id].append(download)       
        else:
            # We havent seen this obs_id before, create a new download for it.
            download = {
                'latest': timestamp,
                'earliest': timestamp,
                'files': [filename]
            }

            self.obs_downloads[obs_id] = [download]

    @on_shutdown
    def cleanup_ngas(self):
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