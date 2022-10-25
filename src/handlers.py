import json
import logging

from datetime import datetime
from enum import Enum

logger = logging.getLogger()


class JobState(Enum):
    QUEUED = 0
    PROCESSING = 1
    COMPLETE = 2
    ERROR = 3
    EXPIRED = 4
    CANCELLED = 5


def skip(processor, line, match):
    return

def on_start(processor):
    logger.info("Cleaning up table.")

    sql = """
        TRUNCATE TABLE jobs_history;
    """
    processor.batch_run_sql(sql)

    
def on_finish(processor):
    sql = """
        UPDATE jobs_history
        SET error_code = 1, error_text = "job_failed", completed = started
        WHERE job_state = 1
    """
    processor.batch_run_sql(sql)

    logger.info("Finishing..")


def consumed_message(processor, line, match):
    timestamp = match.group(1)
    job_id = match.group(2)
    job_type = match.group(3)
    user_id = match.group(4)
    job_params = json.loads(match.group(5).replace("'", '"'))

    logger.info(f"Job created: {job_id}")

    timestamp = datetime.strptime(timestamp, "%Y-%m-%d %H:%M:%S")

    sql = """
        INSERT INTO jobs_history (id, job_type, user_id, job_params, job_state, started)
        VALUES
            (%s, %s, %s, %s, %s, %s)
        ON CONFLICT do nothing;
    """

    params = (job_id, job_type, user_id, json.dumps(job_params), JobState.PROCESSING.value, timestamp)
    processor.batch_run_sql(sql, params)


def cancel(processor, line, match):
    timestamp = match.group(1)
    job_id = match.group(2)

    logger.info(f"Job cancelled: {job_id}")

    timestamp = datetime.strptime(timestamp, "%Y-%m-%d %H:%M:%S")

    sql = """
        UPDATE jobs_history
        SET job_state = %s, completed = %s
        WHERE id = %s
    """

    params = (JobState.CANCELLED.value, timestamp, job_id)
    processor.batch_run_sql(sql, params)


def complete(processor, line, match):
    timestamp = match.group(1)
    job_id = match.group(2)
    product = json.loads(match.group(3).replace("'", '"').replace("None", "null"))

    logger.info(f"Job completed: {job_id}")

    timestamp = datetime.strptime(timestamp, "%Y-%m-%d %H:%M:%S")

    sql = """
        UPDATE jobs_history
        SET job_state = %s, product = %s, completed = %s
        WHERE id = %s
    """

    params = (JobState.COMPLETE.value, json.dumps(product), timestamp, job_id)
    processor.batch_run_sql(sql, params)