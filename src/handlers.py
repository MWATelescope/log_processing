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


def skip(repository, line, match) -> None:
    return


def on_start(repository) -> None:
    logger.info("Starting processing.")

    
def on_finish(repository) -> None:
    """
    Runs at the end of the processing routine. We're making the assumption that anything that wasn't completed or cancelled, failed for some reason.
    So go and update the database to set the error fields and completed time properly.
    """

    sql = """
        UPDATE jobs_history
        SET error_code = 1, error_text = 'Job Failed', completed = started
        WHERE job_state = 1
    """
    repository.queue_op(sql, run_now=True)

    logger.info("Finishing..")


def consumed_message(repository, line, match):
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
    repository.queue_op(sql, params)


def cancel(repository, line, match):
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
        WHERE id = %s
    """

    params = (JobState.CANCELLED.value, timestamp, job_id)
    repository.queue_op(sql, params)


def complete(repository, line, match):
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
    repository.queue_op(sql, params)