import json
import time
import logging

from enum import Enum
from datetime import datetime
from collections import defaultdict

from handlers import Handler

logger = logging.getLogger()

class JobState(Enum):
    QUEUED = 0
    PROCESSING = 1
    COMPLETE = 2
    ERROR = 3
    EXPIRED = 4
    CANCELLED = 5

THRESHOLD = 60 * 60 * 24
obs_downloads = defaultdict(dict)
handler = Handler()


@handler.on_startup
def start_processing(repository):
    logger.info("Starting processing.")


#@handler.on_shutdown
def cleanup_asvo(repository):
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
    repository.queue_op(sql, params, run_now=True)

    logger.info("Finishing..")


#@handler.add_rule("processor_down_vis.*", "(.*?),.*Consumed message.*job_id: (.*) job_type: (.*) user_id: (.*) job_params: (.*).*")
#@handler.add_rule("processor_conv_queue.*", "(.*?),.*Consumed message.*job_id: (.*) job_type: (.*) user_id: (.*) job_params: (.*).*")
def consumed_message(repository, file_path, line, match):
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


#@handler.add_rule("processor_down_vis.*", "(.*?),.*Cancel message.*job_id: ([0-9]*) .*")
#@handler.add_rule("processor_conv_queue.*", "(.*?),.*Cancel message.*job_id: ([0-9]*) .*")
def cancel(repository, file_path, line, match):
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
    repository.queue_op(sql, params)


#@handler.add_rule("processor_down_vis.*", "(.*?),.*Visibility download complete.*job_id: ([0-9]*) .* product: (.*)")
#@handler.add_rule("processor_conv_queue.*", "(.*?),.*Conversion complete.*job_id: ([0-9]*) .* product: (.*)")
def complete(repository, file_path, line, match):
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


#@handler.add_rule("proxy\.log.*", "(.*?),.*Client: (\d+\.\d+\.\d+\.\d+) Request Complete. Path.*QUERY.*&like=+(.*)%&.*")
def query(repository, file_path, line, match):
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
        repository.queue_op(sql, params)


#@handler.add_rule(".*nglog.*", ".*RETRIEVE.*")

@handler.add_rule("proxy\.log.*", "(.*?),.*Client: (\d+\.\d+\.\d+\.\d+) Request Complete. Path: \/RETRIEVE\?file_id=(\d+_\d+.*)")
def ngas_retrieve(repository, file_path, line, match):
    log_datetime = match.group(1)
    filename = match.group(3)
    timestamp = time.mktime(datetime.strptime(log_datetime, "%Y-%m-%d %H:%M:%S").timetuple())
    obs_id = int(filename.split('_')[0])

    logger.info(f"Processing file {filename}")

    if obs_id in obs_downloads:
        # If we've seen it before
        for download in obs_downloads[obs_id]:
            # For each download for our obs_id
            if download['earliest'] - THRESHOLD < timestamp < download['latest'] + THRESHOLD:
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

            obs_downloads[obs_id].append(download)       
    else:
        # We havent seen this obs_id before, create a new download for it.
        download = {
            'latest': timestamp,
            'earliest': timestamp,
            'files': [filename]
        }

        obs_downloads[obs_id] = [download]


@handler.on_shutdown
def cleanup_ngas(repository):
    #print(json.dumps(obs_downloads, sort_keys=True, indent=4))
    for obs_id in obs_downloads:
        for download in obs_downloads[obs_id]:
            sql = """
                INSERT INTO ngas_history (completed, obs_id, num_files)
                VALUES
                    (%s, %s, %s)
                ON CONFLICT do nothing;   
            """
            params = (datetime.fromtimestamp(download['latest']), obs_id, len(download['files']))

            repository.queue_op(sql, params)


#@handler.add_rule("processor_down_vis.*", ".*")
#@handler.add_rule("processor_conv_queue.*", ".*")
@handler.add_rule("proxy\.log.*", ".*")
def skip(handler, file_path, line, match):
    #print("skipping")
    return