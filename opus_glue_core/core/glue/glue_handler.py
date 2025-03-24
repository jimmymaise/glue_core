import json
import logging
import time

import boto3


# from core.config import get_logger


class GlueJobHandler:
    def __init__(self, job_name, glue_client=None):
        self.job_name = job_name
        self.glue_client = glue_client or boto3.client('glue')
        self.logger = logging.getLogger()

    def run(self, job_args, delay_seconds_each_status_check_attempt: int = 30, timeout: int = None):
        response = self.glue_client.start_job_run(JobName=self.job_name, Arguments=job_args)
        job_id = response['JobRunId']
        self.logger.info(
            f'starting run Glue Job: {self.job_name} with job_id {job_id} job_args  {json.dumps(job_args)}')

        i = 0
        check_status_attempts = 0
        if timeout:
            check_status_attempts = int(timeout / delay_seconds_each_status_check_attempt)

        while not check_status_attempts or i < check_status_attempts:
            response = self.glue_client.get_job_run(JobName=self.job_name, RunId=job_id)
            status = response['JobRun']['JobRunState']
            if status == 'SUCCEEDED':
                self.logger.info(
                    f'Complete run Glue Job: {self.job_name} with job_id {job_id}  job_args  {json.dumps(job_args)}')
                time.sleep(delay_seconds_each_status_check_attempt)
                return True
            elif status in ['RUNNING', 'STARTING', 'STOPPING']:
                self.logger.info(
                    f'This glue job  {self.job_name} with job_id {job_id} is {status}. Waiting it to complete')
                time.sleep(delay_seconds_each_status_check_attempt)
                i += 1
            else:
                self.logger.error(f'Error during  {self.job_name}  execution: {status}')
                return False
            if i == check_status_attempts:
                self.logger.error(
                    f'Execution timeout: {check_status_attempts * delay_seconds_each_status_check_attempt} seconds')
                return False

    def trigger_crawler(self, crawler_name):
        self.logger.info(
            f'Trigger {crawler_name}')
        self.glue_client.start_crawler(Name=crawler_name)
