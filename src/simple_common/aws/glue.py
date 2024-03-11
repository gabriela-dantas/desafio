import boto3
import json

from botocore.exceptions import ClientError

from simple_common.logger import logger


class Glue:
    def __init__(self) -> None:
        self.__glue_client = boto3.client("glue")
        self.__logger = logger

    def start_job_for_s3(self, job_name: str, bucket_name: str, file_key: str) -> None:
        try:
            self.__logger.debug(f"Iniciando Job {job_name}")
            response = self.__glue_client.start_job_run(
                JobName=job_name, Arguments={"--bucket_name": bucket_name, "--file_key": file_key})
            status = self.__glue_client.get_job_run(JobName=job_name, RunId=response['JobRunId'])
            self.__logger.debug(f"Job {job_name} iniciado com status {status}")
        except ClientError as error:
            self.__logger.exception(
                f"Erro ao tentar iniciar Job {job_name}: {error}"
            )
            raise error
