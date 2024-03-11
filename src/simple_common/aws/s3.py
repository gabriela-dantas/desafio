import boto3

from botocore.exceptions import ClientError

from simple_common.logger import logger


class S3:
    def __init__(self) -> None:
        self.__s3_client = boto3.client("s3")
        self.__logger = logger

    def get_file_content(self, bucket: str, key: str) -> bytes:
        try:
            response = self.__s3_client.get_object(
                Bucket=bucket,
                Key=key,
            )
            return response["Body"].read()
        except ClientError as error:
            self.__logger.exception(
                f"Erro ao buscar arquivo {key} no bucket {bucket}: {error}"
            )
            raise error

    def put_object(self, bucket: str, key: str, data: bytes) -> None:
        try:
            self.__s3_client.put_object(Body=data, Bucket=bucket, Key=key)
        except ClientError as error:
            self.__logger.exception(
                f"Erro ao buscar escrever arquivo {key} no bucket {bucket}: {error}"
            )
            raise error

    def copy_object(self, target_bucket: str, target_key: str,
                    source_bucket: str, source_key: str) -> None:
        try:
            copy_source = {'Bucket': source_bucket, 'Key': source_key}
            self.__s3_client.copy_object(
                Bucket=target_bucket, CopySource=copy_source, Key=target_key)
            self.__s3_client.delete_object(Bucket=target_bucket, Key=source_key)
        except ClientError as error:
            self.__logger.exception(
                f"Erro ao copiar objeto do bucket {source_bucket}/{source_key} "
                f"para bucket {target_bucket}{target_key}: {error}"
            )
            raise error
