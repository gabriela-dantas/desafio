import unidecode

from typing import Dict, Any, Tuple
from aws_lambda_typing import events, context as context_
from datetime import date

from simple_common.logger import logger
from simple_common.utils import set_lambda_response
from simple_common.constants import HTTPStatusCodes
from simple_common.aws.s3 import S3
from simple_common.aws.glue import Glue


def lambda_handler(event: events.S3Event, _context: context_.Context) -> Dict[str, Any]:
    logger.info(f"Recebido evento do S3 {event}")
    message = SftpFileHandler().move(event)
    return set_lambda_response(
        HTTPStatusCodes.OK.value, message
    )


class SftpFileHandler:
    def __init__(self) -> None:
        self.__s3_service = S3()
        self.__logger = logger
        self.__glue = Glue()

    def __get_file_key(self, file_key: str) -> Tuple[str, str]:
        key_by_adm: Dict[str, dict] = {
            "volkswagen": {
                "cotasdocliente": {
                    "prefix": "asset-portfolio",
                    "job_name": "from_asset_portfolio_file_to_stage_raw_"
                }
            }
        }

        keys = file_key.split('/')
        adm = keys[0]
        file_name = keys[1]

        try:
            file_name = unidecode.unidecode(
                file_name.lower().replace(" ", "")
            )
            keys_by_file_name = key_by_adm[adm]
            target_key = list(filter(lambda key: key in file_name, list(keys_by_file_name.keys())))
            target_data = keys_by_file_name[target_key[0]]

            prefix = target_data["prefix"]
            job_name = target_data["job_name"]
            job_name += adm

            today = date.today().strftime("%Y-%m-%d")
            file_key = f"{adm}/{prefix}/received/{today}-{file_name}"
            return file_key, job_name
        except (KeyError, IndexError):
            message = f"Prefixo do arquivo não mapeado: {file_name}"
            self.__logger.warning(message)
            raise KeyError(message)

    def move(self, event: dict) -> str:
        try:
            params = event['detail']['requestParameters']
            bucket_name = params["bucketName"]
            file_key = params["key"]
        except KeyError as error:
            self.__logger.exception(
                f"Erro ao tentar obter nome do bucket e chave do arquivo sftp: {error}", exc_info=error
            )
            raise error

        try:
            new_file_key, job_name = self.__get_file_key(file_key)
        except KeyError as error:
            return str(error)

        self.__logger.debug(
            f"Movendo arquivo no bucket {bucket_name} com chave {file_key}"
        )
        self.__s3_service.copy_object(bucket_name, new_file_key, bucket_name, file_key)

        self.__glue.start_job_for_s3(job_name, bucket_name, new_file_key)

        message = "Arquivo movido para diretório correspondente no bucket."
        self.__logger.info(message)
        return message


if __name__ == "__main__":  # pragma: no cover
    s3_event_by_event_bridge = {
        "detail": {
            "requestParameters": {
                "bucketName": "sftp-archive-adms-sandbox",
                "key": "volkswagen/CotasDoClienteSPBAZAR20231010.xlsx"
            }
        }
    }

    lambda_handler(s3_event_by_event_bridge, {})
