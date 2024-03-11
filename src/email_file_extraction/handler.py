import email
import re
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
    EmailFileExtraction().extract(event)
    return set_lambda_response(
        HTTPStatusCodes.OK.value, "Arquivos escritos com sucesso no bucket!"
    )


class EmailFileExtraction:
    def __init__(self) -> None:
        self.__possible_file_types = {"excel": "csv", "csv": "csv", "sheet": "xlsx"}

        self.__s3_service = S3()
        self.__glue = Glue()
        self.__logger = logger

    def __get_data_from_mine(self, mime_data: bytes) -> Tuple[str, str, str, bytes]:
        self.__logger.debug("Obtendo dados do arquivo a partir da mensagem.")
        message = email.message_from_bytes(mime_data)
        subject = message["subject"]

        try:
            target_subject = re.search(r"^Re:\s*([\w\s]+)", subject).group(1)
            adm = re.search(r"(-\s\w+)$", subject).group(1)
            adm = adm.strip("- ")
            self.__logger.debug(f"Obtido assunto {target_subject}, e ADM {adm}:")
        except AttributeError:
            message = f"Nnehum assunto ou ADM encontrados no formato esperado, no subject recebido: {subject}"
            self.__logger.exception(message)
            raise AttributeError(message)

        for part in message.walk():
            content_type = part.get_content_type()
            is_application = re.search(r"^(application/|text/csv)", content_type)

            if is_application is not None:
                match_types = "|".join(list(self.__possible_file_types.keys()))
                try:
                    file_type = re.search(rf"{match_types}", content_type).group(0)
                    file_type = self.__possible_file_types[file_type]
                    file_data = part.get_payload(decode=True)
                    self.__logger.debug(f"Obtidos dados do arquivo do tipo {file_type}")
                    return adm, target_subject, file_type, file_data
                except AttributeError:
                    continue

        message = "Nenhum arquivo encontrado em anexo nos formatos esperados."
        self.__logger.exception(message)
        raise AttributeError(message)

    def __get_file_key(self, adm: str, target_subject: str, file_type: str) -> Tuple[str, str]:
        key_by_subject = {
            "cotasnacarteira": {
                "subject_key": "asset-portfolio",
                "job_name": "from_asset_portfolio_file_to_stage_raw_"
            }
        }

        try:
            target_subject = unidecode.unidecode(
                target_subject.lower().replace(" ", "")
            )
            subject_key = key_by_subject[target_subject]["subject_key"]
            adm = unidecode.unidecode(adm.lower())
            today = date.today().strftime("%Y-%m-%d")
            file_key = f"{adm}/{subject_key}/received/partner-report-{today}.{file_type}"

            job_name = key_by_subject[target_subject]["job_name"]
            job_name += adm
            return file_key, job_name
        except KeyError:
            message = f"Subject do e-mail não mapeado: {target_subject}"
            self.__logger.exception(message)
            raise KeyError(message)

    def extract(self, event: dict) -> None:
        try:
            s3_data = event["Records"][0]["s3"]
            bucket_name = s3_data["bucket"]["name"]
            file_key = s3_data["object"]["key"]
        except KeyError as error:
            self.__logger.exception(
                f"Erro ao tentar obter nome do bucket e chave: {error}", exc_info=error
            )
            raise error

        self.__logger.debug(
            f"Obtendo arquivo MIME no bucket {bucket_name} | key: {file_key}"
        )
        mime_data = self.__s3_service.get_file_content(bucket_name, file_key)

        adm, target_subject, file_type, file_data = self.__get_data_from_mine(mime_data)
        file_key, job_name = self.__get_file_key(adm, target_subject, file_type)

        self.__logger.debug(
            f"Escrevendo arquivo no bucket {bucket_name} com chave {file_key}"
        )
        self.__s3_service.put_object(bucket_name, file_key, file_data)

        self.__glue.start_job_for_s3(job_name, bucket_name, file_key)
        self.__logger.info("Arquivo do e-mail salvo no bucket.")


if __name__ == "__main__":  # pragma: no cover
    s3_event = {
        "Records": [
            {
                "eventVersion": "2.0",
                "eventSource": "aws:s3",
                "awsRegion": "{region}",
                "eventTime": "1970-01-01T00:00:00Z",
                "eventName": "ObjectCreated:Put",
                "userIdentity": {"principalId": "EXAMPLE"},
                "requestParameters": {"sourceIPAddress": "127.0.0.1"},
                "responseElements": {
                    "x-amz-request-id": "EXAMPLE123456789",
                    "x-amz-id-2": "EXAMPLE123/5678abcdefghijklambdaisawesome/mnopqrstuvwxyzABCDEFGH",
                },
                "s3": {
                    "s3SchemaVersion": "1.0",
                    "configurationId": "testConfigRule",
                    "bucket": {
                        "name": "cota-adms-sandbox",
                        "ownerIdentity": {"principalId": "EXAMPLE"},
                        "arn": "arn:{partition}:s3:::mybucket",
                    },
                    "object": {
                        "key": "all-adms-emails/lh8fja7vi0tvtr6lrn696a0hjhuslspoit4tnp81",
                        "size": 1024,
                        "eTag": "0123456789abcdef0123456789abcdef",
                        "sequencer": "0A1B2C3D4E5F678901",
                    },
                },
            }
        ]
    }
    lambda_handler(s3_event, {})
