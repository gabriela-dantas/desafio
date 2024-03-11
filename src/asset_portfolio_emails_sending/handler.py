import os

from typing import Dict, Any, List
from aws_lambda_typing import events, context as context_
from datetime import date

from simple_common.utils import set_lambda_response
from simple_common.logger import logger
from simple_common.constants import HTTPStatusCodes
from simple_common.aws.secret_manager import SecretManager
from simple_common.aws.s3 import S3
from simple_common.aws.ses import SES


def lambda_handler(
    event: events.EventBridgeEvent, _context: context_.Context
) -> Dict[str, Any]:
    logger.info(f"Recebido evento {event}")
    ADMEmailsSending().send_to_all_adms()
    return set_lambda_response(HTTPStatusCodes.OK.value, "Emails enviados com sucesso!")


class ADMEmailsSending:
    def __init__(self) -> None:
        self.__adms = ["porto", "santander", "gmac"]
        self.__bucket_templates = os.environ["BUCKET_TEMPLATES"]
        self.__secret_name_emails = os.environ["SECRET_NAME_EMAIL"]

        self.__s3_service = S3()
        self.__ses = SES()
        self.__secret_manager = SecretManager()
        self.__logger = logger

    @property
    def adms(self) -> List[str]:
        return self.__adms

    def send_to_all_adms(self) -> None:
        self.__logger.info("Iniciando envio de emails a todas as ADMs.")

        emails: Dict[str, str] = self.__secret_manager.get_secret_value(
            self.__secret_name_emails
        )
        self.__logger.debug(f"Recuperados emails no Secret: {emails}")

        key = ""

        for adm in self.__adms:
            subject = f"Cotas na Carteira - Mensal - {date.today().strftime('%d/%m/%Y')} - {adm.upper()}"

            try:
                key = f"recipient_{adm}"
                recipients = emails[key]
                recipients = recipients.split(",")
            except KeyError as error:
                self.__logger.exception(
                    f"Chave {key} não presente no secret {self.__secret_name_emails}",
                    error,
                )
                raise error

            file_key = f"asset_portfolio/{adm}.html"
            encoded_body_html = self.__s3_service.get_file_content(
                self.__bucket_templates, file_key
            )
            body_html = encoded_body_html.decode("utf-8")

            self.__ses.send_html_email(emails["sender"], recipients, subject, body_html)


if __name__ == "__main__":  # pragma: no cover
    lambda_handler({}, {})
