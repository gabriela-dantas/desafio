import boto3

from typing import List
from botocore.exceptions import ClientError

from simple_common.logger import logger


class SES:
    def __init__(self) -> None:
        self.__ses_client = boto3.client("ses")
        self.__charset = "UTF-8"
        self.__logger = logger

    def send_html_email(
        self, sender: str, recipients: List[str], subject: str, body_html: str
    ) -> None:
        self.__logger.info(
            f"Enviando email: remetente: {sender} | "
            f"destinatários: {recipients} | Assunto: {subject}"
        )
        try:
            response = self.__ses_client.send_email(
                Destination={"ToAddresses": recipients},
                Message={
                    "Body": {
                        "Html": {
                            "Charset": self.__charset,
                            "Data": body_html,
                        },
                    },
                    "Subject": {
                        "Charset": self.__charset,
                        "Data": subject,
                    },
                },
                Source=sender,
            )
            self.__logger.info(
                f"Email enviado com sucesso! Message ID: {response['MessageId']}"
            )
        except ClientError as error:
            self.__logger.error(
                f"Erro ao enviar email: {error.response['Error']['Message']}"
            )
            raise error
