import boto3
import json
import os

from botocore.exceptions import ClientError

from common.event_schemas.webhook_event_schema import WebhookEventSchema
from simple_common.logger import logger
from common.exceptions import InternalServerError
from common.repositories.dynamo.santander_webhook_event import (
    SantanderWebhookEventRepository,
)


class EventProcess:
    def __init__(self, event: WebhookEventSchema) -> None:
        self.__event = event
        self.__logger = logger
        self.__lambda_client = boto3.client("lambda")
        self.__lambda_name = os.environ["SANTANDER_FLOW_LAMBDA_NAME"]
        self.__webhook_event_repository = SantanderWebhookEventRepository()

    def __save_event(self) -> None:
        self.__logger.debug("Salvando evento no Dynamo.")
        event = self.__event.dict()
        item = {
            "share_id": event["payload"].pop("shareId"),
            "event_date": event.pop("eventDate"),
            "event_type": event.pop("action"),
            "data": event,
        }

        self.__webhook_event_repository.put_item(item)
        self.__logger.debug("Evento salvo.")

    def process(self) -> None:
        self.__save_event()

        try:
            self.__logger.debug(f"A invocar Lambda {self.__lambda_name}...")
            payload = json.dumps(self.__event.dict())

            self.__lambda_client.invoke(
                FunctionName=self.__lambda_name,
                InvocationType="Event",
                Payload=payload,
            )

            self.__logger.debug(f"Invocação da Lambda {self.__lambda_name} concluída.")

        except ClientError as client_error:
            message = f"Não foi possível invocar Lambda {self.__lambda_name}: {client_error.args}"
            self.__logger.error(message, exc_info=client_error)
            raise InternalServerError(message)
