import json

from aws_lambda_typing import context as context_
from typing import Dict, Any
from fastapi import status
from fastapi.exceptions import ValidationError

from common.event_schemas.webhook_event_schema import WebhookEventSchema
from santander_webhook.event_process import EventProcess
from simple_common.logger import logger
from simple_common.utils import set_lambda_response
from common.exceptions import UnprocessableEntity


def lambda_handler(event: Dict[str, Any], _context: context_.Context) -> Dict[str, Any]:
    logger.info(f"Recebido evento: {event}")

    try:
        event_data = json.loads(event.get("body", "{}"))
        event_data = WebhookEventSchema(**event_data)
        EventProcess(event_data).process()
    except ValidationError as validation_error:
        logger.exception(
            f"Erro de validação nos dados recebidos do evento: {validation_error.errors()}",
            exc_info=validation_error,
        )
        raise UnprocessableEntity(str(validation_error.errors()))

    message = "Evento processado com sucesso."
    logger.info(message)

    return set_lambda_response(status.HTTP_200_OK, message)
