from aws_lambda_typing import context as context_
from typing import Dict, Any
from fastapi import status
from fastapi.exceptions import ValidationError

from common.event_schemas.webhook_event_schema import WebhookEventSchema
from santander_flow.event_flow import EventFlow
from simple_common.logger import logger
from simple_common.utils import set_lambda_response
from common.exceptions import UnprocessableEntity


def lambda_handler(event: Dict[str, Any], _context: context_.Context) -> Dict[str, Any]:
    logger.info(f"Recebido evento: {event}")

    try:
        event_data = WebhookEventSchema(**event)
        result = EventFlow(event_data).start()
    except ValidationError as validation_error:
        logger.exception(
            f"Erro de validação nos dados recebidos do evento: {validation_error.errors()}",
            exc_info=validation_error,
        )
        raise UnprocessableEntity(str(validation_error.errors()))

    message = f"Evento processado com sucesso: {result}"
    logger.info(message)

    return set_lambda_response(status.HTTP_200_OK, result)


if __name__ == "__main__":  # pragma: no cover
    event = {
        "dispatchId": "26e17183-3d4e-4496-9ace-94ee4dc596c7",
        "platform": "Mercado Secundário",
        "action": "PROPOSAL_SELECTED",
        "eventDate": "2023-08-24T23:34:18.398Z",
        "payload": {
            "shareId": "008328e9-1cd5-4031-93f0-902e9494d5b2",
            "proposalId": "fb332012-dfa3-45d1-91a4-0f151f00d643",
        },
    }

    lambda_handler(event, {})
