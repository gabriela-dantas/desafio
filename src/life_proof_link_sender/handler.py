from aws_lambda_typing import context as context_
from typing import Dict, Any

from fastapi import status
from fastapi.exceptions import ValidationError

from common.event_schemas.life_proof_link_sender import ShareIdSchema
from life_proof_link_sender.lambda_life_proof_link_sender import (
    LambdaLifeProofLinkSender,
)
from simple_common.logger import logger
from common.exceptions import UnprocessableEntity
from simple_common.utils import set_lambda_response

logger = logger


def lambda_handler(event: Dict[str, Any], _context: context_.Context) -> Dict[str, Any]:
    logger.info(f"Evento recebido {event}")
    try:
        data_company_bond = ShareIdSchema(**event)
        LambdaLifeProofLinkSender().invoke(data_company_bond)
    except ValidationError as validation_error:
        logger.exception(
            f"Erro de validação nos dados recebidos do evento: {validation_error.errors()}",
            exc_info=validation_error,
        )
        raise UnprocessableEntity(str(validation_error.errors()))

    message = "Invocação da lambda de link de prova de vida executada com sucesso."
    logger.info(message)

    return set_lambda_response(status.HTTP_201_CREATED, message)


if __name__ == "__main__":  # pragma: no cover
    event = {"shareId": "02581c7e-16bb-4a70-af26-cfecf5fefa82"}

    lambda_handler(event, {})
