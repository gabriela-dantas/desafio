from aws_lambda_typing import context as context_
from typing import Dict, Any
from fastapi import status
from fastapi.exceptions import ValidationError

from common.event_schemas.quota_creation_event_schema import ExtractEventSchema
from quota_creation_invoke.lambda_quota_creation_invoke import LambdaQuotaCreationInvoke
from simple_common.logger import logger
from common.exceptions import UnprocessableEntity
from simple_common.utils import set_lambda_response

logger = logger


def lambda_handler(event: Dict[str, Any], _context: context_.Context) -> Dict[str, Any]:
    try:
        logger.info(f"Evento recebido {event}")
        data_quotas_code = ExtractEventSchema(**event["detail"])
        LambdaQuotaCreationInvoke().invoke(data_quotas_code)
    except ValidationError as validation_error:
        logger.exception(
            f"Erro de validação nos dados recebidos do evento: {validation_error.errors()}",
            exc_info=validation_error,
        )
        raise UnprocessableEntity(str(validation_error.errors()))

    message = "Invocação da lambda de criação de cota feita com sucesso."
    logger.info(message)

    return set_lambda_response(status.HTTP_201_CREATED, message)
