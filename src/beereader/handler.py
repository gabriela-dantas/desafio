from aws_lambda_typing import context as context_
from typing import Dict, Union, Any
from fastapi import HTTPException, status
from fastapi.encoders import jsonable_encoder
from fastapi.exceptions import ValidationError

from beereader.extract_data import ExtractData
from beereader.bpm_confirmation import BPMConfirmation
from simple_common.logger import logger
from common.event_schemas.extract_event_schema import ExtractEventSchema
from simple_common.utils import set_lambda_response

logger = logger


def log_request(event: Dict[str, Union[str, int]]) -> None:
    truncated_url = event.get("extract_url", "").split("&X-Amz-Credential=")[0]
    actual_url = event.get("extract_url", "")
    event["extract_url"] = truncated_url

    logger.info(f"Recebido evento (URL Truncada para dados sensíveis): {event}")

    event["extract_url"] = actual_url


def lambda_handler(
    event: Dict[str, Union[str, int]], _context: context_.Context
) -> Dict[str, Any]:
    log_request(event)

    try:
        extract_event = ExtractEventSchema(**event)
        result = ExtractData().extract_and_save(extract_event)
    except ValidationError as validation_error:
        logger.exception(
            f"Erro de validação nos dados recebidos da requisição: {validation_error.errors()}",
            exc_info=validation_error,
        )

        bpm_response = BPMConfirmation().send_confirmation(event.get("quota_id"))
        body = {
            "error_description": jsonable_encoder(validation_error.errors()),
            "bpm_response": bpm_response,
        }

        return set_lambda_response(status.HTTP_422_UNPROCESSABLE_ENTITY, body)

    except HTTPException as http_exception:
        logger.exception(
            f"Erro identificado ao processar evento com extrato: {http_exception.detail}",
            exc_info=http_exception,
        )

        bpm_response = BPMConfirmation().send_confirmation(event.get("quota_id"))
        body = {
            "error_description": http_exception.detail,
            "bpm_response": bpm_response,
        }

        return set_lambda_response(http_exception.status_code, body)

    except Exception as generic_exception:
        logger.exception(
            f"Erro desconhecido ao processar evento com extrato: {generic_exception}",
            exc_info=generic_exception,
        )

        bpm_response = BPMConfirmation().send_confirmation(event.get("quota_id"))
        body = {
            "error_description": str(generic_exception),
            "bpm_response": bpm_response,
        }

        return set_lambda_response(status.HTTP_500_INTERNAL_SERVER_ERROR, body)

    logger.info(f"Processamento finalizado com resultado: {result}")

    return set_lambda_response(status.HTTP_201_CREATED, result)
